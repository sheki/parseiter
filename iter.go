package parseiter

import (
	"fmt"
	"time"

	"github.com/tmc/parse"
)

const fmtISO = "2006-01-02T15:04:05.999Z07:00"

func New(appID, masterKey, className string) (*Iter, error) {
	c, err := parse.NewClient(appID, "")
	if err != nil {
		return nil, err
	}
	c = c.WithMasterKey(masterKey)
	return &Iter{client: c, currentTime: toISO(time.Time{}), class: className}, nil
}

type Iter struct {
	client       *parse.Client
	currentBatch []interface{}
	index        int
	currentTime  string
	class        string
}

func (i *Iter) fetchCurrent() ([]interface{}, error) {
	whereStr := fmt.Sprintf(`{"createdAt" : { "$gt" : "%s" } }`, i.currentTime)
	where := parse.QueryOptions{Where: whereStr, Limit: 1000, Order: "createdAt"}
	var currentBatch []interface{}
	err := i.client.QueryClass(i.class, &where, &currentBatch)
	return currentBatch, err
}

func (i *Iter) Next() (interface{}, bool) {
	if i.index == 0 || i.index >= len(i.currentBatch) {
		i.currentBatch, _ = i.fetchCurrent()
		i.index = 0
		fmt.Println("progress")
	}

	current := i.currentBatch[i.index]
	i.currentTime = current.(map[string]interface{})["createdAt"].(string)
	i.index++
	return current, true
}

func toISO(timestamp time.Time) string {
	timestamp = timestamp.UTC()
	response := timestamp.Format(fmtISO)
	expectedLength := 24

	if timestamp.Year() < 0 {
		// +1 for the leading -
		expectedLength++
	}

	// Go truncates the timestamp if there are no milliseconds
	// so we put back the zeros that go drops.
	if len(response) == expectedLength {
		return response
	}
	numZeros := expectedLength - len(response)
	if numZeros < 0 {
		panic("Bad time " + response)
	}
	zeros := ""
	if numZeros == 4 {
		zeros = "."
		numZeros--
	}
	for numZeros > 0 {
		zeros += "0"
		numZeros--
	}
	response = fmt.Sprintf("%s%sZ", response[0:len(response)-1], zeros)
	return response
}
