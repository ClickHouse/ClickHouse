package utils

import "time"

func MakeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
