package data

import (
	"strings"
)

// DiagnosticBundle contains the results from a Collector
// each frame can represent a table or collection of data files. By allowing multiple frames a single DiagnosticBundle
// can potentially contain many related tables
type DiagnosticBundle struct {
	Frames map[string]Frame
	// Errors is a property to be set if the Collector has an error. This can be used to indicate a partial collection
	// and failed frames
	Errors FrameErrors
}

type FrameErrors struct {
	Errors []error
}

func (fe *FrameErrors) Error() string {
	errors := make([]string, len(fe.Errors))
	for i := range errors {
		errors[i] = fe.Errors[i].Error()
	}
	return strings.Join(errors, "\n")
}
