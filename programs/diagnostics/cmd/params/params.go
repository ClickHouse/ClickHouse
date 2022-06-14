package params

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/config"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/utils"
	"github.com/spf13/cobra"
	"strings"
)

type cliParamType uint8

const (
	String cliParamType = iota
	StringList
	StringOptionsList
	Integer
	Boolean
)

type CliParam struct {
	Description string
	Default     interface{}
	//this should always be an address to a value - as required by cobra
	Value interface{}
	Type  cliParamType
}

type ParamMap map[string]map[string]CliParam

func NewParamMap(configs map[string]config.Configuration) ParamMap {
	paramMap := make(ParamMap)
	for name, configuration := range configs {
		for _, param := range configuration.Params {
			switch p := param.(type) {
			case config.StringParam:
				paramMap = paramMap.createStringParam(name, p)
			case config.StringListParam:
				paramMap = paramMap.createStringListParam(name, p)
			case config.StringOptions:
				paramMap = paramMap.createStringOptionsParam(name, p)
			case config.IntParam:
				paramMap = paramMap.createIntegerParam(name, p)
			case config.BoolParam:
				paramMap = paramMap.createBoolParam(name, p)
			}
		}
	}
	return paramMap
}

func (m ParamMap) createBoolParam(rootKey string, bParam config.BoolParam) ParamMap {
	if _, ok := m[rootKey]; !ok {
		m[rootKey] = make(map[string]CliParam)
	}
	var value bool
	param := CliParam{
		Description: bParam.Description(),
		Default:     bParam.Value,
		Value:       &value,
		Type:        Boolean,
	}
	m[rootKey][bParam.Name()] = param
	return m
}

func (m ParamMap) createStringParam(rootKey string, sParam config.StringParam) ParamMap {
	if _, ok := m[rootKey]; !ok {
		m[rootKey] = make(map[string]CliParam)
	}
	var value string
	param := CliParam{
		Description: sParam.Description(),
		Default:     sParam.Value,
		Value:       &value,
		Type:        String,
	}
	m[rootKey][sParam.Name()] = param
	return m
}

func (m ParamMap) createStringListParam(rootKey string, lParam config.StringListParam) ParamMap {
	if _, ok := m[rootKey]; !ok {
		m[rootKey] = make(map[string]CliParam)
	}
	var value []string
	param := CliParam{
		Description: lParam.Description(),
		Default:     lParam.Values,
		Value:       &value,
		Type:        StringList,
	}
	m[rootKey][lParam.Name()] = param
	return m
}

func (m ParamMap) createStringOptionsParam(rootKey string, oParam config.StringOptions) ParamMap {
	if _, ok := m[rootKey]; !ok {
		m[rootKey] = make(map[string]CliParam)
	}
	value := StringOptionsVar{
		Options: oParam.Options,
		Value:   oParam.Value,
	}
	param := CliParam{
		Description: oParam.Description(),
		Default:     oParam.Value,
		Value:       &value,
		Type:        StringOptionsList,
	}
	m[rootKey][oParam.Name()] = param
	return m
}

func (m ParamMap) createIntegerParam(rootKey string, iParam config.IntParam) ParamMap {
	if _, ok := m[rootKey]; !ok {
		m[rootKey] = make(map[string]CliParam)
	}
	var value int64
	param := CliParam{
		Description: iParam.Description(),
		Default:     iParam.Value,
		Value:       &value,
		Type:        Integer,
	}
	m[rootKey][iParam.Name()] = param
	return m
}

func (c CliParam) GetConfigParam(name string) config.ConfigParam {
	// this is a config being passed to a collector - required can be false
	param := config.NewParam(name, c.Description, false)
	switch c.Type {
	case String:
		return config.StringParam{
			Param: param,
			// values will be pointers
			Value: *(c.Value.(*string)),
		}
	case StringList:
		return config.StringListParam{
			Param:  param,
			Values: *(c.Value.(*[]string)),
		}
	case StringOptionsList:
		optionsVar := *(c.Value.(*StringOptionsVar))
		return config.StringOptions{
			Param:   param,
			Options: optionsVar.Options,
			Value:   optionsVar.Value,
		}
	case Integer:
		return config.IntParam{
			Param: param,
			Value: *(c.Value.(*int64)),
		}
	case Boolean:
		return config.BoolParam{
			Param: param,
			Value: *(c.Value.(*bool)),
		}
	}
	return param
}

type StringOptionsVar struct {
	Options []string
	Value   string
}

func (o StringOptionsVar) String() string {
	return o.Value
}

func (o *StringOptionsVar) Set(p string) error {
	isIncluded := func(opts []string, val string) bool {
		for _, opt := range opts {
			if val == opt {
				return true
			}
		}
		return false
	}
	if !isIncluded(o.Options, p) {
		return fmt.Errorf("%s is not included in options: %v", p, o.Options)
	}
	o.Value = p
	return nil
}

func (o *StringOptionsVar) Type() string {
	return "string"
}

type StringSliceOptionsVar struct {
	Options []string
	Values  []string
}

func (o StringSliceOptionsVar) String() string {
	str, _ := writeAsCSV(o.Values)
	return "[" + str + "]"
}

func (o *StringSliceOptionsVar) Set(val string) error {
	values, err := readAsCSV(val)
	if err != nil {
		return err
	}
	vValues := utils.Distinct(values, o.Options)
	if len(vValues) > 0 {
		return fmt.Errorf("%v are not included in options: %v", vValues, o.Options)
	}
	o.Values = values
	return nil
}

func (o *StringSliceOptionsVar) Type() string {
	return "stringSlice"
}

func writeAsCSV(vals []string) (string, error) {
	b := &bytes.Buffer{}
	w := csv.NewWriter(b)
	err := w.Write(vals)
	if err != nil {
		return "", err
	}
	w.Flush()
	return strings.TrimSuffix(b.String(), "\n"), nil
}

func readAsCSV(val string) ([]string, error) {
	if val == "" {
		return []string{}, nil
	}
	stringReader := strings.NewReader(val)
	csvReader := csv.NewReader(stringReader)
	return csvReader.Read()
}

func AddParamMapToCmd(paramMap ParamMap, cmd *cobra.Command, prefix string, hide bool) {
	for rootKey, childMap := range paramMap {
		for childKey, value := range childMap {
			paramName := fmt.Sprintf("%s.%s.%s", prefix, rootKey, childKey)
			switch value.Type {
			case String:
				cmd.Flags().StringVar(value.Value.(*string), paramName, value.Default.(string), value.Description)
			case StringList:
				cmd.Flags().StringSliceVar(value.Value.(*[]string), paramName, value.Default.([]string), value.Description)
			case StringOptionsList:
				cmd.Flags().Var(value.Value.(*StringOptionsVar), paramName, value.Description)
			case Integer:
				cmd.Flags().Int64Var(value.Value.(*int64), paramName, value.Default.(int64), value.Description)
			case Boolean:
				cmd.Flags().BoolVar(value.Value.(*bool), paramName, value.Default.(bool), value.Description)
			}
			// this ensures flags from collectors and outputs are not shown as they will pollute the output
			if hide {
				_ = cmd.Flags().MarkHidden(paramName)
			}
		}
	}
}

func ConvertParamsToConfig(paramMap ParamMap) map[string]config.Configuration {
	configuration := make(map[string]config.Configuration)
	for rootKey, childMap := range paramMap {
		if _, ok := configuration[rootKey]; !ok {
			configuration[rootKey] = config.Configuration{}
		}
		for childKey, value := range childMap {
			configParam := value.GetConfigParam(childKey)
			configuration[rootKey] = config.Configuration{Params: append(configuration[rootKey].Params, configParam)}
		}
	}
	return configuration
}
