package config

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/utils"
)

type ConfigParam interface {
	Name() string
	Required() bool
	Description() string
	validate(defaultConfig ConfigParam) error
}

type Configuration struct {
	Params []ConfigParam
}

type Param struct {
	name        string
	description string
	required    bool
}

func NewParam(name string, description string, required bool) Param {
	return Param{
		name:        name,
		description: description,
		required:    required,
	}
}

func (bp Param) Name() string {
	return bp.name
}

func (bp Param) Required() bool {
	return bp.required
}

func (bp Param) Description() string {
	return bp.description
}

func (bp Param) validate(defaultConfig ConfigParam) error {
	return nil
}

func (c Configuration) GetConfigParam(paramName string) (ConfigParam, error) {
	for _, param := range c.Params {
		if param.Name() == paramName {
			return param, nil
		}
	}
	return nil, fmt.Errorf("%s does not exist", paramName)
}

// ValidateConfig finds the intersection of a config c and a default config. Requires all possible params to be in default.
func (c Configuration) ValidateConfig(defaultConfig Configuration) (Configuration, error) {
	var finalParams []ConfigParam
	for _, defaultParam := range defaultConfig.Params {
		setParam, err := c.GetConfigParam(defaultParam.Name())
		if err == nil {
			// check the set value is valid
			if err := setParam.validate(defaultParam); err != nil {
				return Configuration{}, fmt.Errorf("parameter %s is invalid - %s", defaultParam.Name(), err.Error())
			}
			finalParams = append(finalParams, setParam)
		} else if defaultParam.Required() {
			return Configuration{}, fmt.Errorf("missing required parameter %s - %s", defaultParam.Name(), err.Error())
		} else {
			finalParams = append(finalParams, defaultParam)
		}
	}
	return Configuration{
		Params: finalParams,
	}, nil
}

type StringParam struct {
	Param
	Value      string
	AllowEmpty bool
}

func (sp StringParam) validate(defaultConfig ConfigParam) error {
	dsp := defaultConfig.(StringParam)
	if !dsp.AllowEmpty && strings.TrimSpace(sp.Value) == "" {
		return fmt.Errorf("%s cannot be empty", sp.Name())
	}
	// if the parameter is not required it doesn't matter
	return nil
}

type StringListParam struct {
	Param
	Values []string
}

type StringOptions struct {
	Param
	Options    []string
	Value      string
	AllowEmpty bool
}

func (so StringOptions) validate(defaultConfig ConfigParam) error {
	dso := defaultConfig.(StringOptions)
	if !dso.AllowEmpty && strings.TrimSpace(so.Value) == "" {
		return fmt.Errorf("%s cannot be empty", so.Name())
	}
	if !utils.Contains(dso.Options, so.Value) {
		return fmt.Errorf("%s is not a valid value for %s - %v", so.Value, so.Name(), so.Options)
	}
	// if the parameter is not required it doesn't matter
	return nil
}

type IntParam struct {
	Param
	Value int64
}

type BoolParam struct {
	Param
	Value bool
}
