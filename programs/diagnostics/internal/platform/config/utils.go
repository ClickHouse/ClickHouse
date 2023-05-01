package config

import (
	"fmt"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/utils"
)

func ReadStringListValues(conf Configuration, paramName string) ([]string, error) {
	param, err := conf.GetConfigParam(paramName)
	if err != nil {
		return nil, err
	}
	value, ok := param.(StringListParam)
	if !ok {
		value, ok = param.(StringListParam)
		if !ok {
			return nil, fmt.Errorf("%s must be a list of strings", paramName)
		}
	}

	return value.Values, nil
}

func ReadStringValue(conf Configuration, paramName string) (string, error) {
	param, err := conf.GetConfigParam(paramName)
	if err != nil {
		return "", err
	}
	value, ok := param.(StringParam)
	if !ok {
		return "", fmt.Errorf("%s must be a list of strings", paramName)
	}
	return value.Value, nil
}

func ReadIntValue(conf Configuration, paramName string) (int64, error) {
	param, err := conf.GetConfigParam(paramName)
	if err != nil {
		return 0, err
	}
	value, ok := param.(IntParam)
	if !ok {
		return 9, fmt.Errorf("%s must be an unsigned integer", paramName)
	}
	return value.Value, nil
}

func ReadBoolValue(conf Configuration, paramName string) (bool, error) {
	param, err := conf.GetConfigParam(paramName)
	if err != nil {
		return false, err
	}
	value, ok := param.(BoolParam)
	if !ok {
		return false, fmt.Errorf("%s must be a boolean", paramName)
	}
	return value.Value, nil
}

func ReadStringOptionsValue(conf Configuration, paramName string) (string, error) {
	param, err := conf.GetConfigParam(paramName)
	if err != nil {
		return "", err
	}
	value, ok := param.(StringOptions)
	if !ok {
		return "", fmt.Errorf("%s must be a string options", paramName)
	}
	if !utils.Contains(value.Options, value.Value) {
		return "", fmt.Errorf("%s is not a valid option in %v for the the parameter %s", value.Value, value.Options, paramName)
	}
	return value.Value, nil
}
