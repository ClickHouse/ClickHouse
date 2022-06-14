package config_test

import (
	"testing"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/config"
	"github.com/stretchr/testify/require"
)

var conf = config.Configuration{
	Params: []config.ConfigParam{
		config.StringListParam{
			Values: []string{"some", "values"},
			Param:  config.NewParam("paramA", "", false),
		},
		config.StringParam{
			Value: "random",
			Param: config.NewParam("paramB", "", true),
		},
		config.StringParam{
			Value:      "",
			AllowEmpty: true,
			Param:      config.NewParam("paramC", "", false),
		},
		config.StringOptions{
			Value:      "random",
			Options:    []string{"random", "very_random", "very_very_random"},
			Param:      config.NewParam("paramD", "", false),
			AllowEmpty: true,
		},
	},
}

func TestGetConfigParam(t *testing.T) {

	t.Run("can find get config param by name", func(t *testing.T) {
		paramA, err := conf.GetConfigParam("paramA")
		require.Nil(t, err)
		require.NotNil(t, paramA)
		require.IsType(t, config.StringListParam{}, paramA)
		stringListParam, ok := paramA.(config.StringListParam)
		require.True(t, ok)
		require.False(t, stringListParam.Required())
		require.Equal(t, stringListParam.Name(), "paramA")
		require.ElementsMatch(t, stringListParam.Values, []string{"some", "values"})
	})

	t.Run("throws error on missing element", func(t *testing.T) {
		paramZ, err := conf.GetConfigParam("paramZ")
		require.Nil(t, paramZ)
		require.NotNil(t, err)
		require.Equal(t, err.Error(), "paramZ does not exist")
	})
}

func TestValidateConfig(t *testing.T) {

	t.Run("validate adds the default and allows override", func(t *testing.T) {
		customConf := config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Value: "custom",
					Param: config.NewParam("paramB", "", true),
				},
			},
		}
		newConf, err := customConf.ValidateConfig(conf)
		require.Nil(t, err)
		require.NotNil(t, newConf)
		require.Len(t, newConf.Params, 4)
		// check first param
		require.IsType(t, config.StringListParam{}, newConf.Params[0])
		stringListParam, ok := newConf.Params[0].(config.StringListParam)
		require.True(t, ok)
		require.False(t, stringListParam.Required())
		require.Equal(t, stringListParam.Name(), "paramA")
		require.ElementsMatch(t, stringListParam.Values, []string{"some", "values"})
		// check second param
		require.IsType(t, config.StringParam{}, newConf.Params[1])
		stringParam, ok := newConf.Params[1].(config.StringParam)
		require.True(t, ok)
		require.True(t, stringParam.Required())
		require.Equal(t, "paramB", stringParam.Name())
		require.Equal(t, "custom", stringParam.Value)
	})

	t.Run("validate errors if missing param", func(t *testing.T) {
		//missing required paramB
		customConf := config.Configuration{
			Params: []config.ConfigParam{
				config.StringListParam{
					Values: []string{"some", "values"},
					Param:  config.NewParam("paramA", "", false),
				},
			},
		}
		newConf, err := customConf.ValidateConfig(conf)
		require.Nil(t, newConf.Params)
		require.NotNil(t, err)
		require.Equal(t, "missing required parameter paramB - paramB does not exist", err.Error())
	})

	t.Run("validate errors if invalid string value", func(t *testing.T) {
		//missing required paramB
		customConf := config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Value: "",
					Param: config.NewParam("paramB", "", true),
				},
			},
		}
		newConf, err := customConf.ValidateConfig(conf)
		require.Nil(t, newConf.Params)
		require.NotNil(t, err)
		require.Equal(t, "parameter paramB is invalid - paramB cannot be empty", err.Error())
	})

	t.Run("allow empty string value if specified", func(t *testing.T) {
		//missing required paramB
		customConf := config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Value: "",
					Param: config.NewParam("paramC", "", true),
				},
				config.StringParam{
					Value: "custom",
					Param: config.NewParam("paramB", "", true),
				},
			},
		}
		newConf, err := customConf.ValidateConfig(conf)
		require.NotNil(t, newConf.Params)
		require.Nil(t, err)
	})

	t.Run("validate errors if invalid string options value", func(t *testing.T) {
		//missing required paramB
		customConf := config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Value: "not_random",
					Param: config.NewParam("paramB", "", true),
				},
				config.StringOptions{
					Value: "custom",
					// this isn't ideal we need to ensure options are set for this to validate correctly
					Options: []string{"random", "very_random", "very_very_random"},
					Param:   config.NewParam("paramD", "", true),
				},
			},
		}
		newConf, err := customConf.ValidateConfig(conf)
		require.Nil(t, newConf.Params)
		require.NotNil(t, err)
		require.Equal(t, "parameter paramD is invalid - custom is not a valid value for paramD - [random very_random very_very_random]", err.Error())
	})

	t.Run("allow empty string value for StringOptions if specified", func(t *testing.T) {
		//missing required paramB
		customConf := config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Value: "custom",
					Param: config.NewParam("paramB", "", true),
				},
				config.StringOptions{
					Param: config.Param{},
					// this isn't ideal we need to ensure options are set for this to validate correctly
					Options: []string{"random", "very_random", "very_very_random"},
					Value:   "",
				},
			},
		}
		newConf, err := customConf.ValidateConfig(conf)
		require.NotNil(t, newConf.Params)
		require.Nil(t, err)
	})

	//TODO: Do we need to test if parameters of the same name but wrong type are passed??
}
