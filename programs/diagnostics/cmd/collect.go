package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/cmd/params"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/collectors"
	_ "github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/collectors/clickhouse"
	_ "github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/collectors/system"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/outputs"
	_ "github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/outputs/file"
	_ "github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/outputs/terminal"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/utils"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var id string
var output = params.StringOptionsVar{
	Options: outputs.GetOutputNames(),
	Value:   "simple",
}

// access credentials
var host string
var port uint16
var username string
var password string

var collectorNames = params.StringSliceOptionsVar{
	Options: collectors.GetCollectorNames(false),
	Values:  collectors.GetCollectorNames(true),
}

// holds the collector params passed by the cli
var collectorParams params.ParamMap

// holds the output params passed by the cli
var outputParams params.ParamMap

const collectHelpTemplate = `Usage:{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasAvailableSubCommands}}
  {{.CommandPath}} [command]{{end}}{{if gt (len .Aliases) 0}}

Aliases:
  {{.NameAndAliases}}{{end}}{{if .HasExample}}

Examples:
{{.Example}}{{end}}{{if .HasAvailableSubCommands}}

Available Commands:{{range .Commands}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}

Additional help topics:
	Use "{{.CommandPath}} [command] --help" for more information about a command.
	Use "{{.Parent.Name}} help --collector [collector]" for more information about a specific collector.
	Use "{{.Parent.Name}} help --output [output]" for more information about a specific output.
`

func init() {
	collectCmd.Flags().StringVar(&id, "id", getHostName(), "Id of diagnostic bundle")

	// access credentials
	collectCmd.Flags().StringVar(&host, "host", "localhost", "ClickHouse host")
	collectCmd.Flags().Uint16VarP(&port, "port", "p", 9000, "ClickHouse native port")
	collectCmd.Flags().StringVarP(&username, "username", "u", "", "ClickHouse username")
	collectCmd.Flags().StringVar(&password, "password", "", "ClickHouse password")
	// collectors and outputs
	collectCmd.Flags().VarP(&output, "output", "o", fmt.Sprintf("Output Format for the diagnostic Bundle, options: [%s]\n", strings.Join(output.Options, ",")))
	collectCmd.Flags().VarP(&collectorNames, "collectors", "c", fmt.Sprintf("Collectors to use, options: [%s]\n", strings.Join(collectorNames.Options, ",")))

	collectorConfigs, err := collectors.BuildConfigurationOptions()
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to build collector configurations")
	}
	collectorParams = params.NewParamMap(collectorConfigs)

	outputConfigs, err := outputs.BuildConfigurationOptions()
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to build output configurations")
	}
	params.AddParamMapToCmd(collectorParams, collectCmd, "collector", true)

	outputParams = params.NewParamMap(outputConfigs)
	params.AddParamMapToCmd(outputParams, collectCmd, "output", true)

	collectCmd.SetFlagErrorFunc(handleFlagErrors)
	collectCmd.SetHelpTemplate(collectHelpTemplate)
	rootCmd.AddCommand(collectCmd)
}

var collectCmd = &cobra.Command{
	Use:   "collect",
	Short: "Collect a diagnostic bundle",
	Long:  `Collect a ClickHouse diagnostic bundle for a specified ClickHouse instance`,
	PreRun: func(cmd *cobra.Command, args []string) {
		bindFlagsToConfig(cmd)
	},
	Example: fmt.Sprintf(`%s collect --username default --collector=%s --output=simple`, rootCmd.Name(), strings.Join(collectorNames.Options[:2], ",")),
	Run: func(cmd *cobra.Command, args []string) {
		log.Info().Msgf("executing collect command with %v collectors and %s output", collectorNames.Values, output.Value)
		outputConfig := params.ConvertParamsToConfig(outputParams)[output.Value]
		runConfig := internal.NewRunConfiguration(id, host, port, username, password, output.Value, outputConfig, collectorNames.Values, params.ConvertParamsToConfig(collectorParams))
		internal.Capture(runConfig)
		os.Exit(0)
	},
}

func getHostName() string {
	name, err := os.Hostname()
	if err != nil {
		name = "clickhouse-diagnostics"
	}
	return name
}

// these flags are nested under the cmd name in the config file to prevent collisions
var flagsToNest = []string{"output", "collectors"}

// this saves us binding each command manually to viper
func bindFlagsToConfig(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		err := viper.BindEnv(f.Name, fmt.Sprintf("%s_%s", envPrefix,
			strings.ToUpper(strings.Replace(f.Name, ".", "_", -1))))
		if err != nil {
			log.Error().Msgf("Unable to bind %s to config", f.Name)
		}
		configFlagName := f.Name
		if utils.Contains(flagsToNest, f.Name) {
			configFlagName = fmt.Sprintf("%s.%s", cmd.Use, configFlagName)
		}
		err = viper.BindPFlag(configFlagName, f)
		if err != nil {
			log.Error().Msgf("Unable to bind %s to config", f.Name)
		}
		// here we prefer the config value when the param is not set on the cmd line
		if !f.Changed && viper.IsSet(configFlagName) {
			val := viper.Get(configFlagName)
			log.Debug().Msgf("Setting parameter %s from configuration file", f.Name)
			err = cmd.Flags().Set(f.Name, fmt.Sprintf("%v", val))
			if err != nil {
				log.Error().Msgf("Unable to read \"%s\" value from config", f.Name)
			} else {
				log.Debug().Msgf("Set parameter \"%s\" from configuration", f.Name)
			}
		}
	})
}
