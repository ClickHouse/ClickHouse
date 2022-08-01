package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

var (
	Version = "" // set at compile time with -ldflags "-X versserv/cmd.Version=x.y.yz"
	Commit  = ""
)

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of clickhouse-diagnostics",
	Long:  `All software has versions. This is clickhouse-diagnostics`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Clickhouse Diagnostics %s (%s)\n", Version, Commit)
	},
}
