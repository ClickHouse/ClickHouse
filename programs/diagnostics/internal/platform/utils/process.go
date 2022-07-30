package utils

import (
	"github.com/elastic/gosigar"
	"strings"
)

func FindClickHouseProcesses() ([]gosigar.ProcArgs, error) {
	pids := gosigar.ProcList{}
	err := pids.Get()
	if err != nil {
		return nil, err
	}
	var clickhousePs []gosigar.ProcArgs
	for _, pid := range pids.List {
		args := gosigar.ProcArgs{}
		if err := args.Get(pid); err != nil {
			continue
		}
		if len(args.List) > 0 {
			if strings.Contains(args.List[0], "clickhouse-server") {
				clickhousePs = append(clickhousePs, args)
			}
		}
	}
	return clickhousePs, nil
}

func FindConfigsFromClickHouseProcesses() ([]string, error) {
	clickhouseProcesses, err := FindClickHouseProcesses()
	if err != nil {
		return nil, err
	}
	var configs []string
	if len(clickhouseProcesses) > 0 {
		// we have candidate matches
		for _, ps := range clickhouseProcesses {
			for _, arg := range ps.List {
				if strings.Contains(arg, "--config") {
					configFile := strings.ReplaceAll(arg, "--config-file=", "")
					// containers receive config with --config
					configFile = strings.ReplaceAll(configFile, "--config=", "")
					configs = append(configs, configFile)
				}
			}
		}
	}
	return configs, err
}
