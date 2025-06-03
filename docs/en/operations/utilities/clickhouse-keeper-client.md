---
slug: /en/operations/utilities/clickhouse-keeper-client
sidebar_label: clickhouse-keeper-client
---

# clickhouse-keeper-client

A client application to interact with clickhouse-keeper by its native protocol.

## Keys {#clickhouse-keeper-client}

-   `-q QUERY`, `--query=QUERY` — Query to execute. If this parameter is not passed, `clickhouse-keeper-client` will start in interactive mode.
-   `-h HOST`, `--host=HOST` — Server host. Default value: `localhost`.
-   `-p N`, `--port=N` — Server port. Default value: 9181
-   `-c FILE_PATH`, `--config-file=FILE_PATH` — Set path of config file to get the connection string. Default value: `config.xml`.
-   `--connection-timeout=TIMEOUT` — Set connection timeout in seconds. Default value: 10s.
-   `--session-timeout=TIMEOUT` — Set session timeout in seconds. Default value: 10s.
-   `--operation-timeout=TIMEOUT` — Set operation timeout in seconds. Default value: 10s.
-   `--history-file=FILE_PATH` — Set path of history file. Default value: `~/.keeper-client-history`.
-   `--log-level=LEVEL` — Set log level. Default value: `information`.
-   `--no-confirmation` — If set, will not require a confirmation on several commands. Default value `false` for interactive and `true` for query
-   `--help` — Shows the help message.

## Example {#clickhouse-keeper-client-example}

```bash
./clickhouse-keeper-client -h localhost -p 9181 --connection-timeout 30 --session-timeout 30 --operation-timeout 30
Connected to ZooKeeper at [::1]:9181 with session_id 137
/ :) ls
keeper foo bar
/ :) cd 'keeper'
/keeper :) ls
api_version
/keeper :) cd 'api_version'
/keeper/api_version :) ls

/keeper/api_version :) cd 'xyz'
Path /keeper/api_version/xyz does not exist
/keeper/api_version :) cd ../../
/ :) ls
keeper foo bar
/ :) get 'keeper/api_version'
2
```

## Commands {#clickhouse-keeper-client-commands}

-   `ls '[path]'` -- Lists the nodes for the given path (default: cwd)
-   `cd '[path]'` -- Changes the working path (default `.`)
-   `exists '<path>'` -- Returns `1` if node exists, `0` otherwise
-   `set '<path>' <value> [version]` -- Updates the node's value. Only updates if version matches (default: -1)
-   `create '<path>' <value> [mode]` -- Creates new node with the set value
-   `touch '<path>'` -- Creates new node with an empty string as value. Doesn't throw an exception if the node already exists
-   `get '<path>'` -- Returns the node's value
-   `rm '<path>' [version]` -- Removes the node only if version matches (default: -1)
-   `rmr '<path>'` -- Recursively deletes path. Confirmation required
-   `flwc <command>` -- Executes four-letter-word command
-   `help` -- Prints this message
-   `get_direct_children_number '[path]'` -- Get numbers of direct children nodes under a specific path
-   `get_all_children_number '[path]'` -- Get all numbers of children nodes under a specific path
-   `get_stat '[path]'` -- Returns the node's stat (default `.`)
-   `find_super_nodes <threshold> '[path]'` -- Finds nodes with number of children larger than some threshold for the given path (default `.`)
-   `delete_stale_backups` -- Deletes ClickHouse nodes used for backups that are now inactive
-   `find_big_family [path] [n]` -- Returns the top n nodes with the biggest family in the subtree (default path = `.` and n = 10)
-   `sync '<path>'` -- Synchronizes node between processes and leader
-   `reconfig <add|remove|set> "<arg>" [version]` -- Reconfigure Keeper cluster. See https://clickhouse.com/docs/en/guides/sre/keeper/clickhouse-keeper#reconfiguration
