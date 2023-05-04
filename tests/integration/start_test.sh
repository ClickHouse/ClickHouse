#!/bin/bash

./runner --binary $HOME/ClickHouse/build/programs/clickhouse  --odbc-bridge-binary $HOME/ClickHouse/build/programs/clickhouse-odbc-bridge --base-configs-dir $HOME/ClickHouse/programs/server/ 'test_log_family_remote_disk -ss'
