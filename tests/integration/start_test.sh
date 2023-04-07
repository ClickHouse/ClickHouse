#!/bin/bash

./runner --binary $HOME/ClickHouse/build/programs/clickhouse  --odbc-bridge-binary $HOME/ClickHouse/build/programs/clickhouse-odbc-bridge --base-configs-dir $HOME/ClickHouse/programs/server/ 'test_merge_tree_s3 -ss'
