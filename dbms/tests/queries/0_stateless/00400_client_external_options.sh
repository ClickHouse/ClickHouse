#!/usr/bin/env bash
set -e

echo -ne "1\n2\n3\n" | clickhouse-client --query="SELECT * FROM _data" --external --file=- --types=Int8;
echo -ne "1\n2\n3\n" | clickhouse-client --query="SELECT * FROM _data" --external --file - --types Int8;
echo -ne "1\n2\n3\n" | clickhouse-client --query="SELECT * FROM _data" --external --file=- --types Int8;
echo -ne "1\n2\n3\n" | clickhouse-client --query="SELECT * FROM _data" --external --file - --types=Int8;
echo -ne "1\n2\n3\n" | clickhouse-client --query "SELECT * FROM _data" --external --file=- --types=Int8;
