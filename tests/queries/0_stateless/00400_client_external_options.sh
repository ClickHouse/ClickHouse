#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

echo -ne "1\n2\n3\n" | $CLICKHOUSE_CLIENT --query="SELECT * FROM _data" --external --file=- --types=Int8;
echo -ne "1\n2\n3\n" | $CLICKHOUSE_CLIENT --query="SELECT * FROM _data" --external --file - --types Int8;
echo -ne "1\n2\n3\n" | $CLICKHOUSE_CLIENT --query="SELECT * FROM _data" --external --file=- --types Int8;
echo -ne "1\n2\n3\n" | $CLICKHOUSE_CLIENT --query="SELECT * FROM _data" --external --file - --types=Int8;
echo -ne "1\n2\n3\n" | $CLICKHOUSE_CLIENT --query "SELECT * FROM _data" --external --file=- --types=Int8;
