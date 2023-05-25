#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

curl -sS "${CLICKHOUSE_URL}&query=SELECT%20%24%24doc%24%24VALUE%24%24doc%24%24";
curl -sS "${CLICKHOUSE_URL}&query=SELECT%20%24%24abc%24%24";
curl -sS "${CLICKHOUSE_URL}&query=SELECT%20%24%24abc%24%24d";
curl -sS "${CLICKHOUSE_URL}&query=SELECT%20%24%24ab" | grep -c "DB::Exception";
curl -sS "${CLICKHOUSE_URL}&query=SELECT%20%24%24" | grep -c "DB::Exception";
curl -sS "${CLICKHOUSE_URL}&query=SELECT%20%24" | grep -c "DB::Exception";
