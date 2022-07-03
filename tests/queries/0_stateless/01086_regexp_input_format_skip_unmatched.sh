#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS regexp";
$CLICKHOUSE_CLIENT --query="CREATE TABLE regexp (id UInt32, string String) ENGINE = Memory";

echo 'id: 1 string: str1
id: 2 string: str2
id=3, string=str3
id: 4 string: str4' | $CLICKHOUSE_CLIENT --query="INSERT INTO regexp FORMAT Regexp SETTINGS format_regexp='id: (.+?) string: (.+?)', format_regexp_escaping_rule='Escaped', format_regexp_skip_unmatched=1";

$CLICKHOUSE_CLIENT --query="SELECT * FROM regexp";
$CLICKHOUSE_CLIENT --query="DROP TABLE regexp";
 
