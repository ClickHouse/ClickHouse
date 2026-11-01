#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

obf="$CLICKHOUSE_FORMAT --obfuscate"

echo "CREATE DICTIONARY d (id UInt64, name String) PRIMARY KEY id SOURCE(MYSQL(HOST 'localhost' PORT 3306 USER 'root' PASSWORD '' DB 'test' TABLE 'source')) LAYOUT(FLAT()) LIFETIME(0)" | $obf
echo "CREATE DICTIONARY d (id UInt64, name String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(COMPLEX_KEY_HASHED(SHARDS 16 PREALLOCATE 1)) LIFETIME(0)" | $obf
echo "CREATE DICTIONARY d (id UInt64, name String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 16777216 SIZE_IN_CELLS 1000000)) LIFETIME(0)" | $obf
echo "CREATE DICTIONARY d (id UInt64, name String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(RANGE_HASHED()) LIFETIME(0)" | $obf

# Corner case: function call as value — arguments must be obfuscated
echo "CREATE DICTIONARY d (id UInt64, name String) PRIMARY KEY id SOURCE(CLICKHOUSE(HOST func(my_arg, other_arg) PORT 9000 TABLE 't')) LAYOUT(FLAT()) LIFETIME(0)" | $obf

# Corner case: outside dict context, `port` is preserved (it's a registered function in Functions/URL/port.cpp),
# while `password` and `db` are not registered functions and get obfuscated.
echo "SELECT port, password, db FROM host" | $obf

# Corner case: nested parentheses in value expressions
echo "CREATE DICTIONARY d (id UInt64, name String) PRIMARY KEY id SOURCE(CLICKHOUSE(HOST concat(func(a), func(b)) TABLE 't')) LAYOUT(FLAT()) LIFETIME(0)" | $obf
