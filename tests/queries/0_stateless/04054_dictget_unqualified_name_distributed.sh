#!/usr/bin/env bash
# Tags: distributed

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/44301 and https://github.com/ClickHouse/ClickHouse/issues/50382
# Doesn't work without enable_analyzer = 1

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The test runs in the default database s.t. the shard (which connects with database=default) can resolve the unqualified dictionary name.
# Object names include CLICKHOUSE_DATABASE to avoid collisions in parallel runs.
SUFFIX="${CLICKHOUSE_DATABASE}"
CLICKHOUSE_CLIENT_DEFAULT_DB=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--database=${CLICKHOUSE_DATABASE}"'/--database=default/g')

${CLICKHOUSE_CLIENT_DEFAULT_DB} <<SQL
DROP TABLE IF EXISTS test_table_dist_${SUFFIX};
DROP TABLE IF EXISTS test_table_${SUFFIX};
DROP DICTIONARY IF EXISTS test_dict_${SUFFIX};

CREATE DICTIONARY test_dict_${SUFFIX} (id UInt64, val UInt64)
PRIMARY KEY id
LAYOUT(FLAT)
SOURCE(CLICKHOUSE(QUERY 'SELECT number AS id, number AS val FROM numbers(100)'))
LIFETIME(0);

CREATE TABLE test_table_${SUFFIX} ENGINE = Log AS SELECT number FROM numbers(200);

CREATE TABLE test_table_dist_${SUFFIX} ENGINE = Distributed(test_shard_localhost, 'default', test_table_${SUFFIX}) AS test_table_${SUFFIX};

SELECT number, dictGet('test_dict_${SUFFIX}', 'val', toUInt64(number)) FROM test_table_dist_${SUFFIX} ORDER BY number SETTINGS enable_analyzer = 1;

DROP TABLE test_table_dist_${SUFFIX};
DROP TABLE test_table_${SUFFIX};
DROP DICTIONARY test_dict_${SUFFIX};
SQL
