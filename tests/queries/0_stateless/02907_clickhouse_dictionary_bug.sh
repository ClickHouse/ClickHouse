#!/usr/bin/env bash
# Tags: zookeeper

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    DROP DICTIONARY IF EXISTS 02907_dictionary;
    DROP TABLE IF EXISTS 02907_table;

    CREATE TABLE 02907_table (A String, B String) ENGINE=Memory AS SELECT 'a', 'b';
    CREATE DICTIONARY 02907_dictionary(A String, B String) PRIMARY KEY A
    SOURCE(CLICKHOUSE(QUERY \$\$  SELECT A, B FROM ${CLICKHOUSE_DATABASE}.02907_table ORDER BY A DESC LIMIT 1 BY A  \$\$))
    LAYOUT(complex_key_direct());

    SELECT dictGet('02907_dictionary','B','a');

    DROP DICTIONARY 02907_dictionary;
    DROP TABLE 02907_table;"
