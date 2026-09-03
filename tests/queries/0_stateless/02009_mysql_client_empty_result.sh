#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${MYSQL_CLIENT} --batch --execute 'SELECT 1 AS x'
${MYSQL_CLIENT} --batch --execute 'SELECT 1 AS x WHERE 0'

${MYSQL_CLIENT} <<<"
    DROP TABLE IF EXISTS b;
    CREATE TABLE b (i UInt8) ENGINE=MergeTree() PRIMARY KEY(i) ORDER BY (i);
    INSERT INTO b VALUES (1), (2), (3);
"

${MYSQL_CLIENT} --batch --execute 'SELECT * FROM b WHERE i>=3;'
${MYSQL_CLIENT} --batch --execute 'SELECT * FROM b WHERE i>=300;'

${MYSQL_CLIENT} <<<"
    DROP TABLE b;
"
