#!/usr/bin/env bash

# Test for a complex-key dictionary whose key columns are not the leading columns in the
# dictionary definition, reading from a local ClickHouse source via an explicit query.
# Before the fix the source columns were matched to the dictionary structure by position
# instead of by name, so the columns were mixed up and the load failed with CANNOT_PARSE_TEXT.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery "
DROP DICTIONARY IF EXISTS dict_non_leading_key;
DROP TABLE IF EXISTS dict_source;

CREATE TABLE dict_source (id UInt32, field1 String, field2 String, someValue String) ENGINE = MergeTree ORDER BY id;
INSERT INTO dict_source (id, field1, field2, someValue) VALUES (1, 'qwe', 'rty', 'val1'), (2, 'qwe1', 'rty1', 'val2');

CREATE DICTIONARY dict_non_leading_key (id UInt32, field1 String, field2 String, someValue String)
    PRIMARY KEY field1, field2
    SOURCE(CLICKHOUSE(
        HOST 'localhost'
        PORT tcpPort()
        USER 'default'
        PASSWORD ''
        DB currentDatabase()
        QUERY 'SELECT id, field1, field2, someValue FROM ${CLICKHOUSE_DATABASE}.dict_source'))
    LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(MIN 0 MAX 0);

SELECT id, field1, field2, someValue FROM dict_non_leading_key ORDER BY id;

SELECT dictGet(currentDatabase() || '.dict_non_leading_key', 'id', ('qwe', 'rty'));
SELECT dictGet(currentDatabase() || '.dict_non_leading_key', 'someValue', ('qwe1', 'rty1'));
SELECT dictHas(currentDatabase() || '.dict_non_leading_key', ('qwe', 'rty'));
SELECT dictHas(currentDatabase() || '.dict_non_leading_key', ('nope', 'nope'));

DROP DICTIONARY dict_non_leading_key;
DROP TABLE dict_source;
"

# Backward compatibility: a local source whose query does not name its columns to match the
# dictionary attributes (e.g. 'SELECT 100, ...') must still load via positional matching.
${CLICKHOUSE_CLIENT} --multiquery "
DROP DICTIONARY IF EXISTS dict_positional;

CREATE DICTIONARY dict_positional (k UInt64, v String)
    PRIMARY KEY k
    SOURCE(CLICKHOUSE(
        HOST 'localhost'
        PORT tcpPort()
        USER 'default'
        PASSWORD ''
        DB currentDatabase()
        QUERY 'SELECT 100, ''hello'''))
    LAYOUT(HASHED()) LIFETIME(MIN 0 MAX 0);

SELECT dictGet(currentDatabase() || '.dict_positional', 'v', toUInt64(100));
SELECT dictHas(currentDatabase() || '.dict_positional', toUInt64(100));

DROP DICTIONARY dict_positional;
"

# A range dictionary expects the range columns right after the key, but the query below returns
# them last. Names still match, so the load must reorder them (issue #113935).
${CLICKHOUSE_CLIENT} --multiquery "
DROP DICTIONARY IF EXISTS dict_range_non_leading;
DROP TABLE IF EXISTS dict_range_source;

CREATE TABLE dict_range_source (meter_no String, id Int64, contract_time Date, end_date Date) ENGINE = MergeTree ORDER BY meter_no;
INSERT INTO dict_range_source VALUES ('M1', 97955, '2020-01-01', '2030-01-01');

CREATE DICTIONARY dict_range_non_leading (meter_no String, id Int64, contract_time Date, end_date Date)
    PRIMARY KEY meter_no
    SOURCE(CLICKHOUSE(
        HOST 'localhost'
        PORT tcpPort()
        USER 'default'
        PASSWORD ''
        DB currentDatabase()
        QUERY 'SELECT meter_no, id, contract_time, end_date FROM ${CLICKHOUSE_DATABASE}.dict_range_source'))
    LAYOUT(RANGE_HASHED()) RANGE(MIN contract_time MAX end_date) LIFETIME(MIN 0 MAX 0);

SELECT dictGet(currentDatabase() || '.dict_range_non_leading', 'id', 'M1', toDate('2025-01-01'));

DROP DICTIONARY dict_range_non_leading;
DROP TABLE dict_range_source;
"
