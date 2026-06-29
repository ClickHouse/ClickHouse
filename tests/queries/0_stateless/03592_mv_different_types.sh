#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE source
(
    value String
)
ENGINE = MergeTree
ORDER BY value;

CREATE TABLE middle
(
    type Enum8('ENUM_VAL_1' = 0, 'ENUM_VAL_2' = 34)
)
ENGINE = MergeTree
ORDER BY type;

CREATE TABLE destination
(
    type String,
    jsType String MATERIALIZED lower(type)
)
ENGINE = MergeTree
ORDER BY type;
EOF

echo "test source_to_destination_mv"
${CLICKHOUSE_CLIENT} <<EOF
CREATE MATERIALIZED VIEW source_to_destination_mv TO destination
(
    type Enum8('ENUM_VAL_1' = 0, 'ENUM_VAL_2' = 34)
)
AS SELECT
    value as type
FROM source;
EOF

${CLICKHOUSE_CLIENT} <<EOF
--set send_logs_level='test';
INSERT INTO source ("value")
VALUES ('ENUM_VAL_1'),
       ('ENUM_VAL_2');
EOF

${CLICKHOUSE_CLIENT} <<EOF
INSERT INTO source ("value")
VALUES ('ENUMN_VAL_WRONG');
EOF

${CLICKHOUSE_CLIENT} <<EOF
SELECT *
FROM destination
ORDER BY type
EOF

${CLICKHOUSE_CLIENT} <<EOF
DROP TABLE IF EXISTS source_to_destination_mv;
EOF

echo "test source_to_middle_mv and middle_to_destination_mv"
${CLICKHOUSE_CLIENT} <<EOF
CREATE MATERIALIZED VIEW source_to_middle_mv TO middle
AS SELECT
    value as type
FROM source;

CREATE MATERIALIZED VIEW middle_to_destination_mv TO destination
(
    type Enum8('ENUM_VAL_1' = 0, 'ENUM_VAL_2' = 34)
)
AS SELECT
    type as type
FROM middle;
EOF

${CLICKHOUSE_CLIENT} <<EOF
--set send_logs_level='test';
INSERT INTO source ("value")
VALUES ('ENUM_VAL_1'),
       ('ENUM_VAL_2');
EOF


${CLICKHOUSE_CLIENT} <<EOF
INSERT INTO source ("value")
VALUES ('ENUMN_VAL_WRONG'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
EOF

${CLICKHOUSE_CLIENT} <<EOF
SELECT *
FROM destination
ORDER BY type
EOF

${CLICKHOUSE_CLIENT} <<EOF
DROP TABLE IF EXISTS source_to_middle_mv;
DROP TABLE IF EXISTS middle_to_destination_mv;
EOF

#############
${CLICKHOUSE_CLIENT} <<EOF
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS middle;
DROP TABLE IF EXISTS destination;
EOF
