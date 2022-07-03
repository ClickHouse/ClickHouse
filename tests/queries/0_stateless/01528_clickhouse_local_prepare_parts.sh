#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_FOLDER_01528="${CLICKHOUSE_TMP}/01528_clickhouse_local_prepare_parts"
rm -rf "${WORKING_FOLDER_01528}"

mkdir -p "${WORKING_FOLDER_01528}/metadata/local"

## Checks scenario of preparing parts offline by clickhouse-local

## that is the metadata for the table we want to fill
## schema should match the schema of the table from server
## (the easiest way is just to copy it from the server)
cat <<EOF > "${WORKING_FOLDER_01528}/metadata/local/test.sql"
ATTACH TABLE local.test (id UInt64, d Date, s String) Engine=MergeTree ORDER BY id PARTITION BY toYYYYMM(d);
EOF

#################

echo "Option 1. Prepare parts from from table with Engine=File defined in metadata, read from an arbitrary path"

## Source file:
cat <<EOF > "${WORKING_FOLDER_01528}/data.csv"
1,2020-01-01,"String"
2,2020-02-02,"Another string"
3,2020-03-03,"One more string"
4,2020-01-02,"String for first partition"
EOF

## metadata written into file
cat <<EOF > "${WORKING_FOLDER_01528}/metadata/local/data_csv.sql"
ATTACH TABLE local.data_csv (id UInt64, d Date, s String) Engine=File(CSV, '${WORKING_FOLDER_01528}/data.csv');
EOF

## feed the table
${CLICKHOUSE_LOCAL} --query "INSERT INTO local.test SELECT * FROM local.data_csv;" --path="${WORKING_FOLDER_01528}"

## check the parts were created
${CLICKHOUSE_LOCAL} --query "SELECT * FROM local.test WHERE id < 10 ORDER BY id;" --path="${WORKING_FOLDER_01528}"

#################

echo "Option 2. Prepare parts from from table with Engine=File defined in metadata, read from stdin (pipe)"

cat <<EOF > "${WORKING_FOLDER_01528}/metadata/local/stdin.sql"
ATTACH TABLE local.stdin (id UInt64, d Date, s String) Engine=File(CSV, stdin);
EOF

cat <<EOF | ${CLICKHOUSE_LOCAL} --query "INSERT INTO local.test SELECT * FROM local.stdin;" --path="${WORKING_FOLDER_01528}"
11,2020-01-01,"String"
12,2020-02-02,"Another string"
13,2020-03-03,"One more string"
14,2020-01-02,"String for first partition"
EOF

${CLICKHOUSE_LOCAL} --query "SELECT * FROM local.test WHERE id BETWEEN 10 AND 19 ORDER BY id;" --path="${WORKING_FOLDER_01528}"

#################

echo "Option 3. Prepare parts from from table with Engine=File defined via command line, read from stdin (pipe)"

cat <<EOF | ${CLICKHOUSE_LOCAL} --query "INSERT INTO local.test SELECT * FROM table;" -S "id UInt64, d Date, s String" --input-format=CSV --path="${WORKING_FOLDER_01528}"
21,2020-01-01,"String"
22,2020-02-02,"Another string"
23,2020-03-03,"One more string"
24,2020-01-02,"String for first partition"
EOF

${CLICKHOUSE_LOCAL} --query "SELECT * FROM local.test WHERE id BETWEEN 20 AND 29 ORDER BY id;" --path="${WORKING_FOLDER_01528}"

#################

echo "Possibility to run optimize on prepared parts before sending parts to server"

${CLICKHOUSE_LOCAL} --query "OPTIMIZE TABLE local.test FINAL;" --path="${WORKING_FOLDER_01528}"

# ensure we have one part per partition
${CLICKHOUSE_LOCAL} --query "SELECT toYYYYMM(d) m, uniqExact(_part) FROM local.test GROUP BY m ORDER BY m" --path="${WORKING_FOLDER_01528}"

# cleanup
rm -rf "${WORKING_FOLDER_01528}"
