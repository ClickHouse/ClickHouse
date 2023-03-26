#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS btc"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE btc (data JSON) ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_object_type 1

cat $CUR_DIR/data_json/btc_transactions.json | ${CLICKHOUSE_CLIENT} -q "INSERT INTO btc FORMAT JSONAsObject"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM btc WHERE NOT ignore(*)"
${CLICKHOUSE_CLIENT} -q "DESC btc SETTINGS describe_extend_object_types = 1"

${CLICKHOUSE_CLIENT} -q "SELECT avg(data.fee), median(data.fee) FROM btc"

${CLICKHOUSE_CLIENT} -q "SELECT avg(length(data.inputs.prev_out.spending_outpoints) AS outpoints_length), median(outpoints_length) FROM btc"

${CLICKHOUSE_CLIENT} -q "SELECT data.out.spending_outpoints AS outpoints FROM btc WHERE arrayExists(x -> notEmpty(x), outpoints)"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS btc"
