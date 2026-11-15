#!/usr/bin/env bash
# Tags: no-parallel-replicas
# no-parallel-replicas: the query from query_log errors due to missing columns.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_x=%7B%22a%22%3A1%7D" -d "SELECT {x:Json} as j1, '{\"a\":1}'::Json as j2, j1 = j2 as equals, JSONAllPathsWithTypes(j1), JSONAllPathsWithTypes(j2)";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_x=1" -d "SELECT {x:Dynamic} as d1, 1::Dynamic as d2, d1 = d2 as equals, dynamicType(d1), dynamicType(d2)";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_x=1.1" -d "SELECT {x:Variant(Float32, String)} as v1, 1.1::Variant(Float32, String) as v2, v1 = v2 as equals, variantType(v1), variantType(v2)";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_x=%5C%27Hello%5C%27" -d "SELECT {x:Dynamic} as d, dynamicType(d) format Raw";
