#!/usr/bin/env bash
# Tags: no-old-analyzer

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS date_dim;
    DROP TABLE IF EXISTS store_sales;
    DROP TABLE IF EXISTS item;
    CREATE TABLE date_dim (d_date_sk UInt32, d_year Int64) ORDER BY d_date_sk;
    CREATE TABLE store_sales (ss_sold_date_sk UInt32, ss_item_sk Int64) ORDER BY ss_sold_date_sk;
    CREATE TABLE item (i_item_sk Int64, i_brand_id Int64) ORDER BY i_item_sk;
"

# The whole formatted query goes into the message, so the part that says what to do about the mistake must
# come before it - otherwise it is behind kilobytes of text for a query of a realistic size.
hint_comes_before_the_query()
{
    echo -n "$1: "
    ${CLICKHOUSE_CLIENT} -q "$2" 2>&1 | grep -m1 -F 'Maybe you meant' | awk '
        {
            found = 1
            hint = index($0, "Maybe you meant")
            query = index($0, "In scope")
            print (query > 0 && hint < query) ? "hint first" : "BAD: " $0
        }
        END { if (!found) print "BAD: no hint in the message" }'
}

hint_comes_before_the_query "column"            "SELECT d_yaer FROM date_dim"
hint_comes_before_the_query "qualified column"  "SELECT date_dim.d_yaer FROM date_dim"
hint_comes_before_the_query "function"          "SELECT conut() FROM date_dim"
hint_comes_before_the_query "window function"   "SELECT avgf(d_year) OVER () FROM date_dim"
hint_comes_before_the_query "table"             "SELECT count() FROM date_dm"

echo
# A comma missing between two table names in the FROM clause silently turns the second one into an alias of
# the first. The message used to blame a "table with name item" while a table named `item` does exist.
echo -n 'alias of another table: '
${CLICKHOUSE_CLIENT} -q "SELECT item.i_brand_id FROM date_dim AS dt, store_sales item WHERE dt.d_date_sk = 1" 2>&1 \
    | grep -m1 -oF "cannot be resolved from table with name item (an alias of store_sales)"

# A genuine table reference must not be described as an alias of itself.
echo -n 'not an alias: '
${CLICKHOUSE_CLIENT} -q "SELECT item.no_such_column FROM item" 2>&1 | grep -cF "an alias of" || true

# A materialized CTE is backed by a temporary table with a generated name; the CTE name is what the user
# wrote, so that is what the message must show.
echo -n 'materialized CTE: '
${CLICKHOUSE_CLIENT} -q "WITH cte AS MATERIALIZED (SELECT 1 AS x) SELECT a.no_such FROM cte AS a" 2>&1 \
    | grep -m1 -oE "an alias of [A-Za-z_][A-Za-z_0-9]*" || true

${CLICKHOUSE_CLIENT} -q "DROP TABLE date_dim; DROP TABLE store_sales; DROP TABLE item"
