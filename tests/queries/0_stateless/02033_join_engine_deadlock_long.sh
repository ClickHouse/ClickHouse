#!/usr/bin/env bash
# Tags: long, deadlock

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

create_table () {
    $CLICKHOUSE_CLIENT --query "
            CREATE TABLE join_block_test
            (
                id String,
                num Int64
            )
            ENGINE = Join(ANY, LEFT, id)
        "
}

drop_table () {
    # Force a sync drop to free the memory before ending the test
    # Otherwise things get interesting if you run the test many times before the database is finally dropped
    $CLICKHOUSE_CLIENT --query "
            DROP TABLE join_block_test SYNC
        "
}

populate_table_bg () {
    (
        $CLICKHOUSE_CLIENT --query "
            INSERT INTO join_block_test
            SELECT toString(number) as id, number * number as num
            FROM system.numbers LIMIT 3000000
        " >/dev/null
    ) &
}

read_table_bg () {
    (
        $CLICKHOUSE_CLIENT --query "
            SELECT *
            FROM
            (
                SELECT toString(number) AS user_id
                FROM system.numbers LIMIT 10000 OFFSET 20000
            ) AS t1
            LEFT JOIN
            (
                SELECT
                    *
                FROM join_block_test AS i1
                ANY LEFT JOIN
                (
                    SELECT *
                    FROM join_block_test
                ) AS i2 ON i1.id = toString(i2.num)
            ) AS t2 ON t1.user_id = t2.id
        " >/dev/null
    ) &
}

create_table
for _ in {1..5};
do
    populate_table_bg
    sleep 0.05
    read_table_bg
    sleep 0.05
done

wait
drop_table
