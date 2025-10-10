#!/usr/bin/env bash

# FIXME: Note, this is not a queries test at all, move it somewhere else

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cat <<EOF | $CLICKHOUSE_CLIENT -nm
DROP TABLE IF EXISTS left;
DROP TABLE IF EXISTS right;

CREATE TABLE left (g UInt32, i UInt32)
  ORDER BY (g, i);

INSERT INTO left VALUES
(0, 1), (0, 2), (0, 3), (0, 4), (0, 5), (2, 0);

CREATE TABLE right (g UInt32, i UInt32)
  ORDER BY (g, i);

INSERT INTO right VALUES
(0, 0), (0, 3), (0, 4), (0, 6), (1, 0);
EOF

echo 'run enable-analyzer=1';
cat <<EOF | $CLICKHOUSE_CLIENT --enable-analyzer=1 -nm
with differences as
    (
        (
            select g, i from left
            where g BETWEEN 0 and 10
            EXCEPT ALL
            select g, i from right
            where g BETWEEN 0 and 10
        )
        UNION ALL
        (
            select g, i from right
            where g BETWEEN 0 and 10
            EXCEPT ALL
            select g, i from left
            where g BETWEEN 0 and 10
        )
    ),
diff_counts as
    (
        select g, count(*) from differences group by g
    )
select * from diff_counts;
EOF

echo 'run enable-analyzer=1 ignore';
cat <<EOF | $CLICKHOUSE_CLIENT --enable-analyzer=1 -nm
with differences as
    (
        (
            select g, i from left
            where g BETWEEN 0 and 10
            EXCEPT ALL
            select g, i from right
            where g BETWEEN 0 and 10
        )
        UNION ALL
        (
            select g, i from right
            where g BETWEEN 0 and 10
            EXCEPT ALL
            select g, i from left
            where g BETWEEN 0 and 10
        )
    ),
diff_counts as
    (
        select g, count(ignore(*)) from differences group by g
    )
select * from diff_counts;
EOF

echo 'run enable-analyzer=0';
cat <<EOF | $CLICKHOUSE_CLIENT --enable-analyzer=0 -nm
with differences as
    (
        (
            select g, i from left
            where g BETWEEN 0 and 10
            EXCEPT ALL
            select g, i from right
            where g BETWEEN 0 and 10
        )
        UNION ALL
        (
            select g, i from right
            where g BETWEEN 0 and 10
            EXCEPT ALL
            select g, i from left
            where g BETWEEN 0 and 10
        )
    ),
diff_counts as
    (
        select g, count(*) from differences group by g
    )
select * from diff_counts;
EOF
