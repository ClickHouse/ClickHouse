#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SQL_FILE_NAME=$"03156_default_multiquery_split_$$.sql"

cat << EOF > "$SQL_FILE_NAME"
drop table if exists test1; drop table if exists test2;
create table test1 (value Float64) ENGINE=MergeTree ORDER BY tuple();
create table test2 (value String) ENGINE=MergeTree ORDER BY tuple();
insert into test1 values
(101),
(102);
insert into test2 format csv
1
2;

insert into test2 format csv
3
4

6
EOF

$CLICKHOUSE_CLIENT -m -n < "$SQL_FILE_NAME"

cat << EOF > "$SQL_FILE_NAME"
insert into test2 values
('7'),
('8'),

('9');

select * from test1 order by value;
select * from test2 order by value;
drop table test1;drop table test2;
EOF

$CLICKHOUSE_CLIENT -m -n < "$SQL_FILE_NAME"

rm "$SQL_FILE_NAME"