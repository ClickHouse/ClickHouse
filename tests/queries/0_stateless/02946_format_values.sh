#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "insert into table1 values (1, [1,3], 'fd'), (2, [2,4], 'sd'), (3, [3,5], 'td')" | ${CLICKHOUSE_FORMAT}

echo "======================================"

cat <<EOF | ${CLICKHOUSE_FORMAT} -n
select a from table1;

insert into table1 values (1, [1,3], 'fd'), (2, [2,4], 'sd'), (3, [3,5], 'td');

select b from table1;

EOF

echo "======================================"

cat <<EOF | ${CLICKHOUSE_FORMAT} -n --comments

-- begin
select a from table1;

-- some insert query

insert into table1 values (1, [1,3], 'fd'), (2, [2,4], 'sd'), (3, [3,5], 'td');

-- more comments
-- in a row

select b from table1;

-- end

EOF

echo "======================================"


cat <<EOF | ${CLICKHOUSE_FORMAT} -n --comments --max_line_length=25
select b from table1;
select b, c from table1;
select b, c, d from table1;
select b, c, d, e from table1;
select b, c, d, e, f from table1;
select b, c from (select b, c from table1);
select b, c, d, e, f from ( select b, c, d, e, f from table1 );
EOF

echo "======================================"

cat <<EOF | ${CLICKHOUSE_FORMAT} -n --comments --max_line_length=50
select b from table1;
select b, c from table1;
select b, c, d from table1;
select b, c, d, e from table1;
select b, c, d, e, f from table1;
select b, c from (select b, c from table1);
select b, c, d, e, f from ( select b, c, d, e, f from table1 );
EOF

echo "======================================"

echo "select b, c, d, e, f from ( select b, c, d, e, f from table1 )" | ${CLICKHOUSE_FORMAT} --max_line_length=50
echo "select b, c, d, e, f from ( select b, c, d, e, f from table1 )" | ${CLICKHOUSE_FORMAT} --max_line_length=250

echo "======================================"

{ echo "select 1" | ${CLICKHOUSE_FORMAT} --comments --max_line_length=260 2>&1; echo $?; }
{ echo "select 1" | ${CLICKHOUSE_FORMAT} --comments --max_line_length=120 --oneline 2>&1; echo $?; }
