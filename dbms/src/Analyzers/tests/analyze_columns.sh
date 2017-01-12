#!/bin/sh

echo "SELECT dummy, number, one.dummy, numbers.number, system.one.dummy, system.numbers.number, one.*, numbers.*, system.one.*, system.numbers.*, *, t.*, t.number FROM system.one, system.numbers AS t" | ./analyze_columns
echo
echo "SELECT arrayMap((x, y) -> arrayMap((y, z) -> x[y], x, c), [[1], [2, 3]]) FROM (SELECT 1 AS c, 2 AS d)" | ./analyze_columns
echo
echo "SELECT x, arrayMap((x, y) -> x + y, x, c) FROM (SELECT 1 AS x, 2 AS c)" | ./analyze_columns
