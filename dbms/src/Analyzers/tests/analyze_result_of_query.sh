#!/bin/sh

echo "SELECT 1, 2 + 3, toFixedString('Hello, world', 20) AS x, ('Hello', 1).1 AS y, z FROM system.numbers WHERE (arrayJoin([-1, 1]) AS z) = 1" | ./analyze_result_of_query
