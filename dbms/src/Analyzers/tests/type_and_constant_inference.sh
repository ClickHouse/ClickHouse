#!/bin/sh

echo "SELECT
    1,
    *,
    sleep(1),
    rand(),
    'Hello' || ' ' || 'World' AS world,
    e() AS e,
    e + pi(),
    count(*),
    sum(e),
    quantileTiming(0.5)(1),
    [1, -1, 0.1],
    toFixedString('Hello, world', 20),
    ('Hello', 1).1
FROM system.numbers" | ./type_and_constant_inference

echo
echo "SELECT t.x FROM (SELECT 1 AS x) AS t" | ./type_and_constant_inference
echo
echo "SELECT x FROM (SELECT 1 AS x)" | ./type_and_constant_inference
echo
echo "SELECT t.x, x, 1 FROM (SELECT 1 AS x) AS t" | ./type_and_constant_inference
echo
echo "SELECT *, z FROM (SELECT (1, 2) AS x, (SELECT 3, 4) AS y), (SELECT 'Hello, world' AS z)" | ./type_and_constant_inference
