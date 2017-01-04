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
    [1, -1, 0.1]
FROM system.numbers" | ./type_and_constant_inference
