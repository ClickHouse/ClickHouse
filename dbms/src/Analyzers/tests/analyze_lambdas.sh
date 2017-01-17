#!/bin/sh

echo "SELECT arrayMap((x, y) -> x + arrayMap(x -> x[y], arr3), arr1, arr2)" | ./analyze_lambdas
