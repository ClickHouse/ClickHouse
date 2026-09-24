#!/usr/bin/env bash
# Tags: long, no-debug, no-asan, no-tsan, no-msan, no-ubsan, no-random-settings, no-random-merge-tree-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

exec >"$CURDIR"/test.stdout
sleep 12 &
sleep 12 &
sleep 12 &
sleep 12 &
sleep 12 &
echo 1
wait
