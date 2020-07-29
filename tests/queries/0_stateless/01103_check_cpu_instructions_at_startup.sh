#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

command=$(command -v ${CLICKHOUSE_LOCAL})

qemu-x86_64-static -cpu qemu64                        $command --query "SELECT 1" 2>&1 | grep -v -F "warning: TCG doesn't support requested feature" ||:
qemu-x86_64-static -cpu qemu64,+ssse3                 $command --query "SELECT 1" 2>&1 | grep -v -F "warning: TCG doesn't support requested feature" ||:
qemu-x86_64-static -cpu qemu64,+ssse3,+sse4.1         $command --query "SELECT 1" 2>&1 | grep -v -F "warning: TCG doesn't support requested feature" ||:
qemu-x86_64-static -cpu qemu64,+ssse3,+sse4.1,+sse4.2 $command --query "SELECT 1" 2>&1 | grep -v -F "warning: TCG doesn't support requested feature" ||:

