#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --query "
    SELECT count()
    FROM numbers(30000)
    WHERE and(
        number = 0,
        [number] + if(number = 0, [number], [number, number]) = [0])
    SETTINGS
        enable_adaptive_short_circuit_lazy_execution = 1,
        short_circuit_function_evaluation = 'force_enable'"
