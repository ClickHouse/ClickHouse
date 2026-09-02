#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# check for buffer overflow in unbin (due to not enough memory preallocate for output buffer)
# we iterate over all remainders of input string length modulo word_size and check that no assertions are triggered

word_size=8
for i in $(seq 1 $((word_size+1))); do
  str=$(printf "%${i}s" | tr ' ' 'x')
  $CLICKHOUSE_CLIENT -q "SELECT count() FROM numbers(99) GROUP BY unbin(toFixedString(materialize('$str'), $i)) WITH ROLLUP WITH TOTALS FORMAT NULL"
done

word_size=8
for i in $(seq 1 $((word_size+1))); do
  str=$(printf "%${i}s" | tr ' ' 'x')
  $CLICKHOUSE_CLIENT -q "SELECT count() FROM numbers(99) GROUP BY unbin(materialize('$str')) WITH ROLLUP WITH TOTALS FORMAT NULL"
done

word_size=2
for i in $(seq 1 $((word_size+1))); do
  str=$(printf "%${i}s" | tr ' ' 'x')
  $CLICKHOUSE_CLIENT -q "SELECT count() FROM numbers(99) GROUP BY unhex(toFixedString(materialize('$str'), $i)) WITH ROLLUP WITH TOTALS FORMAT NULL"
done

word_size=2
for i in $(seq 1 $((word_size+1))); do
  str=$(printf "%${i}s" | tr ' ' 'x')
  $CLICKHOUSE_CLIENT -q "SELECT count() FROM numbers(99) GROUP BY unhex(materialize('$str')) WITH ROLLUP WITH TOTALS FORMAT NULL"
done
