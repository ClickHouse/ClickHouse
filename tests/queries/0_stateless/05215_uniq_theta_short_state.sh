#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest -- compiled w/o datasketches

# `compact_theta_sketch_parser::parse` verifies that the buffer holds 8 bytes and then reads header
# fields beyond them - `num_entries` at offset 8, `theta` at offset 16 - before it checks the size
# again. A state shorter than that, which any `CAST` from a string can produce, was therefore read past
# its end (an ASan heap-buffer-overflow), and the out-of-bounds value decided the size the parser
# reported as missing: the same query answered `at least 86160 bytes expected` on one run and something
# else on the next. The size is now the one the parser really needs before it can validate anything.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for STATE in "char(8, 0, 1, 3, 0, 0, 0, 0, 0)" "char(8, 0, 2, 3, 0, 0, 0, 0, 0)" "char(12, 0, 3, 3, 0, 0, 0, 0, 0, 1, 0, 0, 0)" "char(8, 0, 4, 3, 0, 0, 0, 0, 0)"
do
    if ${CLICKHOUSE_CLIENT} --query "SELECT CAST(${STATE}, 'AggregateFunction(uniqTheta, UInt64)')" 2>&1 | grep -q -F "Code: 246"
    then
        echo "rejected ${STATE}"
    else
        echo "accepted ${STATE}"
    fi
done

# The serial version 1 state is eight bytes long and the parser needs twenty-four of them, whatever
# follows the allocation. The size is read through `clickhouse-local`, which prints the message of the
# exception rather than only its code.
${CLICKHOUSE_LOCAL} --query "SELECT CAST(char(8, 0, 1, 3, 0, 0, 0, 0, 0), 'AggregateFunction(uniqTheta, UInt64)')" 2>&1 \
    | grep -o -F "at least 24 bytes expected, actual 8"

# A state that a `uniqTheta` aggregation wrote still reads back.
${CLICKHOUSE_CLIENT} --query "SELECT uniqTheta(number) FROM numbers(1000)"
${CLICKHOUSE_CLIENT} --query "SELECT uniqThetaMerge(s) FROM (SELECT uniqThetaState(number) AS s FROM numbers(1000))"
${CLICKHOUSE_CLIENT} --query "SELECT finalizeAggregation(CAST(unhex(hex(uniqThetaState(number))), 'AggregateFunction(uniqTheta, UInt64)')) FROM numbers(100)"
