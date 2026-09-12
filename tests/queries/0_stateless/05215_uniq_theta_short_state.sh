#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest -- compiled w/o datasketches

# `compact_theta_sketch_parser::parse` verifies that the buffer holds 8 bytes and then reads header
# fields that lie beyond them - `num_entries` at offset 8, `theta` at offset 16 - before it checks the
# size again. A state shorter than that, which any `CAST` from a string can produce, was therefore read
# past its end (an ASan heap-buffer-overflow), and the out-of-bounds value decided the size the parser
# reported as missing: the same query answered `at least 86160 bytes expected` on one run and something
# else on the next. Every size below is now the one the parser really needs before it can validate
# anything, and the same on every run.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The outcome of one state, reduced to the part that used to come from outside the buffer. Read through
# `clickhouse-local`, which prints the message of the exception rather than only its code.
outcome()
{
    ${CLICKHOUSE_LOCAL} --query "SELECT finalizeAggregation(CAST($1, 'AggregateFunction(uniqTheta, UInt64)'))" 2>&1 \
        | grep -o -E "at least [0-9]+ bytes expected, actual [0-9]+|seed hash mismatch|unsupported serial version [0-9]+|^[0-9]+$" \
        | head -n 1
}

# The leading byte of each `char` is the length of the state that follows it. All of these states are
# eight bytes long - the most the parser checks for before it reads further out.
#
# The first four stop at `check_seed_hash`, which is what the fuzzer produced. Only serial version 1
# computes the seed hash itself instead of comparing it, so only it reaches the premature reads.
for STATE in \
    "char(8, 0, 1, 3, 0, 0, 0, 0, 0)" \
    "char(8, 0, 2, 3, 0, 0, 0, 0, 0)" \
    "char(8, 0, 3, 3, 0, 0, 0, 0, 0)" \
    "char(8, 0, 4, 3, 0, 0, 0, 0, 0)" \
    "char(8, 0, 5, 3, 0, 0, 0, 0, 0)"
do
    echo "${STATE} -> $(outcome "${STATE}")"
done

# `204, 147` at offset 6 is the seed hash of the default seed, so these states get past
# `check_seed_hash` and reach every one of the reads that happen before the size is validated:
# `num_entries` alone for two preamble longs, `num_entries` and `theta` for three.
for STATE in \
    "char(8, 2, 2, 3, 0, 0, 0, 204, 147)" \
    "char(8, 3, 2, 3, 0, 0, 0, 204, 147)" \
    "char(8, 2, 3, 3, 0, 0, 0, 204, 147)" \
    "char(8, 3, 3, 3, 0, 0, 0, 204, 147)" \
    "char(8, 2, 4, 3, 0, 0, 0, 204, 147)"
do
    echo "${STATE} -> $(outcome "${STATE}")"
done

# A state that a `uniqTheta` aggregation wrote still reads back.
${CLICKHOUSE_CLIENT} --query "SELECT uniqTheta(number) FROM numbers(1000)"
${CLICKHOUSE_CLIENT} --query "SELECT uniqThetaMerge(s) FROM (SELECT uniqThetaState(number) AS s FROM numbers(1000))"
${CLICKHOUSE_CLIENT} --query "SELECT finalizeAggregation(CAST(unhex(hex(uniqThetaState(number))), 'AggregateFunction(uniqTheta, UInt64)')) FROM numbers(100)"

# A short state cannot simply be rejected, which is why the buffer is padded rather than refused: a
# single-entry sketch is sixteen bytes long, below the twenty-four the parser reads before it validates
# anything. The length of each state is printed next to the value it reads back as - it counts the
# one-byte length prefix, so the sketch itself is one byte smaller.
for ROWS in 0 1 2
do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT
            intDiv(length(hex(uniqThetaState(number))), 2) AS serialized_size,
            finalizeAggregation(CAST(unhex(hex(uniqThetaState(number))), 'AggregateFunction(uniqTheta, UInt64)'))
        FROM numbers(${ROWS})"
done
