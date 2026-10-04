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

# The outcome of one state, reduced to the part that used to come from outside the buffer. The client
# prints the message of the exception rather than only its code.
outcome()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT finalizeAggregation(CAST($1, 'AggregateFunction(uniqTheta, UInt64)'))" 2>&1 \
        | grep -o -E "at least [0-9]+ bytes expected, actual [0-9]+|seed hash mismatch|unsupported serial version [0-9]+|^[0-9]+$" \
        | head -n 1
}

# The leading byte of each `char` is the length of the state that follows it. All of these states are
# eight bytes long - the most the parser checks for before it reads further out.
#
# The four states the fuzzer produced come first, and their seed hash is zero. Serial versions 2 and 4
# compare that against the one they compute and stop at `check_seed_hash`; serial version 1 computes
# the seed hash itself instead of comparing it, and serial version 3 reads `num_entries` before it
# compares, so those two are refused for their size. Serial version 5 does not exist and is refused
# outright.
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
# `num_entries` alone for two preamble longs, `num_entries` and `theta` for three. In particular a
# serial version 2 state with two preamble longs is refused rather than taken for an empty sketch on
# the strength of bytes it does not have.
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
${CLICKHOUSE_CLIENT} --query "
    SELECT uniqTheta(number) FROM numbers(1000);
    SELECT uniqThetaMerge(s) FROM (SELECT uniqThetaState(number) AS s FROM numbers(1000));
    SELECT finalizeAggregation(CAST(unhex(hex(uniqThetaState(number))), 'AggregateFunction(uniqTheta, UInt64)')) FROM numbers(100);
"

# A short state cannot be refused for its length alone: a single-entry sketch is sixteen bytes long,
# below the twenty-four the parser reads for other headers before it validates anything. The length of
# each state is printed next to the value it reads back as - it counts the one-byte length prefix, so
# the sketch itself is one byte smaller.
${CLICKHOUSE_CLIENT} --query "
    SELECT
        intDiv(length(hex(s)), 2) AS serialized_size,
        finalizeAggregation(CAST(unhex(hex(s)), 'AggregateFunction(uniqTheta, UInt64)'))
    FROM
    (
        SELECT rows, uniqThetaStateIf(number, number < rows) AS s
        FROM numbers(2) AS n, (SELECT arrayJoin([0, 1, 2]) AS rows) AS r
        GROUP BY rows
        ORDER BY rows
    )"
