#!/usr/bin/env bash
# A long OR chain of raw `match()` predicates on the same expression. When `multiMatchAny` is not
# used (here the chain contains raw `match()` regexps, so it is kept off the Vectorscan path), the
# rewrite would merge the whole chain into a single combined `match(s, '(p0)|(p1)|...')` regexp.
# Each individual pattern compiles in RE2 on its own, but the merged alternation expands past RE2's
# default 8 MiB program budget (`RE2::Options::kDefaultMaxMem`) and `match` would throw
# `CANNOT_COMPILE_REGEXP`. With `optimize_or_like_chain` now enabled by default, the rewrite must not
# turn such a previously-working query into an exception: when the combined regexp does not compile it
# has to keep the original branches. Note the blow-up here comes from bounded repetition (`{1000}`),
# not from the text length, so a combined-regexp *length* cap would not catch it — only pre-compiling
# the merged regexp does. Verify the query succeeds and returns the same result as the un-rewritten OR
# chain, for both the new and the old analyzer.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_or_like_combined;
    CREATE TABLE t_or_like_combined (s String) ENGINE = Memory;
    -- Two rows match (the first two patterns), one row matches nothing.
    INSERT INTO t_or_like_combined SELECT concat('p', toString(number), repeat('z', 1000)) FROM numbers(2);
    INSERT INTO t_or_like_combined VALUES ('nothing matches here');
"

# Build an OR chain of 2000 raw match() predicates: match(s, 'p0z{1000}') OR ... OR match(s, 'p1999z{1000}').
# Each pattern compiles on its own (~1000 RE2 instructions for `z{1000}`), but the combined
# alternation is ~2,000,000 instructions, far above RE2's ~8 MiB program budget.
predicate=""
for i in $(seq 0 1999); do
    if [ -n "$predicate" ]; then
        predicate="$predicate OR "
    fi
    predicate="${predicate}match(s, 'p${i}z{1000}')"
done

query="SELECT count() FROM t_or_like_combined WHERE ${predicate}"

# Reference (rewrite disabled), then the rewrite enabled for both the old and the new analyzer.
# All three must succeed (no CANNOT_COMPILE_REGEXP) and return the same count.
${CLICKHOUSE_CLIENT} -q "${query} SETTINGS optimize_or_like_chain = 0"
${CLICKHOUSE_CLIENT} -q "${query} SETTINGS optimize_or_like_chain = 1, enable_analyzer = 0"
${CLICKHOUSE_CLIENT} -q "${query} SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_or_like_combined"
