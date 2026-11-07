#!/usr/bin/env bash
# A long OR chain of more than 255 pure-substring LIKE predicates on the same expression. Without
# Aho-Corasick support, `multiSearchAny` throws NOT_IMPLEMENTED for constant needle arrays larger
# than UInt8::max (255), so `optimize_or_like_chain` must not rewrite the chain into it: the chain
# falls through to the `multiMatchAny` path (Vectorscan). Verify that the query succeeds and returns
# the same result as the un-rewritten OR chain, for both analyzers.
#
# The needles must be *pure* substrings: no LIKE metacharacters (`_`, `%`) and no regexp
# metacharacters inside. A stray `_` would make the pattern a wildcard, so it would no longer take
# the `multiSearchAny` path this test targets, and the rewrite would instead build 300 `.`-wildcard
# regexps whose Hyperscan compilation is ~70x slower (enough to blow the flaky-check 180s budget).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_or_like_many;
    CREATE TABLE t_or_like_many (s String) ENGINE = Memory;
    INSERT INTO t_or_like_many VALUES ('xxneedle005xx'), ('nothing matches here'), ('endneedle299'), ('plain string');
"

# Build an OR chain of 300 pure-substring LIKE predicates ('%needle000%' OR ... OR '%needle299%').
predicate=""
for i in $(seq -w 0 299); do
    if [ -n "$predicate" ]; then
        predicate="$predicate OR "
    fi
    predicate="${predicate}s LIKE '%needle${i}%'"
done

query="SELECT count() FROM t_or_like_many WHERE ${predicate}"

# Reference (rewrite disabled), then the rewrite enabled for both the old analyzer and the analyzer.
${CLICKHOUSE_CLIENT} -q "${query} SETTINGS optimize_or_like_chain = 0"
${CLICKHOUSE_CLIENT} -q "${query} SETTINGS optimize_or_like_chain = 1, optimize_or_like_chain_min_patterns = 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_or_like_many"
