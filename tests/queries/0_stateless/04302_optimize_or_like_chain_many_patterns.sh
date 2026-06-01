#!/usr/bin/env bash
# A long OR chain of more than 255 pure-substring LIKE predicates on the same expression used to
# be rewritten by `optimize_or_like_chain` into a single `multiSearchAny()` call. `multiSearchAny`
# throws TOO_MANY_ARGUMENTS_FOR_FUNCTION for constant needle arrays larger than UInt8::max (255),
# so a default-on rewrite turned a previously-working query into an exception. Now such chains fall
# through to the `multiMatchAny`/combined-`match` path, which has no such cap. Verify that the query
# succeeds and returns the same result as the un-rewritten OR chain, for both analyzers.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_or_like_many;
    CREATE TABLE t_or_like_many (s String) ENGINE = Memory;
    INSERT INTO t_or_like_many VALUES ('xx_needle_005_xx'), ('nothing matches here'), ('end_needle_299'), ('plain string');
"

# Build an OR chain of 300 pure-substring LIKE predicates ('%needle_000%' OR ... OR '%needle_299%').
predicate=""
for i in $(seq -w 0 299); do
    if [ -n "$predicate" ]; then
        predicate="$predicate OR "
    fi
    predicate="${predicate}s LIKE '%needle_${i}%'"
done

query="SELECT count() FROM t_or_like_many WHERE ${predicate}"

# Reference (rewrite disabled), then the rewrite enabled for both the old and the new analyzer.
${CLICKHOUSE_CLIENT} -q "${query} SETTINGS optimize_or_like_chain = 0"
${CLICKHOUSE_CLIENT} -q "${query} SETTINGS optimize_or_like_chain = 1, optimize_or_like_chain_min_patterns = 1, enable_analyzer = 0"
${CLICKHOUSE_CLIENT} -q "${query} SETTINGS optimize_or_like_chain = 1, optimize_or_like_chain_min_patterns = 1, enable_analyzer = 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_or_like_many"
