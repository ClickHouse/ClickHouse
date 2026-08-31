#!/usr/bin/env bash
# Regression test for #105742: SEGV in `UniquesHashSet::merge` when sorting an
# aggregate-state column produced by `WITH ROLLUP` / `CUBE` + `ORDER BY` over
# `uniqStateOrDefault` (where some groups have no input rows, so the `-OrFill`
# flag byte is 0). See the PR description for the full root cause.
#
# Runs the crashing queries via `clickhouse-local` so on master (without the
# fix) the SEGV exits the subprocess with a non-zero code and the test fails
# cleanly via `EXIT_CODE`. A `.sql` regression test crashes the shared server,
# which makes Bugfix validation race with the runner's parallel cleanup and
# misclassify the failure as "test runner terminated unexpectedly" instead of
# a reproduced bug.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
CREATE TABLE _r (k UInt32, v Nullable(Int64), g UInt8) ENGINE = MergeTree ORDER BY k;
INSERT INTO _r SELECT number, if(number%3 = 0, NULL, number*10), number%3 FROM numbers(100);

SELECT g, uniqStateOrDefault(v)
FROM _r
GROUP BY g WITH ROLLUP
ORDER BY g DESC
FORMAT Null;

SELECT g, uniqStateOrDefault(v)
FROM _r
GROUP BY g WITH ROLLUP WITH TOTALS
ORDER BY g DESC NULLS FIRST
FORMAT Null;

SELECT g, uniqStateOrDefault(v)
FROM _r
GROUP BY g WITH CUBE
ORDER BY g DESC
FORMAT Null;

-- Bare uniqState shares the convertToValues path.
SELECT g, uniqState(toUInt32(k))
FROM _r
GROUP BY g WITH ROLLUP
ORDER BY g DESC
FORMAT Null;

SELECT 'ok';
"
