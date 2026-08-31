#!/usr/bin/env bash
# Regression test for #105740 / #102460: `finalizeChunk` calls
# `ColumnAggregateFunction::convertToValues` for every `-State` aggregate
# column flowing through `CubeTransform`, `RollupTransform`,
# `TotalsHavingTransform`, or `AggregatingInOrderTransform`. Before #105749 the
# convertToValues loop pushed raw source-state pointers via
# `func->insertResultInto` and relied on `res.src` to keep the source column
# alive. The `-OrFill` flag=0 branch reset `res.src`, so any subsequent flag=1
# row would later read a destructed state (jemalloc free, ASan
# use-after-free for heap-owning states like `uniq`, MSan use-of-uninitialized
# for any State combinator) once the source column was dropped.
#
# Companion to `04275_105742_*` which covers the bare-MergeTree
# `RollupTransform` / `CubeTransform` paths. This test exercises the
# `TotalsHavingTransform` + `CubeTransform` + `RollupTransform WITH TOTALS`
# paths with `RIGHT JOIN` so the unmatched group has all-NULL inputs and the
# `-OrFill` flag=0 branch fires.
#
# Runs via `clickhouse-local` for the same reason as 04275 (see header).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
CREATE TABLE _s1 (g Nullable(UInt64), a Nullable(UInt64), b Nullable(UInt64)) ENGINE = Memory;
CREATE TABLE _s2 (x Nullable(UInt64)) ENGINE = Memory;
INSERT INTO _s1 VALUES (1, 1, 1), (1, 2, 2), (2, 3, 3), (NULL, NULL, NULL);
INSERT INTO _s2 VALUES (1), (2), (3), (4), (5), (6);

-- TotalsHavingTransform path. Exercises the -OrFill flag=0 branch via the
-- unmatched RIGHT JOIN rows. Does not crash on its own pre-fix; included so
-- the same JOIN+TOTALS shape from #105740 stays covered.
SELECT s1.g, uniqStateOrDefault(s1.b)
FROM _s1 AS s1 RIGHT JOIN _s2 ON s1.a = _s2.x
GROUP BY s1.g WITH TOTALS
ORDER BY s1.g ASC NULLS LAST
FORMAT Null;

-- MSan producer from #105740: singleValueOrNullStateOrDefault over the same
-- shape. The inner state does not own heap memory, so this case is MSan-only
-- pre-fix; on release builds it returns cleanly.
SELECT s1.g, singleValueOrNullStateOrDefault(s1.b)
FROM _s1 AS s1 RIGHT JOIN _s2 ON s1.a = _s2.x
GROUP BY s1.g WITH TOTALS
ORDER BY s1.g ASC NULLS LAST
FORMAT Null;

-- CUBE variant: routes through CubeTransform::generate -> finalizeChunk and
-- triggers the convertToValues SEGV pre-fix on release builds.
SELECT s1.g, uniqStateOrDefault(s1.b)
FROM _s1 AS s1 RIGHT JOIN _s2 ON s1.a = _s2.x
GROUP BY s1.g WITH CUBE
ORDER BY s1.g ASC NULLS LAST
FORMAT Null;

-- argMaxStateOrDefault is another heap-owning State combinator; same
-- convertToValues path, combined with WITH ROLLUP WITH TOTALS to exercise
-- both RollupTransform and TotalsHavingTransform together.
SELECT s1.g, argMaxStateOrDefault(s1.b, s1.a)
FROM _s1 AS s1 RIGHT JOIN _s2 ON s1.a = _s2.x
GROUP BY s1.g WITH ROLLUP WITH TOTALS
ORDER BY s1.g DESC NULLS FIRST
FORMAT Null;

SELECT 'ok';
"
