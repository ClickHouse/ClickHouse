-- Pin the join order so each cell executes the plan its label names, and so the reference output does
-- not depend on a random seed. The bug reproduces at every value of both settings.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET join_use_nulls = 1;

DROP TABLE IF EXISTS l_one;
DROP TABLE IF EXISTS l_multi;
DROP TABLE IF EXISTS r_clean;
DROP TABLE IF EXISTS r_nan;
DROP TABLE IF EXISTS r_all_nan;
DROP TABLE IF EXISTS asof_oracle;

-- Single NaN left row: exercises the live matching path.
CREATE TABLE l_one (e UInt8, id UInt8, t Float64) ENGINE = Memory;
INSERT INTO l_one VALUES (1, 1, 5.0), (1, 2, 0. / 0.);

-- The NaN row sorts last, so the right stream is exhausted before it is reached and
-- full_sorting_merge matches it through the saved-state path instead of the live loop.
CREATE TABLE l_multi (e UInt8, id UInt8, t Float64) ENGINE = Memory;
INSERT INTO l_multi VALUES (1, 1, 1.5), (1, 2, 2.5), (1, 3, 10.0), (1, 4, 0. / 0.);

CREATE TABLE r_clean (e UInt8, t Float64, v UInt8) ENGINE = Memory;
INSERT INTO r_clean VALUES (1, 1.0, 11), (1, 2.0, 12), (1, 3.0, 13);

CREATE TABLE r_nan (e UInt8, t Float64, v UInt8) ENGINE = Memory;
INSERT INTO r_nan VALUES (1, 1.0, 11), (1, 2.0, 12), (1, 3.0, 13), (1, 0. / 0., 99);

CREATE TABLE r_all_nan (e UInt8, t Float64, v UInt8) ENGINE = Memory;
INSERT INTO r_all_nan VALUES (1, 0. / 0., 91), (1, 0. / 0., 92);

-- Expected answers, computed without a join from the NaN-free right table: the closest match by
-- ordering, or no match at all. A NaN left key satisfies no inequality, so its expectation is [].
CREATE TABLE asof_oracle (src String, op String, id UInt8, v Array(UInt8)) ENGINE = Memory;
INSERT INTO asof_oracle SELECT 'one', 'ge', l.id, if(countIf(r.t <= l.t) = 0, [], [argMaxIf(r.v, r.t, r.t <= l.t)]) FROM l_one AS l, r_clean AS r WHERE l.e = r.e GROUP BY l.id;
INSERT INTO asof_oracle SELECT 'one', 'gt', l.id, if(countIf(r.t <  l.t) = 0, [], [argMaxIf(r.v, r.t, r.t <  l.t)]) FROM l_one AS l, r_clean AS r WHERE l.e = r.e GROUP BY l.id;
INSERT INTO asof_oracle SELECT 'one', 'le', l.id, if(countIf(r.t >= l.t) = 0, [], [argMinIf(r.v, r.t, r.t >= l.t)]) FROM l_one AS l, r_clean AS r WHERE l.e = r.e GROUP BY l.id;
INSERT INTO asof_oracle SELECT 'one', 'lt', l.id, if(countIf(r.t >  l.t) = 0, [], [argMinIf(r.v, r.t, r.t >  l.t)]) FROM l_one AS l, r_clean AS r WHERE l.e = r.e GROUP BY l.id;
INSERT INTO asof_oracle SELECT 'multi', 'ge', l.id, if(countIf(r.t <= l.t) = 0, [], [argMaxIf(r.v, r.t, r.t <= l.t)]) FROM l_multi AS l, r_clean AS r WHERE l.e = r.e GROUP BY l.id;
INSERT INTO asof_oracle SELECT 'multi', 'gt', l.id, if(countIf(r.t <  l.t) = 0, [], [argMaxIf(r.v, r.t, r.t <  l.t)]) FROM l_multi AS l, r_clean AS r WHERE l.e = r.e GROUP BY l.id;
INSERT INTO asof_oracle SELECT 'multi', 'le', l.id, if(countIf(r.t >= l.t) = 0, [], [argMinIf(r.v, r.t, r.t >= l.t)]) FROM l_multi AS l, r_clean AS r WHERE l.e = r.e GROUP BY l.id;
INSERT INTO asof_oracle SELECT 'multi', 'lt', l.id, if(countIf(r.t >  l.t) = 0, [], [argMinIf(r.v, r.t, r.t >  l.t)]) FROM l_multi AS l, r_clean AS r WHERE l.e = r.e GROUP BY l.id;

SELECT 'oracle', src, op, id, v FROM asof_oracle ORDER BY src, op, id;

-- { echoOn }

-- Every ASOF answer over the NaN-bearing right table must equal the NaN-free expectation: a NaN in
-- the right key must not change what a finite left key matches, and a NaN left key must match
-- nothing. `mismatches` lists (id, expected, got) so a regression reads as a semantic diff.
SELECT 'one ge hash' AS cell, min(ok) AS all_ok, groupArrayIf((id, expected, got), NOT ok) AS mismatches FROM (SELECT o.id AS id, o.v AS expected, g.got AS got, (g.id IS NOT NULL) AND (o.v = g.got) AS ok FROM (SELECT id, v FROM asof_oracle WHERE src = 'one' AND op = 'ge') AS o LEFT JOIN (SELECT l.id AS id, groupArray(r.v) AS got FROM l_one AS l ASOF LEFT JOIN r_nan AS r ON l.e = r.e AND l.t >= r.t GROUP BY l.id) AS g ON o.id = g.id) SETTINGS join_algorithm = 'hash';
SELECT 'one gt hash' AS cell, min(ok) AS all_ok, groupArrayIf((id, expected, got), NOT ok) AS mismatches FROM (SELECT o.id AS id, o.v AS expected, g.got AS got, (g.id IS NOT NULL) AND (o.v = g.got) AS ok FROM (SELECT id, v FROM asof_oracle WHERE src = 'one' AND op = 'gt') AS o LEFT JOIN (SELECT l.id AS id, groupArray(r.v) AS got FROM l_one AS l ASOF LEFT JOIN r_nan AS r ON l.e = r.e AND l.t >  r.t GROUP BY l.id) AS g ON o.id = g.id) SETTINGS join_algorithm = 'hash';
SELECT 'one le hash' AS cell, min(ok) AS all_ok, groupArrayIf((id, expected, got), NOT ok) AS mismatches FROM (SELECT o.id AS id, o.v AS expected, g.got AS got, (g.id IS NOT NULL) AND (o.v = g.got) AS ok FROM (SELECT id, v FROM asof_oracle WHERE src = 'one' AND op = 'le') AS o LEFT JOIN (SELECT l.id AS id, groupArray(r.v) AS got FROM l_one AS l ASOF LEFT JOIN r_nan AS r ON l.e = r.e AND l.t <= r.t GROUP BY l.id) AS g ON o.id = g.id) SETTINGS join_algorithm = 'hash';
SELECT 'one lt hash' AS cell, min(ok) AS all_ok, groupArrayIf((id, expected, got), NOT ok) AS mismatches FROM (SELECT o.id AS id, o.v AS expected, g.got AS got, (g.id IS NOT NULL) AND (o.v = g.got) AS ok FROM (SELECT id, v FROM asof_oracle WHERE src = 'one' AND op = 'lt') AS o LEFT JOIN (SELECT l.id AS id, groupArray(r.v) AS got FROM l_one AS l ASOF LEFT JOIN r_nan AS r ON l.e = r.e AND l.t <  r.t GROUP BY l.id) AS g ON o.id = g.id) SETTINGS join_algorithm = 'hash';

SELECT 'one ge fsmj' AS cell, min(ok) AS all_ok, groupArrayIf((id, expected, got), NOT ok) AS mismatches FROM (SELECT o.id AS id, o.v AS expected, g.got AS got, (g.id IS NOT NULL) AND (o.v = g.got) AS ok FROM (SELECT id, v FROM asof_oracle WHERE src = 'one' AND op = 'ge') AS o LEFT JOIN (SELECT l.id AS id, groupArray(r.v) AS got FROM l_one AS l ASOF LEFT JOIN r_nan AS r ON l.e = r.e AND l.t >= r.t GROUP BY l.id) AS g ON o.id = g.id) SETTINGS join_algorithm = 'full_sorting_merge';
SELECT 'one gt fsmj' AS cell, min(ok) AS all_ok, groupArrayIf((id, expected, got), NOT ok) AS mismatches FROM (SELECT o.id AS id, o.v AS expected, g.got AS got, (g.id IS NOT NULL) AND (o.v = g.got) AS ok FROM (SELECT id, v FROM asof_oracle WHERE src = 'one' AND op = 'gt') AS o LEFT JOIN (SELECT l.id AS id, groupArray(r.v) AS got FROM l_one AS l ASOF LEFT JOIN r_nan AS r ON l.e = r.e AND l.t >  r.t GROUP BY l.id) AS g ON o.id = g.id) SETTINGS join_algorithm = 'full_sorting_merge';
SELECT 'one le fsmj' AS cell, min(ok) AS all_ok, groupArrayIf((id, expected, got), NOT ok) AS mismatches FROM (SELECT o.id AS id, o.v AS expected, g.got AS got, (g.id IS NOT NULL) AND (o.v = g.got) AS ok FROM (SELECT id, v FROM asof_oracle WHERE src = 'one' AND op = 'le') AS o LEFT JOIN (SELECT l.id AS id, groupArray(r.v) AS got FROM l_one AS l ASOF LEFT JOIN r_nan AS r ON l.e = r.e AND l.t <= r.t GROUP BY l.id) AS g ON o.id = g.id) SETTINGS join_algorithm = 'full_sorting_merge';
SELECT 'one lt fsmj' AS cell, min(ok) AS all_ok, groupArrayIf((id, expected, got), NOT ok) AS mismatches FROM (SELECT o.id AS id, o.v AS expected, g.got AS got, (g.id IS NOT NULL) AND (o.v = g.got) AS ok FROM (SELECT id, v FROM asof_oracle WHERE src = 'one' AND op = 'lt') AS o LEFT JOIN (SELECT l.id AS id, groupArray(r.v) AS got FROM l_one AS l ASOF LEFT JOIN r_nan AS r ON l.e = r.e AND l.t <  r.t GROUP BY l.id) AS g ON o.id = g.id) SETTINGS join_algorithm = 'full_sorting_merge';

SELECT 'one ge parallel_hash' AS cell, min(ok) AS all_ok, groupArrayIf((id, expected, got), NOT ok) AS mismatches FROM (SELECT o.id AS id, o.v AS expected, g.got AS got, (g.id IS NOT NULL) AND (o.v = g.got) AS ok FROM (SELECT id, v FROM asof_oracle WHERE src = 'one' AND op = 'ge') AS o LEFT JOIN (SELECT l.id AS id, groupArray(r.v) AS got FROM l_one AS l ASOF LEFT JOIN r_nan AS r ON l.e = r.e AND l.t >= r.t GROUP BY l.id) AS g ON o.id = g.id) SETTINGS join_algorithm = 'parallel_hash';

-- The right table is NaN-free here, so nothing but a NaN left key can go wrong. The NaN left row
-- sorts last, which is what routes full_sorting_merge through the saved-state path.
SELECT 'multi ge hash' AS cell, min(ok) AS all_ok, groupArrayIf((id, expected, got), NOT ok) AS mismatches FROM (SELECT o.id AS id, o.v AS expected, g.got AS got, (g.id IS NOT NULL) AND (o.v = g.got) AS ok FROM (SELECT id, v FROM asof_oracle WHERE src = 'multi' AND op = 'ge') AS o LEFT JOIN (SELECT l.id AS id, groupArray(r.v) AS got FROM l_multi AS l ASOF LEFT JOIN r_clean AS r ON l.e = r.e AND l.t >= r.t GROUP BY l.id) AS g ON o.id = g.id) SETTINGS join_algorithm = 'hash';
SELECT 'multi gt hash' AS cell, min(ok) AS all_ok, groupArrayIf((id, expected, got), NOT ok) AS mismatches FROM (SELECT o.id AS id, o.v AS expected, g.got AS got, (g.id IS NOT NULL) AND (o.v = g.got) AS ok FROM (SELECT id, v FROM asof_oracle WHERE src = 'multi' AND op = 'gt') AS o LEFT JOIN (SELECT l.id AS id, groupArray(r.v) AS got FROM l_multi AS l ASOF LEFT JOIN r_clean AS r ON l.e = r.e AND l.t >  r.t GROUP BY l.id) AS g ON o.id = g.id) SETTINGS join_algorithm = 'hash';
SELECT 'multi le hash' AS cell, min(ok) AS all_ok, groupArrayIf((id, expected, got), NOT ok) AS mismatches FROM (SELECT o.id AS id, o.v AS expected, g.got AS got, (g.id IS NOT NULL) AND (o.v = g.got) AS ok FROM (SELECT id, v FROM asof_oracle WHERE src = 'multi' AND op = 'le') AS o LEFT JOIN (SELECT l.id AS id, groupArray(r.v) AS got FROM l_multi AS l ASOF LEFT JOIN r_clean AS r ON l.e = r.e AND l.t <= r.t GROUP BY l.id) AS g ON o.id = g.id) SETTINGS join_algorithm = 'hash';
SELECT 'multi lt hash' AS cell, min(ok) AS all_ok, groupArrayIf((id, expected, got), NOT ok) AS mismatches FROM (SELECT o.id AS id, o.v AS expected, g.got AS got, (g.id IS NOT NULL) AND (o.v = g.got) AS ok FROM (SELECT id, v FROM asof_oracle WHERE src = 'multi' AND op = 'lt') AS o LEFT JOIN (SELECT l.id AS id, groupArray(r.v) AS got FROM l_multi AS l ASOF LEFT JOIN r_clean AS r ON l.e = r.e AND l.t <  r.t GROUP BY l.id) AS g ON o.id = g.id) SETTINGS join_algorithm = 'hash';

SELECT 'multi ge fsmj' AS cell, min(ok) AS all_ok, groupArrayIf((id, expected, got), NOT ok) AS mismatches FROM (SELECT o.id AS id, o.v AS expected, g.got AS got, (g.id IS NOT NULL) AND (o.v = g.got) AS ok FROM (SELECT id, v FROM asof_oracle WHERE src = 'multi' AND op = 'ge') AS o LEFT JOIN (SELECT l.id AS id, groupArray(r.v) AS got FROM l_multi AS l ASOF LEFT JOIN r_clean AS r ON l.e = r.e AND l.t >= r.t GROUP BY l.id) AS g ON o.id = g.id) SETTINGS join_algorithm = 'full_sorting_merge';
SELECT 'multi gt fsmj' AS cell, min(ok) AS all_ok, groupArrayIf((id, expected, got), NOT ok) AS mismatches FROM (SELECT o.id AS id, o.v AS expected, g.got AS got, (g.id IS NOT NULL) AND (o.v = g.got) AS ok FROM (SELECT id, v FROM asof_oracle WHERE src = 'multi' AND op = 'gt') AS o LEFT JOIN (SELECT l.id AS id, groupArray(r.v) AS got FROM l_multi AS l ASOF LEFT JOIN r_clean AS r ON l.e = r.e AND l.t >  r.t GROUP BY l.id) AS g ON o.id = g.id) SETTINGS join_algorithm = 'full_sorting_merge';
SELECT 'multi le fsmj' AS cell, min(ok) AS all_ok, groupArrayIf((id, expected, got), NOT ok) AS mismatches FROM (SELECT o.id AS id, o.v AS expected, g.got AS got, (g.id IS NOT NULL) AND (o.v = g.got) AS ok FROM (SELECT id, v FROM asof_oracle WHERE src = 'multi' AND op = 'le') AS o LEFT JOIN (SELECT l.id AS id, groupArray(r.v) AS got FROM l_multi AS l ASOF LEFT JOIN r_clean AS r ON l.e = r.e AND l.t <= r.t GROUP BY l.id) AS g ON o.id = g.id) SETTINGS join_algorithm = 'full_sorting_merge';
SELECT 'multi lt fsmj' AS cell, min(ok) AS all_ok, groupArrayIf((id, expected, got), NOT ok) AS mismatches FROM (SELECT o.id AS id, o.v AS expected, g.got AS got, (g.id IS NOT NULL) AND (o.v = g.got) AS ok FROM (SELECT id, v FROM asof_oracle WHERE src = 'multi' AND op = 'lt') AS o LEFT JOIN (SELECT l.id AS id, groupArray(r.v) AS got FROM l_multi AS l ASOF LEFT JOIN r_clean AS r ON l.e = r.e AND l.t <  r.t GROUP BY l.id) AS g ON o.id = g.id) SETTINGS join_algorithm = 'full_sorting_merge';

-- A right table of nothing but NaN can never satisfy an inequality.
SELECT 'all-nan right hash' AS cell, groupArray(r.v) AS got FROM (SELECT 1 AS e, 5.0 AS t) AS l ASOF JOIN r_all_nan AS r ON l.e = r.e AND l.t >= r.t SETTINGS join_algorithm = 'hash';
SELECT 'all-nan right fsmj' AS cell, groupArray(r.v) AS got FROM (SELECT 1 AS e, 5.0 AS t) AS l ASOF JOIN r_all_nan AS r ON l.e = r.e AND l.t >= r.t SETTINGS join_algorithm = 'full_sorting_merge';

-- Inner ASOF JOIN drops a NaN left row entirely, rather than extending it.
SELECT 'inner drops nan left' AS cell, count() AS rows FROM l_one AS l ASOF JOIN r_nan AS r ON l.e = r.e AND l.t >= r.t SETTINGS join_algorithm = 'hash';

-- Narrower float widths take the same code path.
SELECT 'Float32 fin ge' AS cell, groupArray(r.v) AS got FROM (SELECT toUInt8(1) AS e, toFloat32(5.0) AS t) AS l ASOF JOIN (SELECT toUInt8(1) AS e, toFloat32(arrayJoin([1.0, 2.0, 3.0, 0. / 0.])) AS t, toUInt8(if(isNaN(t), 99, 10 + t)) AS v) AS r ON l.e = r.e AND l.t >= r.t SETTINGS join_algorithm = 'hash';
SELECT 'Float32 nan ge' AS cell, groupArray(r.v) AS got FROM (SELECT toUInt8(1) AS e, toFloat32(0. / 0.) AS t) AS l ASOF JOIN (SELECT toUInt8(1) AS e, toFloat32(arrayJoin([1.0, 2.0, 3.0])) AS t, toUInt8(10 + t) AS v) AS r ON l.e = r.e AND l.t >= r.t SETTINGS join_algorithm = 'hash';
SELECT 'BFloat16 fin le' AS cell, groupArray(r.v) AS got FROM (SELECT toUInt8(1) AS e, toBFloat16(5.0) AS t) AS l ASOF JOIN (SELECT toUInt8(1) AS e, toBFloat16(arrayJoin([1.0, 2.0, 3.0, 0. / 0.])) AS t, toUInt8(if(isNaN(t), 99, 10 + t)) AS v) AS r ON l.e = r.e AND l.t <= r.t SETTINGS join_algorithm = 'hash';
SELECT 'BFloat16 nan ge' AS cell, groupArray(r.v) AS got FROM (SELECT toUInt8(1) AS e, toBFloat16(0. / 0.) AS t) AS l ASOF JOIN (SELECT toUInt8(1) AS e, toBFloat16(arrayJoin([1.0, 2.0, 3.0])) AS t, toUInt8(10 + t) AS v) AS r ON l.e = r.e AND l.t >= r.t SETTINGS join_algorithm = 'hash';

-- The three `markNaNsAsNull` template instantiations and its existing-null-map branch are only
-- reachable through `full_sorting_merge`; the cells above go through `RowRefs.cpp` instead. Each
-- shape below was measured to differ between an unfixed and a fixed build, unlike the `>=`
-- finite-left form, which is already correct on master and would assert nothing here.
SELECT 'Float32 fsmj' AS cell, groupArray(r.v) AS got FROM (SELECT toUInt8(1) AS e, toFloat32(5.0) AS t) AS l ASOF JOIN (SELECT toUInt8(1) AS e, toFloat32(arrayJoin([1.0, 2.0, 3.0, 0. / 0.])) AS t, toUInt8(if(isNaN(t), 99, 10 + t)) AS v) AS r ON l.e = r.e AND l.t <= r.t SETTINGS join_algorithm = 'full_sorting_merge';
SELECT 'BFloat16 fsmj' AS cell, groupArray(r.v) AS got FROM (SELECT toUInt8(1) AS e, toBFloat16(0. / 0.) AS t) AS l ASOF JOIN (SELECT toUInt8(1) AS e, toBFloat16(arrayJoin([1.0, 2.0, 3.0])) AS t, toUInt8(10 + t) AS v) AS r ON l.e = r.e AND l.t >= r.t SETTINGS join_algorithm = 'full_sorting_merge';
-- A `Nullable` asof key means the null map already exists, so this covers the branch that mutates it
-- instead of allocating one, and the `NaN` exclusion it detects answered 99 before. Within a single
-- equality-key run the `NaN` also skips the rest of the run, so the cell below is the one that pins
-- the composition itself.
SELECT 'Nullable fsmj' AS cell, groupArray(r.v) AS got FROM (SELECT toUInt8(1) AS e, toNullable(3.5) AS t) AS l ASOF JOIN (SELECT toUInt8(1) AS e, nullIf(arrayJoin([1.0, 2.0, 3.0, 4.5, 0. / 0.]), 4.5) AS t, toUInt8(multiIf(t IS NULL, 88, isNaN(t), 99, 10 + t)) AS v) AS r ON l.e = r.e AND l.t <= r.t SETTINGS join_algorithm = 'full_sorting_merge';
-- Cross equality key: the `NULL` asof row sits under `e = 1` and the `NaN` row under `e = 2`, so the
-- `NaN` cannot skip past the `NULL` row within one key run and the composed null map is what has to
-- reject it. Discarding the pre-existing `NULL` bits instead of composing with them would leave the
-- nested value 5.0 visible to `compareAt` and answer 88.
SELECT 'Nullable fsmj cross key' AS cell, groupArray(r.v) AS got FROM (SELECT toUInt8(1) AS e, toNullable(5.0) AS t) AS l ASOF JOIN (SELECT arrayJoin([(toUInt8(1), nullIf(5.0, 5.0)), (toUInt8(2), 0. / 0.)]) AS p, p.1 AS e, p.2 AS t, toUInt8(multiIf(t IS NULL, 88, isNaN(t), 99, 10 + t)) AS v) AS r ON l.e = r.e AND l.t >= r.t SETTINGS join_algorithm = 'full_sorting_merge';

-- Both exclusions active at once on the hash path: the `NULL` row is dropped by the pre-existing
-- filter in `HashJoin::addBlockToJoin` and the `NaN` row by the new `insert` guard, so 13 (the
-- largest finite value at or below 5.0) is the answer. This is a composition control, not a liveness
-- probe: an unfixed build answers 13 here in most runs anyway, because which wrong row a scrambled
-- lookup vector returns is itself nondeterministic. The cell below is the hash-path `Nullable` probe.
SELECT 'Nullable fin ge' AS cell, groupArray(r.v) AS got FROM (SELECT toUInt8(1) AS e, toNullable(5.0) AS t) AS l ASOF JOIN (SELECT toUInt8(1) AS e, arrayJoin([toNullable(1.0), 2.0, 3.0, NULL, 0. / 0.]) AS t, toUInt8(multiIf(t IS NULL, 88, isNaN(t), 99, 10 + t)) AS v) AS r ON l.e = r.e AND l.t >= r.t SETTINGS join_algorithm = 'hash';

-- A `Nullable` NaN on the PROBE side must match nothing on the hash path too: this is the only cell
-- that pins `SortedLookupVector::findAsof`'s guard for the `Nullable` wrapper. Measured `[13]` on an
-- unfixed build in 12 of 12 runs and `[]` here in 10 of 10, so it is discriminating and not flaky.
SELECT 'Nullable nan probe ge' AS cell, groupArray(r.v) AS got FROM (SELECT toUInt8(1) AS e, toNullable(0. / 0.) AS t) AS l ASOF JOIN (SELECT toUInt8(1) AS e, arrayJoin([toNullable(1.0), 2.0, 3.0, NULL]) AS t, toUInt8(multiIf(t IS NULL, 88, 10 + t)) AS v) AS r ON l.e = r.e AND l.t >= r.t SETTINGS join_algorithm = 'hash';

-- USING resolves to the same ASOF clause as ON.
SELECT 'USING nan left' AS cell, groupArray(r.v) AS got FROM (SELECT toUInt8(1) AS e, 0. / 0. AS t) AS l ASOF JOIN (SELECT toUInt8(1) AS e, arrayJoin([1.0, 2.0, 3.0]) AS t, toUInt8(7) AS v) AS r USING (e, t) SETTINGS join_algorithm = 'hash';

-- Types with no NaN representation, and float values that are not NaN, must keep matching.
SELECT 'Decimal64 fin ge' AS cell, groupArray(r.v) AS got FROM (SELECT toUInt8(1) AS e, toDecimal64(5.0, 3) AS t) AS l ASOF JOIN (SELECT toUInt8(1) AS e, toDecimal64(arrayJoin([1.0, 2.0, 3.0]), 3) AS t, toUInt8(13) AS v) AS r ON l.e = r.e AND l.t >= r.t SETTINGS join_algorithm = 'hash';
SELECT 'DateTime64 fin ge' AS cell, groupArray(r.v) AS got FROM (SELECT toUInt8(1) AS e, toDateTime64('2020-01-05 00:00:00', 3) AS t) AS l ASOF JOIN (SELECT toUInt8(1) AS e, toDateTime64(concat('2020-01-0', toString(number + 1), ' 00:00:00'), 3) AS t, toUInt8(number) AS v FROM numbers(3)) AS r ON l.e = r.e AND l.t >= r.t SETTINGS join_algorithm = 'hash';
SELECT 'signed zero ge' AS cell, groupArray(r.v) AS got FROM (SELECT toUInt8(1) AS e, -0.0 AS t) AS l ASOF JOIN (SELECT toUInt8(1) AS e, 0.0 AS t, toUInt8(5) AS v) AS r ON l.e = r.e AND l.t >= r.t SETTINGS join_algorithm = 'hash';
SELECT 'inf left ge' AS cell, groupArray(r.v) AS got FROM (SELECT toUInt8(1) AS e, 1 / 0. AS t) AS l ASOF JOIN (SELECT toUInt8(1) AS e, arrayJoin([1.0, 2.0, 3.0]) AS t, toUInt8(6) AS v) AS r ON l.e = r.e AND l.t >= r.t SETTINGS join_algorithm = 'hash';

-- Past 256 entries the lookup vector is built the same way for floats, since only non-float keys
-- take the radix sort path.
SELECT 'over 256 entries ge' AS cell, groupArray(r.v) AS got FROM (SELECT toUInt8(1) AS e, 500.0 AS t) AS l ASOF JOIN (SELECT toUInt8(1) AS e, if(number = 0, 0. / 0., toFloat64(number)) AS t, toUInt16(number) AS v FROM numbers(400)) AS r ON l.e = r.e AND l.t >= r.t SETTINGS join_algorithm = 'hash';
SELECT 'over 256 entries nan left' AS cell, groupArray(r.v) AS got FROM (SELECT toUInt8(1) AS e, 0. / 0. AS t) AS l ASOF JOIN (SELECT toUInt8(1) AS e, toFloat64(number) AS t, toUInt16(number) AS v FROM numbers(400)) AS r ON l.e = r.e AND l.t >= r.t SETTINGS join_algorithm = 'hash';

-- { echoOff }

DROP TABLE l_one;
DROP TABLE l_multi;
DROP TABLE r_clean;
DROP TABLE r_nan;
DROP TABLE r_all_nan;
DROP TABLE asof_oracle;
