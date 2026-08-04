-- { echo }

-- A cross-side equality is extracted into a JOIN key and the conjunct above the join is replaced by
-- a constant, on the assumption that the key already enforced it. For an expression that is not
-- deterministic in the scope of the query, or that is stateful, that assumption is false: the key
-- draws its own value, so the surviving rows do not satisfy the predicate the user wrote.

SET enable_analyzer = 1;                          -- the pass only sees JoinStepLogical
SET enable_parallel_replicas = 0;                 -- ditto
SET query_plan_join_swap_table = 0;               -- a swap changes which side is which
SET query_plan_optimize_join_order_randomize = 0; -- the plan-shape rows assert on join order
SET enable_join_runtime_filters = 0;              -- a runtime filter adds terms to the plan text
SET explain_query_plan_default = 'legacy';        -- `Clauses:` is only printed by the legacy format
-- The runner randomizes this one over {0, 1}. At 0 the whole query below a `remote()` side ships to
-- the replica, so the initiator plan is a bare `ReadFromRemote` with no join in it and no `Clauses:`
-- line at all, which would make L7, L8 and C7 pass whatever the guard does.
SET prefer_localhost_replica = 1;

-- `tz`, `hdr` and `disk` carry the non-constant arguments L11, L12 and L20 need: with all arguments
-- constant those calls are folded before the pass runs and the rows would be vacuous.
CREATE TABLE l (k UInt32, a UInt32, an Nullable(UInt32), tz String, hdr String, disk String) ENGINE = Log;
CREATE TABLE r (k UInt32, b UInt8, bn Nullable(UInt8)) ENGINE = Log;
INSERT INTO l SELECT number % 16, number, number, 'UTC', 'Content-Type', 'default' FROM numbers(20000);
INSERT INTO r SELECT number % 16, number % 16, number % 16 FROM numbers(320);

-- Heavily overlapping intervals so runningConcurrency ramps over the whole 0..15 range, and a
-- 2000 x 16 fan-out on a single key: with a small fan-out two independent draws of a 16-valued
-- expression almost never coincide and every arm returns 0 rows, which is silently vacuous.
CREATE TABLE lc (k UInt32, s DateTime, e DateTime) ENGINE = Log;
CREATE TABLE rc (k UInt32, b UInt8) ENGINE = Log;
INSERT INTO lc SELECT 1, toDateTime(1700000000 + intDiv(number, 4)), toDateTime(1700000000 + intDiv(number, 4) + 3000) FROM numbers(2000);
INSERT INTO rc SELECT 1, number % 16 FROM numbers(16);

CREATE TABLE lt (k UInt32, id UInt64) ENGINE = Log;
CREATE TABLE rt (k UInt32, b UInt8) ENGINE = Log;
INSERT INTO lt SELECT number % 100, number FROM numbers(2000);
INSERT INTO rt SELECT number, number % 16 FROM numbers(100);

-- L14 needs a right-side column that is physically Sparse, which only a MergeTree part can be: the
-- kind is chosen per part from ratio_of_defaults_for_sparse_serialization, so it is pinned at the
-- table level because --random-merge-tree-settings otherwise redraws it over the whole 0..1 range.
-- `s` must also be a VARIABLE-size type: for a fixed-size one `byteSize` returns a constant derived
-- from the type without ever reading the column (`byteSize.cpp:45-61`
-- `isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion`), so a UInt64 right side would
-- make the row silently vacuous. `lb.x = 11` is the DENSE byteSize of 'abc' (8 + 3), which is what
-- the join produces and therefore what the user's predicate means; read on the still-sparse column
-- the same value reports 19, because ColumnSparse charges a non-default value an extra
-- `sizeof(UInt64)` for its offset. That 11-vs-19 gap is the whole defect.
CREATE TABLE lb (k UInt32, x UInt64) ENGINE = Log;
CREATE TABLE rb (k UInt32, s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;
INSERT INTO lb SELECT number % 8, 11 FROM numbers(80);
INSERT INTO rb SELECT number % 8, if(number % 63 = 0, 'abc', '') FROM numbers(1024);

CREATE DICTIONARY dic (key UInt32, val UInt8) PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'r' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0);

-- L16 and L17 need a Join-engine table, the only storage `joinGet` accepts. `jtn` sets join_use_nulls
-- so that `joinGet` over it resolves to the EFFECTIVE name `joinGetOrNull`
-- (`FunctionJoinGet.cpp:203-208`), which is what makes the second listed name reachable rather than
-- dead text. The key type must be exactly UInt32: joinGet rejects a mismatch outright
-- (`Type mismatch in joinGet key 0`), so the probe columns below are cast rather than reduced modulo.
CREATE TABLE jt (k UInt32, v UInt8) ENGINE = Join(ANY, LEFT, k);
CREATE TABLE jtn (k UInt32, v UInt8) ENGINE = Join(ANY, LEFT, k) SETTINGS join_use_nulls = 1;
INSERT INTO jt SELECT number, number % 16 FROM numbers(100);
INSERT INTO jtn SELECT number, number % 16 FROM numbers(100);

-- Fixture premise: runningConcurrency must span the whole modulus, else L5 goes vacuous.
SELECT uniqExact(v) = 16 AND min(v) = 0 AND max(v) = 15 FROM (SELECT toUInt8(runningConcurrency(s, e) % 16) AS v FROM lc);

-- The oracle re-selects the non-deterministic side alongside the joined column and counts the output
-- rows that violate their own WHERE. It needs no assumption about how many rows survive: a row count
-- here is Poisson and reaches 0 by chance on a small fixture, which silently passes any
-- `count() > 0` guard. Pre-fix roughly 94% of the emitted rows are violations; the fix makes it 0.
-- The companion `count() > 20000` keeps the row non-vacuous (measured minimum over 25 draws: ~24700,
-- stable at every `max_block_size`).
-- `query_plan_merge_filter_into_join_condition` is pinned on every row because the functional runner
-- randomizes it off on ~5% of draws, which would make these rows vacuous.

-- L1: non-deterministic on the left.
SELECT countIf(x != rb) = 0 AND count() > 20000 FROM (
    SELECT toUInt8(rand(l.a) % 16) AS x, r.b AS rb FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8(rand(l.a) % 16) = r.b)
SETTINGS query_plan_merge_filter_into_join_condition = 1;

-- L2: non-deterministic on the right. A guard applied only to the left operand would let this
-- through, so this row is what makes the symmetry of the guard load-bearing.
SELECT countIf(x != la) = 0 AND count() > 20000 FROM (
    SELECT toUInt8(rand(r.b) % 16) AS x, toUInt8(l.a % 16) AS la FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8(l.a % 16) = toUInt8(rand(r.b) % 16))
SETTINGS query_plan_merge_filter_into_join_condition = 1;

-- L3: rowNumberInAllBlocks, a non-deterministic function that is not rand.
SELECT countIf(x != rb) = 0 AND count() > 20000 FROM (
    SELECT toUInt8((rowNumberInAllBlocks() + l.a) % 16) AS x, r.b AS rb FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8((rowNumberInAllBlocks() + l.a) % 16) = r.b)
SETTINGS query_plan_merge_filter_into_join_condition = 1;

-- L4: Nullable operands.
SELECT countIf(x != rb) = 0 AND count() > 20000 FROM (
    SELECT toNullable(toUInt8(rand(l.an) % 16)) AS x, r.bn AS rb FROM l INNER JOIN r ON l.k = r.k
    WHERE toNullable(toUInt8(rand(l.an) % 16)) = r.bn)
SETTINGS query_plan_merge_filter_into_join_condition = 1;

-- L5: runningConcurrency is stateful yet inherits isDeterministicInScopeOfQuery() == true, so a
-- determinism-only guard would still extract it. Here the whole result set is spurious: the correct
-- answer is empty, and pre-fix 2000 rows are emitted.
SELECT count() = 0 FROM (
    SELECT rc.b AS rb FROM lc INNER JOIN rc ON lc.k = rc.k
    WHERE toUInt8(runningConcurrency(lc.s, lc.e) % 16) = rc.b)
SETTINGS query_plan_merge_filter_into_join_condition = 1;

-- L6: timeSeriesStoreTags is the second stateful function that does not override the determinism
-- predicate. Its side effect (populating the per-query tags collector) makes it order-dependent.
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT rt.b FROM lt INNER JOIN rt ON lt.k = rt.k
    WHERE toUInt8(timeSeriesStoreTags(lt.id, [('a', 'b')]) % 16) = rt.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- L7: a server constant is fixed per executing node, not per query. The two sides of a promoted key
-- are materialized below the scatter exchanges, so a node that deserializes the plan builds its own
-- function object and reads its own value, while the conjunct that would have rechecked the user's
-- predicate has been overwritten with a constant. The `remote()` side is what makes the class
-- observable at all: `FunctionConstantBase::isSuitableForConstantFolding` is `!is_distributed`, so on
-- a single-node query `hostName()` is folded to a constant before the pass runs and never reaches
-- the guard, which is why a naive single-node row would pass unfixed and be silently vacuous.
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM remote('127.0.0.1', currentDatabase(), l) AS lr
    INNER JOIN r ON lr.k = r.k
    WHERE toUInt8((length(hostName()) + lr.a) % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- L8: queryID is in the same class but does not report isServerConstant(), so it is refused by the
-- name list rather than by the flag. This row is what makes the list separately load-bearing.
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM remote('127.0.0.1', currentDatabase(), l) AS lr
    INNER JOIN r ON lr.k = r.k
    WHERE toUInt8((length(queryID()) + lr.a) % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- L9: randConstant draws its value while the function object is being built, and a plan sent to
-- another node is rebuilt there from function names, so the two sides of a promoted key hold
-- different values. The argument is what makes it reach the guard: with no argument the call is
-- constant-folded before the pass runs, so a zero-argument row would pass on an unfixed build.
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM remote('127.0.0.1', currentDatabase(), l) AS lr
    INNER JOIN r ON lr.k = r.k
    WHERE toUInt8(randConstant(lr.a) % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- L10: isConstant reports on the physical representation of its argument, not on its value, so it is
-- not a value that survives being moved into a key. The join materializes a right-side constant
-- column (`materializeColumnsFromRightBlock` converts it to a full column), so at the key position
-- isConstant reads 1 while the conjunct above the join would have read 0 -- and this pass has
-- overwritten that conjunct with a constant. `IFunction.h` names isConstant and toColumnTypeName
-- verbatim as the counterexamples to "same value, different constness => same result".
-- Unlike L6-L9 this one is a wrong-results row, not a plan-text row: pre-fix the whole result set is
-- lost (0 rows emitted where 400000 are correct), which is why the oracle is a row count against the
-- unmerged form rather than a `Clauses:` assertion.
SELECT count() = 400000 FROM (
    SELECT 1 FROM l INNER JOIN (SELECT k, b, 7 AS c FROM r) AS rq ON l.k = rq.k
    WHERE toUInt8(l.a % 16) = toUInt8((isConstant(rq.c) + rq.b) % 16))
SETTINGS query_plan_merge_filter_into_join_condition = 1;

-- L11: now() and now64() bind the clock reading while the function object is built, exactly like
-- randConstant, so a plan fragment rebuilt on another node holds a different value. Their ordinary
-- zero-argument form is folded before this pass runs and stays eligible (C2, C8); a non-constant
-- timezone argument, which allow_nonconst_timezone_arguments permits, is what keeps them live as
-- function nodes and reaching the guard. Modelled on 00515_enhanced_time_zones.sql.
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM remote('127.0.0.1', currentDatabase(), l) AS lr
    INNER JOIN r ON lr.k = r.k
    WHERE toUInt8((toHour(now(lr.tz)) + lr.a) % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1, allow_nonconst_timezone_arguments = 1);
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM remote('127.0.0.1', currentDatabase(), l) AS lr
    INNER JOIN r ON lr.k = r.k
    WHERE toUInt8((toHour(now64(9, lr.tz)) + lr.a) % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1, allow_nonconst_timezone_arguments = 1);

-- L12: getClientHTTPHeader answers from the request that reached the node its function object was
-- built on -- its own documentation states it returns a non-empty result only on the initiator -- so
-- a rebuilt object on another node yields a different value. The argument must be non-constant: with
-- a literal header name the call is folded to a constant before the pass runs, so a literal-argument
-- row would pass on an unfixed build and be silently vacuous.
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM remote('127.0.0.1', currentDatabase(), l) AS lr
    INNER JOIN r ON lr.k = r.k
    WHERE toUInt8((length(getClientHTTPHeader(lr.hdr)) + lr.a) % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1, allow_get_client_http_header = 1);

-- L13: arrayJoin is not a FUNCTION node in an ActionsDAG -- addArrayJoin gives it its own ARRAY_JOIN
-- type and leaves function_base unset -- so the type test in the guard could not see it, although it
-- reports isDeterministicInScopeOfQuery() == false like every other member of the first class.
-- Promoting it duplicates rows rather than returning a wrong value: removeUnusedActions keeps the
-- expansion above the join ("We cannot remove arrayJoin because it changes the number of rows"), so
-- the cloned key expands the array a second time and the plan holds two ARRAY JOIN nodes. The sibling
-- pass partialJoinFilterPushDown refuses the same subgraph for the same reason.
-- Like L10 this is a wrong-results row: pre-fix it emits 800000 rows where 400000 are correct.
SELECT count() = 400000 FROM (
    SELECT 1 FROM l INNER JOIN r ON l.k = r.k
    WHERE arrayJoin([l.a % 16, (l.a + 1) % 16]) = r.b)
SETTINGS query_plan_merge_filter_into_join_condition = 1;

-- L14: constness is not the only physical representation the JOIN removes. Two lines below the
-- `convertToFullColumnIfConst` that makes L10 real, `materializeColumnsFromRightBlock` also calls
-- `recursiveRemoveSparse` ("Sparse columns are not supported on the right side"), and `byteSize`
-- keeps `useDefaultImplementationForSparseColumns() == false` so it is handed the sparse column
-- itself. `ColumnSparse::byteSizeAt` charges a non-default value an extra `sizeof(UInt64)`, so a
-- promoted key reads 19 for 'abc' where the conjunct above the join reads the dense 11 -- and this
-- pass has overwritten that conjunct with a constant. `byteSize` overrides none of the determinism,
-- statefulness or constant-folding predicates, so only the name list can refuse it.
-- Like L10 and L13 this is a wrong-results row: pre-fix the whole result set is lost (0 rows emitted
-- where 170 = 17 non-default values x 10 left rows per key are correct). The first row asserts the
-- fixture premise, because a part that silently came out `Default` would make the row vacuous.
SELECT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'rb' AND column = 's';
SELECT count() = 170 FROM (
    SELECT 1 FROM lb INNER JOIN rb ON lb.k = rb.k
    WHERE lb.x = byteSize(rb.s))
SETTINGS query_plan_merge_filter_into_join_condition = 1;
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT 1 FROM lb INNER JOIN rb ON lb.k = rb.k
    WHERE lb.x = byteSize(rb.s)
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- L15 covers showCertificate and lives in 04696, tagged no-fasttest: that function needs a build with
-- OpenSSL, and that build capability is the only thing that differs between the two files.

-- L16: joinGet reads a StorageJoin whose hash table is not pinned for the query. `StorageJoin.h:60-61`
-- says the read lock is taken "to prevent parallel StorageJoin updates during processing data block
-- (but not during processing whole query)", and `HashJoin.cpp:1026-1028` carries a static_assert whose
-- message is "joinGet are not protected from hash table changes between block processing". So a
-- promoted key and the conjunct above the join can read two different table states. The base
-- `FunctionJoinGet` overrides none of the four predicates -- the `isDeterministic() { return false; }`
-- in that file belongs to the overload RESOLVER, which the guard never sees, since it reads
-- `function_base`. The third argument is a genuine left-side column, which is what keeps the call
-- unfoldable and side-assignable (`getArgumentsThatAreAlwaysConstant` is {0, 1} only).
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8(joinGet('jt', 'v', toUInt32(l.a % 100)) % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- L17: the same lookup over a table that uses nulls, where `buildImpl` sets the effective name to
-- `joinGetOrNull` even though the query says `joinGet`. This row is what makes the second listed name
-- load-bearing rather than dead text: the M20 arm drops only `joinGetOrNull` and reddens this row
-- while L16 stays green.
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8(assumeNotNull(joinGet('jtn', 'v', toUInt32(l.a % 100))) % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- L18: an unsafe call inside a lambda body. A lambda reaches the guard as a `FunctionCapture` whose
-- `children` are only the captured columns, so the body is invisible to a child walk, and the wrapper
-- itself reports none of the four predicates. The guard therefore descends into the inner
-- `ActionsDAG`, which is where the `rand` lives. The `arrayMap(...)[1]` spelling keeps the operand
-- `UInt8`, matching `r.b`: `arrayCount` returns `UInt32` and the pre-existing type test above rejects
-- it, which would make this row vacuous.
SELECT countIf(x != rb) = 0 AND count() > 20000 FROM (
    SELECT arrayMap(z -> toUInt8(rand(z) % 16), [l.a])[1] AS x, r.b AS rb FROM l INNER JOIN r ON l.k = r.k
    WHERE arrayMap(z -> toUInt8(rand(z) % 16), [l.a])[1] = r.b)
SETTINGS query_plan_merge_filter_into_join_condition = 1;

-- L19: a stateful call inside a lambda body, so the descent is asserted for both refusal classes and
-- not only for determinism. As in L5 the correct answer is empty and pre-fix 2000 rows are emitted.
SELECT count() = 0 FROM (
    SELECT rc.b AS rb FROM lc INNER JOIN rc ON lc.k = rc.k
    WHERE arrayMap(z -> toUInt8(runningConcurrency(z, lc.e) % 16), [lc.s])[1] = rc.b)
SETTINGS query_plan_merge_filter_into_join_condition = 1;

-- L20: the filesystem* family reads the executing node's disk map, which it captures when the
-- function object is built, so like L8 it is refused by the name list rather than by a flag. A
-- non-constant disk argument is what keeps the call live as a function node: with a constant
-- argument it is folded before the pass runs and the row would be vacuous.
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8((filesystemAvailable(l.disk) + l.a) % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8((filesystemCapacity(l.disk) + l.a) % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8((filesystemUnreserved(l.disk) + l.a) % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- Controls: a deterministic-in-scope-of-query expression must keep being merged. A `Clauses:` line
-- listing two key groups (`(k, <expr>) = (k, b)`) means the extra term was merged.

-- C7: a deterministic predicate over the same `remote()` vehicle must still be merged. Without this
-- control L7 and L8 could be passing merely because the query is distributed.
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 1 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM remote('127.0.0.1', currentDatabase(), l) AS lr
    INNER JOIN r ON lr.k = r.k
    WHERE toUInt8(lr.a % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- C1: plainly deterministic.
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 1 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8(l.a % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- C2: now() is not deterministic across queries but is within one, so its plan-time value is
-- faithful and it must stay merged. (now() is constant-folded before the pass runs, so this row does
-- not by itself pin the boundary -- see C5 -- but it does pin that the class is not rejected.)
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 1 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8((toHour(now()) + l.a) % 4) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- C8: the zero-argument now64() is the twin of C2 and must stay merged for the same reason: with all
-- arguments constant it is folded before the pass runs, so naming now64 in the guard cannot reach it.
-- C2 and C8 together are what stop the L11 entries being over-broad.
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 1 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8((toHour(now64()) + l.a) % 4) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- C5: dictGet reports isDeterministic() == false and does not override the in-scope predicate, and
-- unlike now() it is not constant-folded, so it reaches the guard as a FUNCTION node. This is the
-- row that pins the isDeterministicInScopeOfQuery boundary: substituting isDeterministic for it
-- rejects this conjunct and loses a correct optimization.
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 1 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8(dictGet(currentDatabase() || '.dic', 'val', toUInt64(l.a % 320))) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- C3: a mixed filter. The benign term must still be merged and the unsafe one must not, which is
-- what a per-conjunct guard buys over a DAG-wide one.
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.a%') = 1 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8(l.a % 251) = toUInt8(r.k % 251) AND toUInt8(rand(l.a) % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%rand%') = 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8(l.a % 251) = toUInt8(r.k % 251) AND toUInt8(rand(l.a) % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- C6: an `ActionsDAG` is a DAG, not a tree. Each alias below consumes the previous one twice, and the
-- analyzer stores one node per distinct subexpression, so the safety walk reaches the same node over
-- many paths and must judge it once. Every function here is deterministic in the scope of the query,
-- so the conjunct must still be merged; the second row pins that sharing does not defeat a rejection
-- either. A walk that mistook a revisited node for an unsafe one would fail the first row while every
-- other row in this file still passed.
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 1 FROM (
    EXPLAIN PLAN actions = 1
    WITH toUInt64(l.a) + 1 AS a1, a1 + a1 AS a2, a2 + a2 AS a3, a3 + a3 AS a4, a4 + a4 AS a5,
         a5 + a5 AS a6, a6 + a6 AS a7, a7 + a7 AS a8, a8 + a8 AS a9, a9 + a9 AS a10
    SELECT r.b FROM l INNER JOIN r ON l.k = r.k WHERE toUInt8(a10 % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 0 FROM (
    EXPLAIN PLAN actions = 1
    WITH toUInt64(rand(l.a)) + 1 AS a1, a1 + a1 AS a2, a2 + a2 AS a3, a3 + a3 AS a4, a4 + a4 AS a5,
         a5 + a5 AS a6, a6 + a6 AS a7, a7 + a7 AS a8, a8 + a8 AS a9, a9 + a9 AS a10
    SELECT r.b FROM l INNER JOIN r ON l.k = r.k WHERE toUInt8(a10 % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- C9: the deterministic twin of L14 over the very same sparse column. `length` reads the VALUE, so
-- the sparse and dense readings agree and the conjunct must still be merged. Without this control
-- L14 could be passing merely because the right side is a MergeTree table or because the key group
-- pattern never matches this query shape.
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 1 FROM (
    EXPLAIN PLAN actions = 1 SELECT 1 FROM lb INNER JOIN rb ON lb.k = rb.k
    WHERE lb.x = length(rb.s)
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- C10: the deterministic twin of L16, in L16's EXACT wrapper. `dictGet` is the other per-row keyed
-- lookup into a separate storage that reaches the guard as a live FUNCTION node, and it must stay
-- merged because a dictionary holds one value for the whole query. This differs from C5, which pins
-- the `isDeterministicInScopeOfQuery`-vs-`isDeterministic` boundary in a different expression shape:
-- what C10 controls is the SHAPE L16 asserts on, so L16 cannot be passing merely because a keyed
-- lookup sits inside `toUInt8(... % 16) = r.b`.
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 1 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8(dictGet(currentDatabase() || '.dic', 'val', toUInt64(l.a % 320)) % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- C11: the deterministic twin of L18, in L18's exact lambda wrapper. The descent into a lambda body
-- must refuse only what the body actually carries, so a lambda over a deterministic expression must
-- still be merged. Without this row the descent could be refusing every lambda outright and L18 would
-- still pass.
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 1 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l INNER JOIN r ON l.k = r.k
    WHERE arrayMap(z -> toUInt8(z % 16), [l.a])[1] = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- C4: the deterministic twin of L1's query, already correct before the fix, so it proves the oracle
-- is not simply reporting "no rows were emitted".
SELECT countIf(x != rb) = 0 AND count() > 20000 FROM (
    SELECT toUInt8(l.a % 16) AS x, r.b AS rb FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8(l.a % 16) = r.b)
SETTINGS query_plan_merge_filter_into_join_condition = 1;

DROP DICTIONARY dic;
DROP TABLE l;
DROP TABLE r;
DROP TABLE lc;
DROP TABLE rc;
DROP TABLE lt;
DROP TABLE rt;
DROP TABLE lb;
DROP TABLE rb;
DROP TABLE jt;
DROP TABLE jtn;
