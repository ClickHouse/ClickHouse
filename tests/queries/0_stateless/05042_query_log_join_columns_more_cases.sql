-- Tags: no-old-analyzer

-- The rest of the cases of the join columns of `system.query_log`, whose main test is
-- 04891_query_log_join_columns.sql. Three groups of them, in this order:
--
--  * A join is reported no matter how deeply it is nested in the query the user sent, because the
--    counters it lands in are looked up through the query context of the thread that assembles the
--    pipeline. Subqueries, common table expressions, views, views of views and the `SELECT` of a
--    materialized view triggered by an `INSERT` all report into the row of that query, even though its
--    text may hold no `JOIN` at all.
--  * `PARALLEL_HASH` and `DIRECT` are the two algorithms the main test never forces: each comes from an
--    `IJoin` that is built only under conditions a query has to set up on purpose, so each is covered
--    over the kinds and the strictness it is picked for. The group ends the other way around, asking for
--    an algorithm whose conditions the query then fails to meet, because the column reports the
--    algorithm that ran and not the one that was requested.
--  * The columns describe the pipeline that was built, so they are empty in the `QueryStart` row of a
--    query and filled in the row that ends it, which is the one carrying the exception when it fails.
--
-- The algorithm is set explicitly wherever it is asserted, because the choice among the algorithms
-- allowed by `join_algorithm` is made at run time and depends on the number of threads.

SET log_queries = 1;
-- The reported kind is the executed one, and the optimizer may execute a join with its sides
-- swapped, which reverses LEFT and RIGHT. Disable that so the kinds are the ones the queries are
-- written with.
SET query_plan_join_swap_table = 0;
-- The strictness an ANY join carries is RIGHT_ANY under this setting, and the direct join is not built
-- for it, so one of the queries below would be rejected instead of reporting the algorithm it asks
-- about.
SET any_join_distinct_right_table_keys = 0;
-- A `Join` engine table remembers the setting it was created with and refuses a LEFT or FULL join
-- whose setting differs, so the table and the query that reads it have to agree on one value.
SET join_use_nulls = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS tj;
DROP TABLE IF EXISTS ta;
DROP TABLE IF EXISTS tb;
DROP VIEW IF EXISTS v_join;
DROP VIEW IF EXISTS v_over_view;
DROP VIEW IF EXISTS mv_first;
DROP VIEW IF EXISTS mv_chained;
DROP VIEW IF EXISTS mv_inner;
DROP VIEW IF EXISTS mv_over_view;
DROP VIEW IF EXISTS mv_populate;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS dst_chained;
DROP TABLE IF EXISTS src_inner;
DROP TABLE IF EXISTS src_over_view;
DROP TABLE IF EXISTS ins;
DROP DICTIONARY IF EXISTS dict_direct;
DROP TABLE IF EXISTS dict_source;

CREATE TABLE t1 (a UInt64, b UInt64) ENGINE = Memory;
CREATE TABLE t2 (a UInt64, b UInt64) ENGINE = Memory;
CREATE TABLE t3 (a UInt64) ENGINE = Memory;
CREATE TABLE tj (a UInt64, b UInt64) ENGINE = Join(ANY, LEFT, a);
CREATE TABLE ta (a UInt64, t UInt64) ENGINE = Memory;
CREATE TABLE tb (a UInt64, t UInt64) ENGINE = Memory;

INSERT INTO t1 SELECT number, number FROM numbers(10);
INSERT INTO t2 SELECT number, number FROM numbers(10);
INSERT INTO t3 SELECT number FROM numbers(10);
INSERT INTO tj SELECT number, number FROM numbers(10);
INSERT INTO ta SELECT number, number FROM numbers(10);
INSERT INTO tb SELECT number, number FROM numbers(10);

SELECT 'join in a subquery in FROM';
-- The subquery is part of the same query plan, so its join is one of the joins of the pipeline.
SELECT count() FROM (SELECT t1.a AS a FROM t1 JOIN t2 ON t1.a = t2.a)
FORMAT Null
SETTINGS log_comment = '05042_join_views_subquery_from', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_views\_subquery\_from%'
ORDER BY log_comment;

SELECT 'join in a subquery in IN';
-- The set is built by a part of the same pipeline, and the join that fills it is reported as well.
SELECT count() FROM t3 WHERE a IN (SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a)
FORMAT Null
SETTINGS log_comment = '05042_join_views_subquery_in', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_views\_subquery\_in%'
ORDER BY log_comment;

SELECT 'join in the right side of a join';
-- The right side of a join is a subquery with a join of its own, so the pipeline holds two joins: the
-- inner one and the one that reads its result.
SELECT count() FROM t1 JOIN (SELECT x.a AS a FROM t1 AS x JOIN t2 AS y ON x.a = y.a) s ON t1.a = s.a
FORMAT Null
SETTINGS log_comment = '05042_join_views_subquery_right', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_views\_subquery\_right%'
ORDER BY log_comment;

SELECT 'join in a common table expression';
-- A common table expression is inlined at each of its references, so a CTE that holds one join and is
-- read twice contributes two joins to the pipeline, and the join between its two references makes
-- three. `enable_materialized_cte` is pinned because a materialized CTE would be a different pipeline.
WITH c AS (SELECT t1.a AS a FROM t1 JOIN t2 ON t1.a = t2.a)
SELECT count() FROM c AS c1 JOIN c AS c2 ON c1.a = c2.a
FORMAT Null
SETTINGS log_comment = '05042_join_views_cte', join_algorithm = 'hash', enable_materialized_cte = 0;

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_views\_cte%'
ORDER BY log_comment;

SELECT 'join in a scalar subquery';
-- A scalar subquery is executed while the outer query is still being analyzed, on the thread of that
-- query, so it reports into the same counters even though it runs a pipeline of its own.
SELECT (SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a)
FORMAT Null
SETTINGS log_comment = '05042_join_views_subquery_scalar', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_views\_subquery\_scalar%'
ORDER BY log_comment;

SELECT 'join inside a view';
CREATE VIEW v_join AS SELECT t1.a AS a FROM t1 JOIN t2 ON t1.a = t2.a;
-- A view is inlined into the query that reads it, so the join of the view is a join of that query,
-- which has none of its own.
SELECT count() FROM v_join
FORMAT Null
SETTINGS log_comment = '05042_join_views_view_a_alone', join_algorithm = 'hash';
-- Joining the view with a table gives two joins: the one inside the view and the one outside it.
SELECT count() FROM v_join JOIN t3 ON v_join.a = t3.a
FORMAT Null
SETTINGS log_comment = '05042_join_views_view_b_joined', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_views\_view\_%'
ORDER BY log_comment;

SELECT 'join inside a view of a view';
CREATE VIEW v_over_view AS SELECT v_join.a AS a FROM v_join JOIN t3 ON v_join.a = t3.a;
-- Both views are inlined, one into the other, so the two joins of the pair are reported for a query
-- that reads the outer view alone.
SELECT count() FROM v_over_view
FORMAT Null
SETTINGS log_comment = '05042_join_views_nested_views', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_views\_nested\_views%'
ORDER BY log_comment;

SELECT 'join inside a materialized view';
CREATE TABLE src (a UInt64) ENGINE = Memory;
CREATE TABLE dst (a UInt64) ENGINE = Memory;
CREATE MATERIALIZED VIEW mv_first TO dst AS SELECT src.a AS a FROM src JOIN t2 ON src.a = t2.a;
-- The `SELECT` of a materialized view runs in a thread group nested in the one of the `INSERT`, and a
-- nested thread group carries the query context of its parent. The join of the view is reported in
-- the row of the `INSERT` that triggered it, which is the only row the `INSERT` writes to
-- `system.query_log`.
INSERT INTO src SELECT number FROM numbers(10)
SETTINGS log_comment = '05042_join_views_mv_a_single', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_views\_mv\_a\_%'
ORDER BY log_comment;

SELECT 'join inside a chain of materialized views';
CREATE TABLE dst_chained (a UInt64) ENGINE = Memory;
CREATE MATERIALIZED VIEW mv_chained TO dst_chained AS SELECT dst.a AS a FROM dst JOIN t3 ON dst.a = t3.a;
-- The insert into `dst` made by `mv_first` triggers `mv_chained` in turn, and its thread group is
-- nested one level deeper. Both joins are reported in the row of the `INSERT` all the same.
INSERT INTO src SELECT number FROM numbers(10)
SETTINGS log_comment = '05042_join_views_mv_b_chained', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_views\_mv\_b\_%'
ORDER BY log_comment;

SELECT 'join inside a materialized view with its own inner table';
CREATE TABLE src_inner (a UInt64) ENGINE = Memory;
-- A materialized view without `TO` writes into an inner table it owns, which is a different storage
-- but the same nested thread group, so the join of its `SELECT` is reported just as it is for one with
-- an explicit destination.
CREATE MATERIALIZED VIEW mv_inner ENGINE = Memory AS SELECT src_inner.a AS a FROM src_inner JOIN t2 ON src_inner.a = t2.a;
INSERT INTO src_inner SELECT number FROM numbers(10)
SETTINGS log_comment = '05042_join_views_mv_c_inner_table', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_views\_mv\_c\_%'
ORDER BY log_comment;

SELECT 'join inside a materialized view that reads a view';
CREATE TABLE src_over_view (a UInt64) ENGINE = Memory;
-- The `SELECT` of the materialized view joins a view which holds a join of its own, and the two are
-- reported together: the nesting of the view inside the materialized view is no different from the
-- nesting of a view inside a plain query.
CREATE MATERIALIZED VIEW mv_over_view ENGINE = Memory AS SELECT src_over_view.a AS a FROM src_over_view JOIN v_join ON src_over_view.a = v_join.a;
INSERT INTO src_over_view SELECT number FROM numbers(10)
SETTINGS log_comment = '05042_join_views_mv_over_view_a_plain', join_algorithm = 'hash';
-- The `SELECT` of the `INSERT` has a join of its own as well, and it is counted along with the two of
-- the materialized view.
INSERT INTO src_over_view SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a
SETTINGS log_comment = '05042_join_views_mv_over_view_b_insert_with_join', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_views\_mv\_over\_view\_%'
ORDER BY log_comment;

SELECT 'join in the SELECT of CREATE MATERIALIZED VIEW POPULATE';
-- `POPULATE` runs the `SELECT` of the view as part of the `CREATE`, and the join it executes is
-- reported in the row of the `CREATE` query.
CREATE MATERIALIZED VIEW mv_populate ENGINE = Memory POPULATE AS SELECT src_inner.a AS a FROM src_inner JOIN t2 ON src_inner.a = t2.a
SETTINGS log_comment = '05042_join_views_mv_populate', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT query_kind, used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_views\_mv\_populate%'
ORDER BY log_comment;

SELECT 'join in the SELECT of an INSERT with no view attached';
CREATE TABLE ins (a UInt64) ENGINE = Memory;
-- Nothing is nested here: the join belongs to the `SELECT` of the `INSERT` itself, and it is reported
-- in the row of that `INSERT`, which is the plainest case of a join in a query whose text is an
-- `INSERT`.
INSERT INTO ins SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a
SETTINGS log_comment = '05042_join_views_insert_plain', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_views\_insert\_plain%'
ORDER BY log_comment;

SELECT 'parallel hash';
-- `parallel_hash` builds a `ConcurrentHashJoin`, which reports `PARALLEL_HASH`. It is picked only for
-- INNER, LEFT, RIGHT and FULL joins over a single disjunct and never for a special storage on the
-- right. When `join_algorithm` allows `hash` as well, the right table also has to be estimated at
-- `parallel_hash_join_threshold` bytes or more, so asking for `parallel_hash` alone keeps the size
-- estimate out of the decision and the algorithm is the one that runs whatever the tables hold.
SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a
FORMAT Null
SETTINGS log_comment = '05042_join_algorithms_parallel_hash', join_algorithm = 'parallel_hash', max_threads = 4;

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_algorithms\_parallel\_hash%'
ORDER BY log_comment;

SELECT 'parallel hash for the other kinds it is picked for';
-- The kinds `ConcurrentHashJoin` is built for are INNER, which is above, and these three. Strictness
-- is not part of the condition, so an ASOF join of an allowed kind is executed with it as well.
SELECT count() FROM t1 LEFT JOIN t2 ON t1.a = t2.a
FORMAT Null
SETTINGS log_comment = '05042_join_algorithms_ph_kind_a_left', join_algorithm = 'parallel_hash', max_threads = 4;
SELECT count() FROM t1 RIGHT JOIN t2 ON t1.a = t2.a
FORMAT Null
SETTINGS log_comment = '05042_join_algorithms_ph_kind_b_right', join_algorithm = 'parallel_hash', max_threads = 4;
SELECT count() FROM t1 FULL JOIN t2 ON t1.a = t2.a
FORMAT Null
SETTINGS log_comment = '05042_join_algorithms_ph_kind_c_full', join_algorithm = 'parallel_hash', max_threads = 4;
SELECT count() FROM ta ASOF LEFT JOIN tb ON ta.a = tb.a AND ta.t >= tb.t
FORMAT Null
SETTINGS log_comment = '05042_join_algorithms_ph_kind_d_asof_left', join_algorithm = 'parallel_hash', max_threads = 4;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_algorithms\_ph\_kind\_%'
ORDER BY log_comment;

SELECT 'parallel hash switching to grace hash';
-- A `parallel_hash` join with a spilling threshold is a `SpillingHashJoin` wrapping the concurrent
-- join, and it reports `PARALLEL_HASH` until it switches. Over the threshold it becomes `grace_hash`
-- while the query is already running, and both algorithms are reported for the one join, the same way
-- a plain `hash` join that switches reports `HASH` and `GRACE_HASH`.
SELECT count() FROM (SELECT number AS a FROM numbers(10000)) s1 JOIN (SELECT number AS a FROM numbers(10000)) s2 ON s1.a = s2.a
FORMAT Null
SETTINGS log_comment = '05042_join_algorithms_parallel_hash_switch', join_algorithm = 'parallel_hash', max_threads = 4, max_bytes_before_external_join = 65536;

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_algorithms\_parallel\_hash\_switch%'
ORDER BY log_comment;

SELECT 'direct join with a dictionary';
CREATE TABLE dict_source (key UInt64, value String) ENGINE = Memory;
INSERT INTO dict_source SELECT number, toString(number) FROM numbers(10);
CREATE DICTIONARY dict_direct (key UInt64 DEFAULT 0, value String DEFAULT '')
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'dict_source'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());
-- `direct` builds a `DirectKeyValueJoin`, which reports `DIRECT`. It needs a key-value storage on the
-- right, here a dictionary, a single equality key and no mixed condition, and it accepts an INNER join
-- of strictness ALL or a LEFT join of strictness ALL, ANY, SEMI or ANTI.
SELECT count() FROM t1 LEFT JOIN dict_direct ON t1.a = dict_direct.key
FORMAT Null
SETTINGS log_comment = '05042_join_algorithms_direct_a_left', join_algorithm = 'direct';
SELECT count() FROM t1 JOIN dict_direct ON t1.a = dict_direct.key
FORMAT Null
SETTINGS log_comment = '05042_join_algorithms_direct_b_inner', join_algorithm = 'direct';
-- The strictness of a LEFT join is one of the four the direct join accepts, and it is reported as
-- written rather than replaced by the algorithm.
SELECT count() FROM t1 ANY LEFT JOIN dict_direct ON t1.a = dict_direct.key
FORMAT Null
SETTINGS log_comment = '05042_join_algorithms_direct_c_any_left', join_algorithm = 'direct';
SELECT count() FROM t1 SEMI LEFT JOIN dict_direct ON t1.a = dict_direct.key
FORMAT Null
SETTINGS log_comment = '05042_join_algorithms_direct_d_semi_left', join_algorithm = 'direct';
SELECT count() FROM t1 ANTI LEFT JOIN dict_direct ON t1.a = dict_direct.key
FORMAT Null
SETTINGS log_comment = '05042_join_algorithms_direct_e_anti_left', join_algorithm = 'direct';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_algorithms\_direct\_%'
ORDER BY log_comment;

SELECT 'algorithms that were asked for but not executed';
-- The reported algorithm is the one that ran, not the one the query asked for. Each of these queries
-- allows an algorithm whose conditions it then fails to meet, and the join falls back to `hash`:
--  * `parallel_hash` is not built for more than one disjunct, and multiple ORs need `hash` to be
--    allowed as well, so it is the only algorithm left.
--  * `parallel_hash` is not built when the right side is a special storage, here a `Join` engine
--    table, which is joined by `FilledJoinStep` with the table's own hash table.
--  * `direct` needs an INNER or a LEFT join, so a RIGHT one falls back.
--  * `direct` looks rows up by the equality key alone and cannot evaluate the mixed condition of the
--    `ON` clause, so it declines a join that carries one rather than drop it.
SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a OR t1.b = t2.b
FORMAT Null
SETTINGS log_comment = '05042_join_algorithms_declined_a_parallel_hash_disjuncts', join_algorithm = 'parallel_hash,hash', max_threads = 4;
SELECT count() FROM t1 ANY LEFT JOIN tj ON t1.a = tj.a
FORMAT Null
SETTINGS log_comment = '05042_join_algorithms_declined_b_parallel_hash_special_storage', join_algorithm = 'parallel_hash', max_threads = 4;
SELECT count() FROM t1 RIGHT JOIN dict_direct ON t1.a = dict_direct.key
FORMAT Null
SETTINGS log_comment = '05042_join_algorithms_declined_c_direct_right', join_algorithm = 'direct,hash';
SELECT count() FROM t1 LEFT JOIN dict_direct ON t1.a = dict_direct.key AND t1.b > dict_direct.key
FORMAT Null
SETTINGS log_comment = '05042_join_algorithms_declined_d_direct_mixed_condition', join_algorithm = 'direct,hash';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_algorithms\_declined\_%'
ORDER BY log_comment;

SELECT 'a query that succeeds';
SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a
FORMAT Null
SETTINGS log_comment = '05042_join_rows_success', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT type, used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_rows\_success%'
ORDER BY type;

SELECT 'a query that fails while its pipeline runs';
-- The join exceeds `max_rows_in_join` while it fills its right side, which is late enough for the
-- pipeline, and the join in it, to have been built and reported.
SELECT count() FROM (SELECT number AS a FROM numbers(100000)) x JOIN (SELECT number AS a FROM numbers(100000)) y ON x.a = y.a
FORMAT Null
SETTINGS log_comment = '05042_join_rows_exception_while_processing', join_algorithm = 'hash', max_rows_in_join = 1000, join_overflow_mode = 'throw'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SYSTEM FLUSH LOGS query_log;
SELECT type, used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_rows\_exception\_while\_processing%'
ORDER BY type;

SELECT 'a query that fails before it starts';
-- The query is rejected while it is analyzed, so no pipeline is built and the join of its text is
-- never one of the joins of a pipeline. The one row it writes reports none.
SELECT count() FROM t1 JOIN no_such_table ON t1.a = no_such_table.a
SETTINGS log_comment = '05042_join_rows_exception_before_start', join_algorithm = 'hash'; -- { serverError UNKNOWN_TABLE }

SYSTEM FLUSH LOGS query_log;
SELECT type, used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND event_date >= yesterday()
  AND log_comment LIKE '05042\_join\_rows\_exception\_before\_start%'
ORDER BY type;

DROP VIEW mv_populate;
DROP VIEW mv_over_view;
DROP VIEW mv_inner;
DROP VIEW v_over_view;
DROP VIEW mv_chained;
DROP VIEW mv_first;
DROP VIEW v_join;
DROP DICTIONARY dict_direct;
DROP TABLE dict_source;
DROP TABLE ins;
DROP TABLE src_over_view;
DROP TABLE src_inner;
DROP TABLE dst_chained;
DROP TABLE dst;
DROP TABLE src;
DROP TABLE tb;
DROP TABLE ta;
DROP TABLE tj;
DROP TABLE t3;
DROP TABLE t2;
DROP TABLE t1;
