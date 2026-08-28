-- A parameterised view's arguments are re-serialised whenever its call is rendered into a
-- per-shard query, so every non-String parameter travels as its CAST-wrapped literal. Serialising
-- that literal without its target type produced text the receiving side could not parse back and
-- the query failed with CANNOT_PARSE_QUOTED_STRING. The INSERT cases below keep the issue's exact
-- repro (with parallel_distributed_insert_select the source now falls back to shipping blocks);
-- the final SELECT forces a real forwarded query and proves it shard-side via system.query_log.

DROP TABLE IF EXISTS t_pv_dist;
DROP TABLE IF EXISTS t_pv;
DROP VIEW IF EXISTS v_pv;
DROP VIEW IF EXISTS v_pv_array;

CREATE TABLE t_pv
(
    name String,
    val Enum8('x' = 0, 'y' = 1),
    d Date,
    u UUID,
    ip IPv4,
    dec Decimal(10, 2)
)
ENGINE = MergeTree ORDER BY name;

CREATE TABLE t_pv_dist AS t_pv ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_pv);

-- Every parameter here is non-String, so each one is CAST-wrapped on its way to the shard.
CREATE VIEW v_pv AS
SELECT
    {name:String} AS name,
    {val:Enum8('x' = 0, 'y' = 1)} AS val,
    {d:Date} AS d,
    {u:UUID} AS u,
    {ip:IPv4} AS ip,
    {dec:Decimal(10, 2)} AS dec;

INSERT INTO t_pv_dist
SELECT * FROM v_pv(
    name = 'a1',
    val = 'y',
    d = '2020-01-02',
    u = '00000000-0000-0000-0000-000000000001',
    ip = '1.2.3.4',
    dec = 1.25)
SETTINGS parallel_distributed_insert_select = 2;

SELECT 'scalar', * FROM t_pv ORDER BY name;

TRUNCATE TABLE t_pv;

-- The same through an enum nested in Array(Tuple(...)), which is how it was first reported.
CREATE VIEW v_pv_array AS
SELECT
    tup.1 AS name,
    tup.2 AS val,
    toDate('2020-01-02') AS d,
    toUUID('00000000-0000-0000-0000-000000000001') AS u,
    toIPv4('1.2.3.4') AS ip,
    toDecimal64(1.25, 2) AS dec
FROM (SELECT arrayJoin({tuples:Array(Tuple(String, Enum8('x' = 0, 'y' = 1)))}) AS tup);

INSERT INTO t_pv_dist
SELECT * FROM v_pv_array(tuples = [('a1', 'y'), ('a2', 'x')])
SETTINGS parallel_distributed_insert_select = 2;

SELECT 'array', name, val FROM t_pv ORDER BY name;

-- A non-GLOBAL IN subquery is forwarded to the shard as query text, and prefer_localhost_replica=0
-- forces a real connection, so the view call and its parameters must survive rendering and re-parsing.
TRUNCATE TABLE t_pv;
INSERT INTO t_pv VALUES ('a1', 'y', '2020-01-02', '00000000-0000-0000-0000-000000000001', '1.2.3.4', 1.25), ('zz', 'x', '2020-01-02', '00000000-0000-0000-0000-000000000001', '1.2.3.4', 1.25);

-- serialize_query_plan is pinned off: the distributed-plan CI flavor would otherwise ship a
-- serialized plan instead of the re-rendered SQL whose text this test is about.
SELECT 'forwarded-in', count() FROM t_pv_dist
WHERE (name, val) IN (SELECT name, val FROM v_pv_array(tuples = [('a1', 'y'), ('a2', 'x')]))
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;

-- The scalar view forwards every CAST-wrapped parameter type at once, so a regression in
-- forwarded scalar re-serialization cannot hide behind the array-tuple case above.
SELECT 'forwarded-in-scalar', count() FROM t_pv_dist
WHERE (name, val, d, u, ip, dec) IN (SELECT * FROM v_pv(
    name = 'a1',
    val = 'y',
    d = '2020-01-02',
    u = '00000000-0000-0000-0000-000000000001',
    ip = '1.2.3.4',
    dec = 1.25))
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;

SYSTEM FLUSH LOGS query_log;

-- The shard really received both view calls as text: non-initial queries mentioning them ran.
SELECT 'shard saw the view call', count() > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query = 0
    AND has(databases, currentDatabase()) AND query LIKE '%v_pv_array%';

SELECT 'shard saw the scalar view call', count() > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query = 0
    AND has(databases, currentDatabase()) AND query LIKE '%v_pv(%';

DROP VIEW v_pv_array;
DROP VIEW v_pv;
DROP TABLE t_pv_dist;
DROP TABLE t_pv;
