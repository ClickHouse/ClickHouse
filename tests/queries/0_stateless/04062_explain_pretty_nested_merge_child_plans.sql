SET enable_analyzer = 1;

DROP TABLE IF EXISTS nested_m_t1;
DROP TABLE IF EXISTS nested_m_t2a;
DROP TABLE IF EXISTS nested_m_t2b;
DROP TABLE IF EXISTS nested_m_t3;
DROP TABLE IF EXISTS nested_m_inner;
DROP TABLE IF EXISTS nested_m_outer;
DROP TABLE IF EXISTS nested_m_dummy;

CREATE TABLE nested_m_t1 (x UInt32) ENGINE = Memory;
CREATE TABLE nested_m_t2a (x UInt32) ENGINE = Memory;
CREATE TABLE nested_m_t2b (x UInt32) ENGINE = Memory;
CREATE TABLE nested_m_t3 (x UInt32) ENGINE = Memory;

CREATE TABLE nested_m_inner (x UInt32) ENGINE = Merge(currentDatabase(), 'nested_m_t2a|nested_m_t2b');
CREATE TABLE nested_m_outer (x UInt32) ENGINE = Merge(currentDatabase(), 'nested_m_t1|nested_m_inner|nested_m_t3');

-- Nested Merge: outer lists three sources; middle source is Merge over two tables → child plans inside child plan.
EXPLAIN PLAN header = 1, pretty = 1
SELECT * FROM nested_m_outer;

-- Extra pipeline depth (Union) so nested child-plan EXPLAIN carries a non-empty tree prefix (regression guard for pretty prefixes).
CREATE TABLE nested_m_dummy (x UInt32) ENGINE = Memory;

EXPLAIN PLAN header = 1, pretty = 1
SELECT * FROM nested_m_outer
UNION ALL
SELECT * FROM nested_m_dummy
WHERE 0;

DROP TABLE IF EXISTS nested_m_t1;
DROP TABLE IF EXISTS nested_m_t2a;
DROP TABLE IF EXISTS nested_m_t2b;
DROP TABLE IF EXISTS nested_m_t3;
DROP TABLE IF EXISTS nested_m_inner;
DROP TABLE IF EXISTS nested_m_outer;
DROP TABLE IF EXISTS nested_m_dummy;
