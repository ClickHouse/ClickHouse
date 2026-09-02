-- Tags: no-parallel
-- A read-only `Overlay` facade must reject `CREATE`/`ATTACH TABLE` for a name that already resolves
-- through it to a source table, before the generic table-existence check in
-- `InterpreterCreateQuery::doCreateTable`. Otherwise `CREATE TABLE IF NOT EXISTS` would silently
-- succeed and a plain `CREATE TABLE` would report `TABLE_ALREADY_EXISTS`, both violating the
-- documented `TABLE_IS_PERMANENTLY_READ_ONLY` contract and leaking the existence of source tables
-- through a facade-scoped `CREATE TABLE` grant.
-- Uses fixed global database names, hence `no-parallel`.
-- Related: https://github.com/ClickHouse/ClickHouse/pull/86768

DROP DATABASE IF EXISTS ov_create_src;
DROP DATABASE IF EXISTS ov_create_face;

CREATE DATABASE ov_create_src ENGINE = Atomic;
CREATE TABLE ov_create_src.t (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ov_create_src.t VALUES (1), (2);

CREATE DATABASE ov_create_face ENGINE = Overlay('ov_create_src');

-- The source table is visible through the facade.
SELECT count() FROM ov_create_face.t;

-- A name that already resolves through the facade is rejected up front: the read-only contract takes
-- priority over the existence check, both for a plain `CREATE TABLE` (no `TABLE_ALREADY_EXISTS`) and
-- for `CREATE TABLE IF NOT EXISTS` (no silent success), as well as for `ATTACH TABLE`.
CREATE TABLE ov_create_face.t (id UInt64) ENGINE = MergeTree ORDER BY id; -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }
CREATE TABLE IF NOT EXISTS ov_create_face.t (id UInt64) ENGINE = MergeTree ORDER BY id; -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }
ATTACH TABLE ov_create_face.t (id UInt64) ENGINE = MergeTree ORDER BY id; -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }

-- A brand-new name through the facade is rejected the same way.
CREATE TABLE ov_create_face.brand_new (id UInt64) ENGINE = MergeTree ORDER BY id; -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }

-- None of the rejected statements touched the source table: it is still present and readable, and no
-- new table leaked into it.
SELECT count() FROM ov_create_face.t;

DROP DATABASE ov_create_face;
DROP DATABASE ov_create_src;
