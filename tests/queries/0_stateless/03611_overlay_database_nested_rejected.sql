-- Tags: no-parallel
-- This test creates databases with fixed global names, so it cannot run concurrently with copies of
-- itself (the flaky check runs a test many times in parallel): a concurrent run would drop a source
-- database out from under another run between `CREATE DATABASE ov_nested_src` and the facade `CREATE`.
-- The sibling `03611_overlay_database_server_side.sql` / `_restore_rename.sql` tests are `no-parallel`
-- for the same reason.

-- A read-only `Overlay` database cannot use another read-only `Overlay` database as a source.
-- Nesting facades would let access checks and row policies bypass the intermediate facade: reading
-- `top.t` with `top = Overlay('mid')` and `mid = Overlay('src')` resolves straight to `src.t`, so a
-- check that only sees the written id (`top.t`) and the resolved storage id (`src.t`) would skip the
-- grants and row policies defined on `mid.t`. Such nesting is therefore rejected at CREATE time.
-- A single-level facade keeps working under both the old and the new analyzer.
-- Related: https://github.com/ClickHouse/ClickHouse/pull/86768

DROP DATABASE IF EXISTS ov_nested_src;
DROP DATABASE IF EXISTS ov_nested_mid;
DROP DATABASE IF EXISTS ov_nested_top;

CREATE DATABASE ov_nested_src ENGINE = Atomic;
CREATE TABLE ov_nested_src.t (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ov_nested_src.t VALUES (1), (2);

-- A single-level facade over a non-Overlay database works (regression guard), under both analyzers.
CREATE DATABASE ov_nested_mid ENGINE = Overlay('ov_nested_src');
SELECT count() FROM ov_nested_mid.t SETTINGS enable_analyzer = 0;
SELECT count() FROM ov_nested_mid.t SETTINGS enable_analyzer = 1;

-- Layering a facade on top of a facade is rejected.
CREATE DATABASE ov_nested_top ENGINE = Overlay('ov_nested_mid'); -- { serverError BAD_ARGUMENTS }

-- Listing a facade among several sources is rejected too.
CREATE DATABASE ov_nested_top ENGINE = Overlay('ov_nested_src', 'ov_nested_mid'); -- { serverError BAD_ARGUMENTS }

DROP DATABASE ov_nested_mid;
DROP DATABASE ov_nested_src;
