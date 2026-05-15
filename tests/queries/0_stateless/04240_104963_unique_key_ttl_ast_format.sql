-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- `UNIQUE KEY` is an experimental feature limited to local-disk MergeTree in
-- an Atomic database (it raises `SUPPORT_IS_DISABLED` on object storage and is
-- not yet wired up for `ReplicatedMergeTree`/`SharedMergeTree`/Replicated DB).
-- Mirrors the tag set used by `04046_unique_key_ddl.sql`.
--
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104963
--
-- A `CREATE TABLE` that combined an inline `PRIMARY KEY`, a `UNIQUE KEY` clause
-- and a `TTL` clause triggered `LOGICAL_ERROR: Inconsistent AST formatting` from
-- the round-trip check in `executeQueryImpl`.
--
-- The round-trip check is gated by `#ifndef NDEBUG`, so the failure only fires
-- in debug builds; release-build binaries silently accept the same statement.
-- This test therefore relies on debug-build CI variants for regression coverage
-- and is categorised as `CI Fix or Improvement` rather than `Bug Fix` (the
-- `pr-bugfix` `Bugfix validation` runner uses a `RelWithDebInfo + NDEBUG`
-- master binary that cannot reproduce the original `LOGICAL_ERROR`).
--
-- Root cause: `ParserStorage::parseImpl` appended children in the order
-- (engine, partition_by, primary_key, order_by, sample_by, ttl_table,
-- unique_key, settings) — but `ASTStorage::formatImpl` and
-- `ASTStorage::normalizeChildrenOrder` use the canonical order
-- (engine, partition_by, primary_key, order_by, unique_key, sample_by,
-- ttl_table, settings). When an inline column-level `PRIMARY KEY` was
-- absent on the re-parsed text, `normalizeChildrenOrder` was not invoked,
-- so the re-parsed AST had `unique_key` and `ttl_table` swapped relative
-- to the original, breaking the dump-comparison round-trip check.
--
-- Fix: reorder the `set` calls in `ParserStorage::parseImpl` and
-- `ASTStorage::clone` to match the canonical order so re-parses and clones
-- both produce identical child order without depending on
-- `normalizeChildrenOrder`.

SET allow_experimental_unique_key = 1;

DROP TABLE IF EXISTS t_uk_ttl_104963;

-- The exact failing statement from the issue.  Without the fix this aborts
-- the debug server with a `LOGICAL_ERROR`.  With the fix it now reports the
-- correct user-facing `BAD_ARGUMENTS` (constant `TTL` expression).
CREATE TABLE t_uk_ttl_104963 (c0 Int PRIMARY KEY)
ENGINE = MergeTree() UNIQUE KEY (c0) TTL 1; -- { serverError BAD_ARGUMENTS }

-- Same shape with a valid `TTL` succeeds and round-trips.
CREATE TABLE t_uk_ttl_104963 (c0 Int PRIMARY KEY, ts DateTime)
ENGINE = MergeTree() UNIQUE KEY (c0) TTL ts + INTERVAL 1 DAY;

SHOW CREATE TABLE t_uk_ttl_104963 FORMAT TSVRaw;

DROP TABLE t_uk_ttl_104963;

-- Storage-level `PRIMARY KEY` (no inline column-level `PRIMARY KEY`) — this is
-- the path that exposes the bug because `normalizeChildrenOrder` is not
-- called by `ParserCreateTableQuery::parseImpl` on this code path.
DROP TABLE IF EXISTS t_uk_ttl_104963_storage_pk;

CREATE TABLE t_uk_ttl_104963_storage_pk (c0 Int, ts DateTime)
ENGINE = MergeTree() PRIMARY KEY (c0) UNIQUE KEY (c0) TTL ts + INTERVAL 1 DAY;

SHOW CREATE TABLE t_uk_ttl_104963_storage_pk FORMAT TSVRaw;

DROP TABLE t_uk_ttl_104963_storage_pk;

-- Full clause combination: `PARTITION BY`, `PRIMARY KEY`, `ORDER BY`,
-- `UNIQUE KEY`, `SAMPLE BY`, `TTL`.  All clauses present at once exercises
-- every child slot — the formatted DDL re-parses cleanly.
DROP TABLE IF EXISTS t_uk_ttl_104963_full;

CREATE TABLE t_uk_ttl_104963_full (c0 UInt32, sk UInt64, ts DateTime)
ENGINE = MergeTree()
PARTITION BY toDate(ts)
PRIMARY KEY (c0, sk)
ORDER BY (c0, sk)
SAMPLE BY sk
UNIQUE KEY (c0)
TTL ts + INTERVAL 1 DAY;

SHOW CREATE TABLE t_uk_ttl_104963_full FORMAT TSVRaw;

DROP TABLE t_uk_ttl_104963_full;
