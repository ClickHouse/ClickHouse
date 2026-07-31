-- Tags: need-query-parameters, no-debug

-- Companion of 04654, split out only because of the tag below. A one-part
-- parameterized name carrying an explicit `UUID` clause reaches a second rebuild site, in
-- `AddDefaultDatabaseVisitor`, which goes through `name` and the constructor instead of
-- `createTable`. Without the clause `StorageID::assertNotEmpty` rejects the empty name first;
-- `StorageID::empty` exempts a non-Nil UUID, so the clause is what lets the name travel further
-- and reach the rebuild that aborted on `!part.empty()`.

-- Tag no-debug: these cases need an explicit `UUID` clause on a table reference, and the formatter
-- drops that clause, so the format-parse-format check in `executeQueryImpl` (guarded by
-- `#ifndef NDEBUG`, so debug-only) raises `Inconsistent AST formatting`. That is a separate
-- pre-existing defect: it fires for a plain `SELECT k FROM t UUID '...'` with no query parameter at
-- all. Sanitizer builds define `NDEBUG` yet still enable `chassert`, and they report
-- `BUILD_TYPE = RelWithDebInfo`, so `no-debug` does not skip them and the assertion stays covered.
-- The sibling `04654` carries every case that a debug build can run, so this tag costs no debug
-- coverage of the other rebuild sites.

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.uu (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.uu VALUES (5);

USE {CLICKHOUSE_DATABASE:Identifier};

-- Release builds stored such a view with an empty body, leaving it permanently uncallable, so both
-- rows below are user-visible fixes and not only assertion coverage.
CREATE VIEW v_uuid AS SELECT k FROM {ptab:Identifier} UUID '01234567-89ab-cdef-0123-456789abcdef';
SELECT 'uuid one-part body preserved', position(create_table_query, '{ptab') > 0
    FROM system.tables WHERE database = currentDatabase() AND name = 'v_uuid';
SELECT 'uuid one-part body callable', (SELECT k FROM v_uuid(ptab = 'uu'));

-- The same shape with an all-zero UUID parses to `Nil`, so `assertNotEmpty` still rejects it. This
-- control pins the UUID value, not the mere presence of the clause, as the discriminating axis.
CREATE VIEW v_uuid_nil AS SELECT k FROM {ptab:Identifier} UUID '00000000-0000-0000-0000-000000000000'; -- { serverError UNKNOWN_TABLE }
