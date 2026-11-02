#include <Parsers/ParserHypotheticalObjectQuery.h>

#include <Parsers/ASTHypotheticalObjectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateIndexQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>

namespace DB
{

bool ParserHypotheticalObjectQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_all(Keyword::ALL);
    ParserKeyword s_hypothetical(Keyword::HYPOTHETICAL);
    ParserKeyword s_index(Keyword::INDEX);
    ParserKeyword s_indexes(Keyword::INDEXES);
    ParserKeyword s_projection(Keyword::PROJECTION);
    ParserKeyword s_projections(Keyword::PROJECTIONS);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_on(Keyword::ON);

    ParserIdentifier object_name_p;
    ParserCreateIndexDeclaration parser_create_idx_decl;

    auto query = make_intrusive<ASTHypotheticalObjectQuery>();

    if (s_create.ignore(pos, expected))
    {
        if (!s_hypothetical.ignore(pos, expected))
            return false;

        if (s_index.ignore(pos, expected))
            query->object_kind = ASTHypotheticalObjectQuery::Index;
        else if (s_projection.ignore(pos, expected))
            query->object_kind = ASTHypotheticalObjectQuery::Projection;
        else
            return false;

        query->kind = ASTHypotheticalObjectQuery::Create;

        if (s_if_not_exists.ignore(pos, expected))
            query->if_not_exists = true;

        ASTPtr object_name;
        if (!object_name_p.parse(pos, object_name, expected))
            return false;

        if (!s_on.ignore(pos, expected))
            return false;

        if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
            return false;

        query->object_name = object_name;
        query->children.push_back(object_name);

        if (query->object_kind == ASTHypotheticalObjectQuery::Projection)
        {
            ASTPtr projection_decl;
            if (!parseProjectionDeclarationBody(pos, expected, object_name->as<ASTIdentifier &>().name(), projection_decl))
                return false;

            query->projection_decl = projection_decl;
            query->children.push_back(projection_decl);
        }
        else
        {
            ASTPtr index_decl;
            if (!parser_create_idx_decl.parse(pos, index_decl, expected))
                return false;

            index_decl->as<ASTIndexDeclaration &>().name = object_name->as<ASTIdentifier &>().name();

            query->index_decl = index_decl;
            query->children.push_back(index_decl);
        }
    }
    else if (s_drop.ignore(pos, expected))
    {
        /// DROP ALL HYPOTHETICAL INDEXES | DROP ALL HYPOTHETICAL PROJECTIONS
        if (s_all.ignore(pos, expected))
        {
            if (!s_hypothetical.ignore(pos, expected))
                return false;

            if (s_indexes.ignore(pos, expected))
                query->object_kind = ASTHypotheticalObjectQuery::Index;
            else if (s_projections.ignore(pos, expected))
                query->object_kind = ASTHypotheticalObjectQuery::Projection;
            else
                return false;

            query->kind = ASTHypotheticalObjectQuery::DropAll;
            node = query;
            return true;
        }

        if (!s_hypothetical.ignore(pos, expected))
            return false;

        if (s_index.ignore(pos, expected))
            query->object_kind = ASTHypotheticalObjectQuery::Index;
        else if (s_projection.ignore(pos, expected))
            query->object_kind = ASTHypotheticalObjectQuery::Projection;
        else
            return false;

        query->kind = ASTHypotheticalObjectQuery::Drop;

        if (s_if_exists.ignore(pos, expected))
            query->if_exists = true;

        ASTPtr object_name;
        if (!object_name_p.parse(pos, object_name, expected))
            return false;

        if (!s_on.ignore(pos, expected))
            return false;

        if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
            return false;

        query->object_name = object_name;
        query->children.push_back(object_name);
    }
    else
    {
        return false;
    }

    if (query->database)
        query->children.push_back(query->database);
    if (query->table)
        query->children.push_back(query->table);

    node = query;
    return true;
}

}

namespace DB
{

void registerStatementHypotheticalIndex(StatementFactory & factory)
{
    factory.registerStatement("HYPOTHETICAL INDEX",
    {
        .description = R"DOCS_MD(
Hypothetical indexes are virtual, session-scoped skip indexes that you can attach to a `MergeTree` family table without actually building or storing them. They exist only inside the current session and are used by [`EXPLAIN WHATIF`](/reference/statements/explain#explain-whatif) to estimate how a real skip index would affect a query — typically the skip ratio (fraction of marks that could be skipped) and a rough cost in marks and bytes.

Use hypothetical indexes to evaluate candidate indexes before paying the cost of materializing them on disk.

## CREATE HYPOTHETICAL INDEX {#create-hypothetical-index}

```sql
CREATE HYPOTHETICAL INDEX [IF NOT EXISTS] name
    ON [db.]table_name (expression) TYPE type[(args)] [GRANULARITY value]
```

The syntax mirrors `ALTER TABLE ... ADD INDEX`, but no index is built or written — only the index description is stored, in the current session.

- `name` — index name; must be unique within `(database, table)` for this session.
- `expression` — the column or expression to index.
- `TYPE type` — `minmax`, `set(N)`, `bloom_filter(p)`, `ngrambf_v1(...)`, `tokenbf_v1(...)`. `text` and `vector_similarity` are not supported and rejected at `CREATE` time, because their real `ALTER TABLE ... ADD INDEX` validation depends on table-level settings the session-only store can't replicate.
- `GRANULARITY value` — number of data granules per index granule. Defaults to 1.

The target table must be a `MergeTree` family table in an `Atomic` database (it must have a UUID). Tables without a UUID — for example in a legacy `Ordinary` database, or old-syntax `MergeTree` — are rejected, because the session store keys hypothetical indexes by table UUID.

**Example**

```sql
CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;
```

## Evaluating a hypothetical index with EXPLAIN WHATIF {#evaluating-a-hypothetical-index-with-explain-whatif}

Defining a hypothetical index by itself does nothing — to see how it would affect a query, run [`EXPLAIN WHATIF`](/reference/statements/explain#explain-whatif) against a representative `SELECT`. The estimator reports each candidate index's applicability, the marks it would read, the resulting skip ratio, and how the estimate was produced (`empirical`, `statistical`, or `applicability_only`).

```sql
CREATE TABLE t (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 100;

INSERT INTO t SELECT number, number FROM numbers(10000);

CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;

EXPLAIN WHATIF SELECT * FROM t WHERE b = 42;
```

Result:

```text
Baseline (after PK + partition + existing indexes):
  table:       default.t
  parts:       1
  marks:       100
  est_bytes:   85.52 KiB

With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    875.00 B
  skip_ratio:   99.0%

Estimation:
  source:           empirical
  empirical_status: ok
  sampled_parts:    1 / 1
  sampled_marks:    100 / 100
  elapsed_us:       631
```

`est_bytes` is an estimate from the table's average row size, so the exact figure varies with storage and compression.

To skip the in-memory empirical scan and estimate from [column statistics](/reference/engines/table-engines/mergetree-family/mergetree#column-statistics) instead, define them on the relevant columns first (they are off by default), wait for the materialize mutation to finish, then disable the empirical path:

```sql
ALTER TABLE t ADD STATISTICS b TYPE tdigest;
ALTER TABLE t MATERIALIZE STATISTICS b SETTINGS mutations_sync = 1;

EXPLAIN WHATIF empirical = 0 SELECT * FROM t WHERE b < 10;
```

```text
With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    1.66 KiB
  skip_ratio:   99.9%

Estimation:
  source:           statistical
  empirical_status: disabled
```

See the [`EXPLAIN WHATIF`](/reference/statements/explain#explain-whatif) reference for the full output schema and settings.

## DROP HYPOTHETICAL INDEX {#drop-hypothetical-index}

```sql
DROP HYPOTHETICAL INDEX [IF EXISTS] name ON [db.]table_name
```

Removes a hypothetical index from the current session.

## DROP ALL HYPOTHETICAL INDEXES {#drop-all-hypothetical-indexes}

```sql
DROP ALL HYPOTHETICAL INDEXES
```

Clears every hypothetical index defined in the current session, regardless of table.

## Scope and lifetime {#scope-and-lifetime}

- Hypothetical indexes live only in the **current session** — they are invisible to other sessions and discarded when the session ends.
- Defining or dropping one builds no index and never affects ordinary queries against the table. Empirical `EXPLAIN WHATIF` does read table data to build the candidate index in memory, and that scan counts against the session's read limits and quotas.
- Inspect the current session's hypothetical indexes via [`system.hypothetical_indexes`](/reference/system-tables/hypothetical_indexes).

## Limitations {#limitations}

`text` and `vector_similarity` candidates are rejected at `CREATE HYPOTHETICAL INDEX` time, because their real validation depends on table-level settings the session-only store cannot replicate.

`EXPLAIN WHATIF` reports `status: not_applicable` for queries with `FINAL` (skip-index pruning interacts with `PrimaryKeyExpand`), and errors with `NOT_IMPLEMENTED` when the query is served from a projection (a parent-table index is not materialized on projection parts).

The empirical `skip_ratio` is an **upper bound**: it counts each surviving granule independently and does not model seek-gap coalescing (`merge_tree_min_rows_for_seek` / `merge_tree_min_bytes_for_seek`), nor the combination of a candidate with an existing skip index under a disjunctive (`OR`) predicate. A real materialized index may therefore read slightly more, or prune in cases the estimate does not.

## Required privileges {#required-privileges}

`CREATE HYPOTHETICAL INDEX` requires `SELECT` on the columns referenced by the index expression — column-level `SELECT` (for example `GRANT SELECT(b)`) is sufficient — because empirical `EXPLAIN WHATIF` reads those columns.

`DROP HYPOTHETICAL INDEX` and `DROP ALL HYPOTHETICAL INDEXES` require no extra privilege; they only remove entries from the session-local store.

## See also {#see-also}

- [`EXPLAIN WHATIF`](/reference/statements/explain#explain-whatif)
- [`system.hypothetical_indexes`](/reference/system-tables/hypothetical_indexes)
- [Data skipping indexes](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes)
)DOCS_MD",
        .syntax = R"(
CREATE HYPOTHETICAL INDEX [IF NOT EXISTS] name ON [db.]table_name (expression) TYPE type[(args)] [GRANULARITY value]
DROP HYPOTHETICAL INDEX [IF EXISTS] name ON [db.]table_name
)",
        .related = {"EXPLAIN", "ALTER TABLE ... INDEX", "CREATE TABLE"},
    });
}

void registerStatementHypotheticalProjection(StatementFactory & factory)
{
    factory.registerStatement("HYPOTHETICAL PROJECTION",
    {
        .description = R"DOCS_MD(
Hypothetical projections are virtual, session-scoped projections that you can attach to a `MergeTree` family table without actually building or storing them. They exist only inside the current session and are listed by [`EXPLAIN WHATIF`](/reference/statements/explain#explain-whatif).

`EXPLAIN WHATIF` does not estimate the benefit of a hypothetical projection yet — it reports each one with `status: not_applicable`. Defining them is useful today for validating a definition against the table without materializing it, and for tooling that reads [`system.hypothetical_projections`](/reference/system-tables/hypothetical_projections).

## CREATE HYPOTHETICAL PROJECTION {#create-hypothetical-projection}

```sql
CREATE HYPOTHETICAL PROJECTION [IF NOT EXISTS] name
    ON [db.]table_name (SELECT <columns> [WHERE ...] [GROUP BY ...] [ORDER BY ...]) [WITH SETTINGS (...)]

CREATE HYPOTHETICAL PROJECTION [IF NOT EXISTS] name
    ON [db.]table_name INDEX <expression> TYPE <projection_index_type> [WITH SETTINGS (...)]
```

The syntax mirrors `ALTER TABLE ... ADD PROJECTION`, and the definition is validated exactly the same way, so a projection rejected here could not have been materialized either. Nothing is built or written — only the description is stored, in the current session.

- `name` — projection name; must be unique within `(database, table)` for this session, and must not collide with a real projection on the table.
- The body accepts the same forms as a real projection: a reordering projection with `ORDER BY`, an aggregating one with `GROUP BY`, a filtered one with `WHERE`, or the projection-index form `INDEX <expression> TYPE <projection_index_type>`.
- `WITH SETTINGS (...)` is accepted and preserved; the settings are visible in `system.hypothetical_projections`.

The target table must be a `MergeTree` family table in an `Atomic` database (it must have a UUID), because the session store keys entries by table UUID. The restrictions a real `ADD PROJECTION` enforces apply here too: tables with `UNIQUE KEY`, non-`Ordinary` merging modes under `deduplicate_merge_projection_mode = throw`, old-syntax `MergeTree`, and immutable disks are rejected.

**Example**

```sql
CREATE HYPOTHETICAL PROJECTION p_by_b ON t (SELECT a, b ORDER BY b);
CREATE HYPOTHETICAL PROJECTION p_idx ON t INDEX b TYPE basic;
```

## DROP HYPOTHETICAL PROJECTION {#drop-hypothetical-projection}

```sql
DROP HYPOTHETICAL PROJECTION [IF EXISTS] name ON [db.]table_name
```

Removes a hypothetical projection from the current session.

## DROP ALL HYPOTHETICAL PROJECTIONS {#drop-all-hypothetical-projections}

```sql
DROP ALL HYPOTHETICAL PROJECTIONS
```

Clears every hypothetical projection defined in the current session, regardless of table. It leaves hypothetical indexes untouched; `DROP ALL HYPOTHETICAL INDEXES` does the reverse.

## Scope and lifetime {#scope-and-lifetime}

- Hypothetical projections live only in the **current session** — they are invisible to other sessions and discarded when the session ends.
- Defining or dropping one builds no projection and never affects ordinary queries against the table.
- Inspect the current session's hypothetical projections via [`system.hypothetical_projections`](/reference/system-tables/hypothetical_projections).

## Required privileges {#required-privileges}

`CREATE HYPOTHETICAL PROJECTION` requires `ALTER ADD PROJECTION` on the table — the same privilege the real `ALTER TABLE ... ADD PROJECTION` needs — because it validates the definition against the table's columns. It reads no table data, so `SELECT` on the projection's columns is not required yet; when `EXPLAIN WHATIF` starts estimating projections it will read those columns and column-level `SELECT` will be required then, as it already is for [`CREATE HYPOTHETICAL INDEX`](/reference/statements/hypothetical-index#required-privileges).

`DROP HYPOTHETICAL PROJECTION` requires the same privilege, so that naming a table in a drop cannot reveal whether it exists or is eligible. `DROP ALL HYPOTHETICAL PROJECTIONS` names no table and requires no privilege.

## See also {#see-also}

- [`CREATE HYPOTHETICAL INDEX`](/reference/statements/hypothetical-index)
- [`EXPLAIN WHATIF`](/reference/statements/explain#explain-whatif)
- [`system.hypothetical_projections`](/reference/system-tables/hypothetical_projections)
- [Projections](/reference/engines/table-engines/mergetree-family/mergetree#projections)
)DOCS_MD",
        .syntax = R"(
CREATE HYPOTHETICAL PROJECTION [IF NOT EXISTS] name ON [db.]table_name (SELECT ... [WHERE ...]) [WITH SETTINGS (...)]
CREATE HYPOTHETICAL PROJECTION [IF NOT EXISTS] name ON [db.]table_name INDEX expression TYPE type [WITH SETTINGS (...)]
DROP HYPOTHETICAL PROJECTION [IF EXISTS] name ON [db.]table_name
DROP ALL HYPOTHETICAL PROJECTIONS
)",
        .related = {"EXPLAIN", "ALTER TABLE ... PROJECTION", "CREATE TABLE"},
    });
}

}
