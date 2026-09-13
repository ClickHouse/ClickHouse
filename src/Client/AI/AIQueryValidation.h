#pragma once

#include <base/types.h>

#include <vector>

namespace DB
{

class IAST;

/// Validates that a statement is acceptable for the unconfirmed read-only query tool of the
/// AI agent. Throws Exception(BAD_ARGUMENTS) with an explanatory message (fed back to the
/// model) when it is not. Allowed are read-only statement types (SELECT, EXPLAIN, SHOW,
/// DESCRIBE, EXISTS, CHECK) without INTO OUTFILE and without SETTINGS clauses that would
/// override the enforced limits (`readonly`, execution time, memory usage): the limits are
/// applied client-side, so a SETTINGS clause of the query could undo them before the server
/// sees the query. Everything else must go through the confirmed query tool.
/// When `allow_schema_access` is false, also reject autonomous access to `system` schema
/// metadata and schema-exploration statements. Those queries can still be proposed through the
/// confirmed tool.
///
/// This is only the first half of the check, and it is not sufficient on its own. It judges what
/// is *written* in the statement; the tables it names are not judged here at all, because a name
/// says nothing about what reading it does - it can be a view over `url`, a `Distributed` table,
/// or a `Dictionary`. The caller must resolve them with `collectNamedTablesForAIAgent` and check
/// what they turn out to be, which takes a query to the server.
void validateReadOnlyQueryForAIAgent(const IAST & ast, bool allow_schema_access = true);

/// Whether the statement changes a setting: a `SET` statement, or a SETTINGS clause anywhere
/// inside it. A session with `readonly = 1` rejects the whole query because of it.
bool changesSettingsForAIAgent(const IAST & ast);

/// Whether the statement only reads: exactly the statement types a session with `readonly = 1`
/// accepts. This is a weaker property than `validateReadOnlyQueryForAIAgent`, which additionally
/// rejects what a read-only statement can still do outside of the server's tables: write a local
/// file with INTO OUTFILE, read one with `file`, reach another server with `remote`, or call an
/// external AI provider. A read-only session allows all of that, so those queries are refused by
/// the read-only tool but can run through the confirmed one.
bool isReadOnlyStatementForAISession(const IAST & ast);

/// Whether the statement type can be considered by the unconfirmed read-only AI tool. Callers
/// must use `validateReadOnlyQueryForAIAgent` before executing it: this predicate only classifies
/// the statement type and does not check for external access or unsafe settings.
bool isReadOnlyStatementForAIAgent(const IAST & ast);

/// A table named in a statement. `database` is empty when the name is written unqualified, and
/// the server resolves it against the session: a temporary table first, then the current database.
/// `table` is empty when the statement names a database rather than a table (`SHOW TABLES FROM
/// db`), in which case only the engine of the database is there to check.
struct AIQueryTableReference
{
    String database;
    String table;

    bool operator==(const AIQueryTableReference & other) const = default;
};

/// The tables named in the statement, deduplicated. `validateReadOnlyQueryForAIAgent` does not
/// judge them: a name says nothing about what reading it does, because it can be a view over an
/// external resource. The caller resolves them against `system.tables` and checks their engines
/// with the predicates below - it takes a query to the server, which this validation cannot do.
///
/// The list is a superset of the tables actually read: a name on the right-hand side of `IN` is
/// ambiguous between a column and a table, and a CTE name is indistinguishable from a table name
/// here. Both are collected; a name that does not resolve to a table is simply not checked.
std::vector<AIQueryTableReference> collectNamedTablesForAIAgent(const IAST & ast);

/// Whether the database is owned by the server, so that its tables are judged by name
/// (`isAllowedServerOwnedTableForAIAgent`) rather than by engine: `system` and the two spellings
/// of `information_schema`, whose tables are views over `system` by design.
bool isServerOwnedDatabaseForAIAgent(const String & database);

/// Whether a table of a server-owned database can be read by the unconfirmed read-only tool.
/// Most of them read metadata local to the server, but a few reach Keeper or object storage.
bool isAllowedServerOwnedTableForAIAgent(const String & database, const String & table);

/// Whether a table with this engine holds data of this server only, so that the unconfirmed
/// read-only tool can read it: the MergeTree and Log families and the simple in-memory engines.
/// Rejected are the engines that read another server or an external system (`Distributed`, `S3`,
/// `MySQL`, `Kafka`, ...), the ones that execute a stored definition (`View`, `Dictionary`), and
/// the ones that redirect to a table whose engine is not the one being checked (`Merge`,
/// `Buffer`, `MaterializedView`).
bool isAllowedTableEngineForAIAgent(const String & engine);

/// Whether a database with this engine keeps its tables on this server and lists all of them in
/// `system.tables`. This is what makes "the name did not resolve to a table" a safe answer: in a
/// database backed by an external catalog (`S3`, `Iceberg`, `MySQL`, ...) a name that is absent
/// from `system.tables` can still open an external resource when it is read.
bool isAllowedDatabaseEngineForAIAgent(const String & engine);

}
