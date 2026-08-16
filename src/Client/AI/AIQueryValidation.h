#pragma once

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
bool isReadOnlyStatementForAIAgent(const IAST & ast);

}
