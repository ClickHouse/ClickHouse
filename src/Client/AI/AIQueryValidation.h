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
void validateReadOnlyQueryForAIAgent(const IAST & ast);

}
