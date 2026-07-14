#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

class IAST;

/// Rewrites compound table references (FROM, JOIN, IN) to the canonical database and table
/// spellings under `standard` name matching, so a persisted view body and the dependencies
/// registered from it keep working in sessions with any matching mode.
/// Short (single-part) identifiers are left intact: after AddDefaultDatabaseVisitor those are
/// query-local names (WITH RECURSIVE aliases, temporary tables), never catalog references.
class CanonicalizeTableReferencesVisitor
{
public:
    static void visit(IAST & ast, const ContextPtr & context);
};

}
