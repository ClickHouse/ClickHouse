#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

class IAST;

/// Rewrites compound table references to the canonical database and table spellings under
/// `standard` name matching, so a persisted view body works in sessions with any matching mode.
class CanonicalizeTableReferencesVisitor
{
public:
    static void visit(IAST & ast, const ContextPtr & context);
};

}
