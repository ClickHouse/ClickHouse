#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Resolve division spelling in a newly submitted query before it is executed or persisted.
/// `divide` in existing metadata always denotes legacy division; `divideExact` records the opt-in
/// semantics explicitly. Never apply this transformation to metadata or queries replayed by replicas.
void normalizeDivision(IAST & ast, const ContextPtr & context);

}
