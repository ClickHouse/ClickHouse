#pragma once

#include <Analyzer/HashUtils.h>
#include <Common/CNFAtomicFormula.h>

namespace DB::Analyzer
{

/// Specialization of the shared CNF atomic formula template for QueryTree nodes
using CNFAtomicFormula = DB::CNFAtomicFormula<QueryTreeNodePtrWithHash>;

}
