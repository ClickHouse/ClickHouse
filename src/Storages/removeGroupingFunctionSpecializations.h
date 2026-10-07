#pragma once

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

class FunctionNode;

/// If `function` is a `grouping` specialization built by the analyzer, returns the number of its trailing
/// constant arguments that carry the specialization parameters (they are removed before the query is sent
/// to a remote server, which rebuilds them itself); otherwise returns 0.
size_t getGroupingFunctionSpecializationStateArgumentsCount(const FunctionNode & function);

void removeGroupingFunctionSpecializations(QueryTreeNodePtr & node);

}
