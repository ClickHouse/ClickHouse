#pragma once

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

class FunctionNode;

/// If `function` is a `grouping` specialization built by the analyzer, returns the number of its trailing
/// constant arguments that carry the specialization parameters (they are removed before the query is sent
/// to a remote server, which rebuilds them itself); otherwise returns 0.
size_t getGroupingFunctionSpecializationStateArgumentsCount(const FunctionNode & function);

/// With `keep_ordinary`, `__groupingOrdinary` is left as is: it has no `__grouping_set` argument, so it
/// resolves from the query text, while its arguments may no longer be GROUP BY keys after the key optimizations.
void removeGroupingFunctionSpecializations(QueryTreeNodePtr & node, bool keep_ordinary = false);

}
