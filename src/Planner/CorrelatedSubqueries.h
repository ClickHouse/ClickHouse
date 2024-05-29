#pragma once

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

/// Build FunctionNode to execute correlated subquery
QueryTreeNodePtr buildFunctionNodeToExecuteCorrelatedSubquery(const QueryTreeNodePtr & node);

}
