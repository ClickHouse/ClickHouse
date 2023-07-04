#pragma once

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

void removeGroupingFunctionSpecializations(QueryTreeNodePtr & node);

}
