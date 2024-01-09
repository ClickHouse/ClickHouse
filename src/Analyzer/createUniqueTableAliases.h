#pragma once

#include <memory>
#include <Interpreters/Context_fwd.h>

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

namespace DB
{

/*
 * For each table expression in the Query Tree generate and add a unique alias.
 * If table expression had an alias in initial query tree, override it.
 */
void createUniqueTableAliases(QueryTreeNodePtr & node, const QueryTreeNodePtr & table_expression, const ContextPtr & context);

}
