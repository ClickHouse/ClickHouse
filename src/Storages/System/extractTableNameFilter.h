#pragma once

#include <Databases/IDatabase.h>
#include <Interpreters/ActionsDAG.h>


namespace DB
{

/// Extract a namespace-pushdown hint from a predicate on `column_name`.
TablesFilter extractTableNameFilter(const ActionsDAG::Node * predicate, const String & column_name);

}
