#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

class QueryPlan;

class PreparedSets;

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

/** Adds three types of columns into block
  * 1. Columns, that are missed inside request, but present in table without defaults (missed columns)
  * 2. Columns, that are missed inside request, but present in table with defaults (columns with default values)
  * 3. Columns that materialized from other columns (materialized columns)
  * Also can substitute NULL with DEFAULT value in case of INSERT SELECT query (null_as_default) if according setting is 1.
  * All three types of columns are materialized (not constants).
  */
void addBuildSubqueriesForSetsStep(
    QueryPlan & query_plan,
    ContextPtr context,
    PreparedSets & prepared_sets,
    const std::vector<ActionsDAGPtr> & result_actions_to_execute);

}
