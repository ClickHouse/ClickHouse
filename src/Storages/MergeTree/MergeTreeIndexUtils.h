#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

/** Build AST filter node for index analysis from WHERE and PREWHERE sections of select query and additional filters.
  * If select query does not have WHERE and PREWHERE and additional filters are empty null is returned.
  */
ASTPtr buildFilterNode(const ASTPtr & select_query, ASTs additional_filters = {});

/// Clone ActionsDAG with re-generated column name for constants.
/// DAG from the query (with enabled analyzer) uses suffixes for constants, like 1_UInt8.
/// DAG from the skip indexes does not use it. This breaks matching by column name sometimes.
/// Also remove useless CASTs to the same type (CAST(column, 'Type') where column already has type Type).
ActionsDAG cloneFilterDAGForIndexesAnalysis(const ActionsDAG & dag);

}
