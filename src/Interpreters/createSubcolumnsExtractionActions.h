#pragma once

#include <Interpreters/ActionsDAG.h>

namespace DB
{

/// Create an ActionsDAG that extracts all subcolumns from required_columns
/// that are not presented in available_columns using getSubcolumn function.
ActionsDAG createSubcolumnsExtractionActions(const Block & available_columns, const Names & required_columns, const ContextPtr & context);

}
