#pragma once

#include <Interpreters/ActionsDAG.h>

namespace DB
{

class ColumnsDescription;

/// Create an ActionsDAG that extracts all subcolumns from required_columns
/// that are not presented in available_columns using getSubcolumn function.
/// A subcolumn registered in `storage_columns` is extracted only from the column it is registered under.
ActionsDAG createSubcolumnsExtractionActions(
    const Block & available_columns, const Names & required_columns, const ContextPtr & context, const ColumnsDescription * storage_columns = nullptr);

}
