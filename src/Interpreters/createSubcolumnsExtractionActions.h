#pragma once

#include <Interpreters/ActionsDAG.h>

namespace DB
{

class ColumnsDescription;

/// Create an ActionsDAG that extracts all subcolumns from required_columns
/// that are not presented in available_columns using getSubcolumn function.
/// With `table_columns`, a name is resolved as the table resolves it, not by the first available column that
/// offers such a subcolumn: next to a `JSON` column `a` and a `Tuple` column `a.x`, the subcolumn `a.x.y` is
/// the element of `a.x`, not the path `x.y` of `a`.
ActionsDAG createSubcolumnsExtractionActions(
    const Block & available_columns,
    const Names & required_columns,
    const ContextPtr & context,
    const ColumnsDescription * table_columns = nullptr);

}
