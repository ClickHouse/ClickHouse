#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

class Block;
class NamesAndTypesList;
class ColumnsDescription;

class ActionsDAG;

/** Adds three types of columns into block
  * 1. Columns, that are missed inside request, but present in table without defaults (missed columns)
  * 2. Columns, that are missed inside request, but present in table with defaults (columns with default values)
  * 3. Columns that materialized from other columns (materialized columns)
  * Also can substitute NULL with DEFAULT value in case of INSERT SELECT query (null_as_default) if according setting is 1.
  * All three types of columns are materialized (not constants).
  */
ActionsDAG addMissingDefaults(
    const Block & header, const NamesAndTypesList & required_columns,
    const ColumnsDescription & columns, ContextPtr context, bool null_as_default = false);
}
