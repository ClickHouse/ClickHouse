#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

class Block;
class NamesAndTypesList;
class ColumnsDescription;
struct NameAndTypePair;

class ActionsDAG;

/** Adds three types of columns into a block:
  * 1. Columns, that are missed in the query, but present in the table without defaults (missed columns)
  * 2. Columns, that are missed in the query, but present in the table with defaults (columns with default values)
  * 3. Columns that are materialized from other columns (materialized columns)
  * Also can substitute NULL with DEFAULT value in case of INSERT SELECT query (null_as_default) if according setting is 1.
  * All three types of columns are materialized (not constants).
  */
ActionsDAG addMissingDefaults(
    const Block & header, const NamesAndTypesList & required_columns,
    const ColumnsDescription & columns, ContextPtr context, bool null_as_default = false,
    bool share_nested_offsets = true);

/// Whether a subcolumn of a column a child does not have is taken from the column's default. A subcolumn that only a
/// custom serialization adds (the codes of the `Quantized` codec) is not part of the value and gets its own default.
bool isSubcolumnOfDefaultValue(const NameAndTypePair & column);
}
