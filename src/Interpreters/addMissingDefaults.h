#pragma once

#include <unordered_map>
#include <string>


namespace DB
{

class Block;
class Context;
class NamesAndTypesList;
class ColumnsDescription;

/** Adds three types of columns into block
  * 1. Columns, that are missed inside request, but present in table without defaults (missed columns)
  * 2. Columns, that are missed inside request, but present in table with defaults (columns with default values)
  * 3. Columns that materialized from other columns (materialized columns)
  * All three types of columns are materialized (not constants).
  */
Block addMissingDefaults(
    const Block & block,
    const NamesAndTypesList & required_columns,
    const ColumnsDescription & columns,
    const Context & context);

}
