#pragma once

#include <unordered_map>
#include <string>


namespace DB
{

class Block;
class Context;
class NamesAndTypesList;
struct ColumnDefault;

void evaluateMissingDefaults(Block & block,
    const NamesAndTypesList & required_columns,
    const std::unordered_map<std::string, ColumnDefault> & column_defaults,
    const Context & context, bool with_block_copy = true);

}
