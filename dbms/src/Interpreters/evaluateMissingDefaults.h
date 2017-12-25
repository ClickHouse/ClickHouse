#pragma once

#include <unordered_map>
#include <string>


namespace DB
{

class Block;
class Context;
class NamesAndTypes;
struct ColumnDefault;

void evaluateMissingDefaults(Block & block,
    const NamesAndTypes & required_columns,
    const std::unordered_map<std::string, ColumnDefault> & column_defaults,
    const Context & context);

}
