#pragma once

#include <unordered_map>
#include <string>


namespace DB
{

class Block;
class Context;
class NamesAndTypesList;
struct ColumnDefault;

/// Adds missing defaults to block according to required_columns
/// using column_defaults map
void evaluateMissingDefaults(Block & block,
    const NamesAndTypesList & required_columns,
    const std::unordered_map<std::string, ColumnDefault> & column_defaults,
    const Context & context, bool save_unneeded_columns = true);

/// Tries to convert columns in block to required_columns
void performRequiredConversions(Block & block,
    const NamesAndTypesList & required_columns,
    const Context & context);
}
