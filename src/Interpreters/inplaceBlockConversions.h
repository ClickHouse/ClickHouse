#pragma once

#include <unordered_map>
#include <string>
#include <memory>


namespace DB
{

class Block;
class Context;
class NamesAndTypesList;
class ColumnsDescription;

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

/// Create actions which adds missing defaults to block according to required_columns using columns description.
/// Return nullptr if no actions required.
ActionsDAGPtr evaluateMissingDefaults(
    const Block & header,
    const NamesAndTypesList & required_columns,
    const ColumnsDescription & columns,
    const Context & context, bool save_unneeded_columns = true);

/// Tries to convert columns in block to required_columns
void performRequiredConversions(Block & block,
    const NamesAndTypesList & required_columns,
    const Context & context);
}
