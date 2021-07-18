#pragma once

#include <Interpreters/Context_fwd.h>

#include <memory>
#include <string>
#include <unordered_map>


namespace DB
{

class Block;
class NamesAndTypesList;
class ColumnsDescription;

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

/// Create actions which adds missing defaults to block according to required_columns using columns description
/// or substitute NULL into DEFAULT value in case of INSERT SELECT query (null_as_default) if according setting is 1.
/// Return nullptr if no actions required.
ActionsDAGPtr evaluateMissingDefaults(
    const Block & header,
    const NamesAndTypesList & required_columns,
    const ColumnsDescription & columns,
    ContextPtr context,
    bool save_unneeded_columns = true,
    bool null_as_default = false);

/// Tries to convert columns in block to required_columns
void performRequiredConversions(Block & block, const NamesAndTypesList & required_columns, ContextPtr context);

}
