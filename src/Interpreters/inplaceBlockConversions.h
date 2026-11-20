#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Common/COW.h>
#include <Storages/ColumnDefault.h>

#include <memory>

namespace DB
{

class Block;
class NamesAndTypesList;
class ColumnsDescription;

class IColumn;
using ColumnPtr = COW<IColumn>::Ptr;
using Columns = std::vector<ColumnPtr>;

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

class ActionsDAG;

/// The default expressions the reader has to evaluate for the `required_columns` that `header` does not provide (the
/// defaults of the columns a default reads included), resolved by the analyzer against a table of `header`'s columns,
/// exactly as `evaluateMissingDefaults` resolves them before building its actions. Every object a default refers to
/// (a dictionary, a `Join` table) is resolved on the way. Return nullptr if nothing has to be evaluated.
QueryTreeNodePtr resolveMissingDefaults(
    const Block & header,
    const NamesAndTypesList & required_columns,
    const ColumnsDescription & columns,
    ContextPtr context,
    bool null_as_default = false);

/// Create actions which adds missing defaults to block according to required_columns using columns description
/// or substitute NULL into DEFAULT value in case of INSERT SELECT query (null_as_default) if according setting is 1.
/// Return nullptr if no actions required.
std::optional<ActionsDAG> evaluateMissingDefaults(
    const Block & header,
    const NamesAndTypesList & required_columns,
    const ColumnsDescription & columns,
    ContextPtr context,
    bool save_unneeded_columns = true,
    bool null_as_default = false);

/// Tries to convert columns in block to required_columns
void performRequiredConversions(Block & block, const NamesAndTypesList & required_columns, ContextPtr context,
    const ColumnDefaults & column_defaults, bool forbid_default_defaults = false);

void fillMissingColumns(
    Columns & res_columns,
    size_t num_rows,
    const NamesAndTypesList & requested_columns,
    const NamesAndTypesList & available_columns,
    const NameSet & partially_read_columns,
    StorageSnapshotPtr storage_snapshot,
    const NameSet & missing_columns = {},
    bool share_nested_offsets = true,
    const NameSet & additional_available_columns = {});

}
