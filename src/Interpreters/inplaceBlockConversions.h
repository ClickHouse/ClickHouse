#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Common/COW.h>
#include <Storages/ColumnDefault.h>

#include <memory>
#include <unordered_map>

namespace DB
{

class Block;
class NamesAndTypesList;
class ColumnsDescription;

class IColumn;
using ColumnPtr = COW<IColumn>::Ptr;
using Columns = std::vector<ColumnPtr>;

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

class ActionsDAG;

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

/// If `shared_offsets_of_missing_defaults` is passed, then for every requested column that is
/// missing from the part but deferred to `evaluateMissingDefaults` because it has a default
/// expression, the array offsets the part already stores for it (the shared offsets of a `Nested`
/// structure, read from a sibling subcolumn or salvaged from the partial read of the missing
/// column itself) are exported there, keyed by the requested column name, one offsets column per
/// consecutive array level starting from the outermost.
void fillMissingColumns(
    Columns & res_columns,
    size_t num_rows,
    const NamesAndTypesList & requested_columns,
    const NamesAndTypesList & available_columns,
    const NameSet & partially_read_columns,
    StorageSnapshotPtr storage_snapshot,
    bool share_nested_offsets = true,
    const NameSet & additional_available_columns = {},
    std::unordered_map<String, Columns> * shared_offsets_of_missing_defaults = nullptr);

/// Reconcile a column evaluated from its default expression with the array sizes the data part
/// already stores for it (see `shared_offsets_of_missing_defaults` above). The expression knows
/// nothing about the shared offsets of the `Nested` structure the column belongs to, so its value
/// may have different array sizes; writing such a value next to the sibling subcolumns would
/// corrupt the shared offsets. Per row: the evaluated value is kept when its array sizes agree
/// with the shared offsets at every level they pin down, and degrades to the type-default value
/// shaped by those offsets otherwise (exactly what a column without a default expression reads as).
ColumnPtr reconcileEvaluatedDefaultWithSharedOffsets(
    const ColumnPtr & evaluated_column,
    const DataTypePtr & type,
    const Columns & shared_offsets);

}
