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
struct NameAndTypePair;
class ColumnsDescription;

class IColumn;
using ColumnPtr = COW<IColumn>::Ptr;
using Columns = std::vector<ColumnPtr>;

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

class ActionsDAG;

/// The sets of the defaults that read a table (`x IN table`), shared by all blocks and readers of one reading query,
/// so that each set is built once per query rather than once per block. It is safe to use from several threads.
class DefaultExpressionsSets;
using DefaultExpressionsSetsPtr = std::shared_ptr<DefaultExpressionsSets>;
DefaultExpressionsSetsPtr createDefaultExpressionsSets();

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
/// A default that reads a table (`x IN table`) has its set built right away, with the access of `context`.
/// If `allow_reading_tables` is false, because `context` belongs to no query, such a default throws `ACCESS_DENIED`.
/// If `reused_sets` is set, the sets already built for it are reused, and the sets built here are added to it.
/// Return nullptr if no actions required.
std::optional<ActionsDAG> evaluateMissingDefaults(
    const Block & header,
    const NamesAndTypesList & required_columns,
    const ColumnsDescription & columns,
    ContextPtr context,
    bool save_unneeded_columns = true,
    bool null_as_default = false,
    bool allow_reading_tables = true,
    DefaultExpressionsSets * reused_sets = nullptr);

/// Tries to convert columns in block to required_columns
/// A conversion of `NULL` to the default of a column may read a table (`x IN table`), as in `evaluateMissingDefaults`.
void performRequiredConversions(Block & block, const NamesAndTypesList & required_columns, ContextPtr context,
    const ColumnDefaults & column_defaults, bool forbid_default_defaults = false, bool allow_reading_tables = true,
    DefaultExpressionsSets * reused_sets = nullptr);

/// Checks that the columns of `block` can be converted to `required_columns`, as `performRequiredConversions`
/// would do, without changing `block`. It is meant for the validation of metadata (`ALTER`), so it never reads
/// the tables of the defaults (`x IN table`): such a conversion is only resolved and type-checked, not executed.
void checkRequiredConversions(const Block & block, const NamesAndTypesList & required_columns, ContextPtr context,
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

/// Whether `fillMissingColumns` may leave `column` unfilled for `evaluateMissingDefaults`.
bool mayLeaveColumnToMissingDefaults(const StorageSnapshotPtr & storage_snapshot, const NameAndTypePair & column);

}
