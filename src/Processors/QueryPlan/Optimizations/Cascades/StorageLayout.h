#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Core/Field.h>
#include <Core/Names.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ActionsDAG.h>
#include <base/types.h>
#include <memory>
#include <optional>
#include <variant>

namespace DB
{

class Block;
struct KeyDescription;

/// A function that divides the rows of a table between the nodes with no exchange. Two reads with equal
/// functions put rows with equal arguments on the same node, and a join on the arguments needs no exchange.
/// The function has one of two forms.
///
/// A partition expression is one component of a partition key, for example `cityHash64(user_id) % 16`.
/// All rows of a part have the same value of it, so a read can give whole parts to nodes by that value.
/// It is an `ActionsDAG` with one output; its inputs are the arguments. Two such functions are equal
/// when the DAGs have the same structure (functions, constants and types), with the arguments compared by
/// position and not by name: `cityHash64(user_id) % 16` of one table is equal to `cityHash64(id) % 16`
/// of another table when the two columns have the same type.
///
/// Key ranges divide the values of the first primary key columns at border values: a row is in range `i`
/// when its key is in `(borders[i-1], borders[i]]`. A table that is sorted by the columns can be read by
/// such ranges at any borders, so the planner selects the borders for a query, see
/// `getKeyRangeLayoutFunction`. Two such functions are equal when the types, the sort direction and the
/// borders are equal.
class StorageLayoutFunction
{
public:
    struct PartitionExpression
    {
        ActionsDAG dag;
        /// The argument columns of the table, in the order used to compare two functions.
        Names argument_names;
    };

    struct KeyRanges
    {
        bool in_reverse_order = false;
        /// In the order of the key. A border can have fewer values than the function has arguments.
        VectorWithMemoryTracking<VectorWithMemoryTracking<Field>> borders;
    };

    /// The function of component `component_index` of the partition key. Returns nullptr when the
    /// component is not supported.
    ///
    /// A supported component holds only columns, literals and functions whose result depends only on the
    /// values and the types of the arguments: `cityHash64`, `modulo`, `intDiv`, `toYYYYMM`,
    /// `toStartOfMonth` and `toDate`, over integers, strings, `Date` and `DateTime` with a time zone in
    /// its type. For each other component two tables with equal functions could hold different partition
    /// values for equal arguments.
    static std::shared_ptr<const StorageLayoutFunction> tryCreate(const KeyDescription & partition_key, size_t component_index);

    /// The function that divides the values of key columns with `key_types` at `borders`.
    static std::shared_ptr<const StorageLayoutFunction> createKeyRanges(
        DataTypes key_types, bool in_reverse_order, VectorWithMemoryTracking<VectorWithMemoryTracking<Field>> borders);

    [[nodiscard]] const PartitionExpression * tryGetPartitionExpression() const { return std::get_if<PartitionExpression>(&form); }
    [[nodiscard]] const KeyRanges * tryGetKeyRanges() const { return std::get_if<KeyRanges>(&form); }
    [[nodiscard]] const DataTypes & getArgumentTypes() const { return argument_types; }

    [[nodiscard]] bool equals(const StorageLayoutFunction & other) const;
    [[nodiscard]] UInt64 getHash() const { return structure_hash; }
    /// For example `moduloLegacy(cityHash64(#0), 16)` or `keyRanges(#0, #1; 3 borders)`.
    [[nodiscard]] String dump() const;

private:
    StorageLayoutFunction() = default;

    std::variant<PartitionExpression, KeyRanges> form;
    DataTypes argument_types;
    UInt64 structure_hash = 0;
};

using StorageLayoutFunctionPtr = std::shared_ptr<const StorageLayoutFunction>;

/// Both absent, or both present and equal.
bool sameStorageLayoutFunction(const StorageLayoutFunctionPtr & left, const StorageLayoutFunctionPtr & right);

/// A layout that the result of a group can have with no exchange: the function, and its argument columns
/// under the names they have in the output of the group.
struct StorageLayout
{
    StorageLayoutFunctionPtr function;
    /// One set for each argument, in the order of the function. A set holds every output name that
    /// carries the argument column unchanged.
    VectorWithMemoryTracking<NameSet> argument_columns;
};

using StorageLayouts = VectorWithMemoryTracking<StorageLayout>;

/// The layouts of a table read, one for each usable component of the partition key.
StorageLayouts getStorageLayouts(const KeyDescription & partition_key);

/// Renames the argument columns of the layouts through a step that computes `actions` over its input.
/// A layout is dropped when one of its arguments does not reach the output unchanged.
StorageLayouts remapStorageLayouts(const StorageLayouts & layouts, const ActionsDAG & actions);

/// The result of a group can be divided by ranges of these columns with no exchange: they are the first
/// columns of the primary key of the tables that the group reads. The borders are not a part of it, a
/// rule selects them for all the reads together.
struct KeyRangeLayoutCandidate
{
    /// One set for each key column, in the order of the key. A set holds every output name that
    /// carries the column unchanged.
    VectorWithMemoryTracking<NameSet> key_columns;
    DataTypes key_types;
    bool in_reverse_order = false;
    /// The groups of the table reads that must be divided at the same borders, in ascending order.
    VectorWithMemoryTracking<size_t> read_groups;

    /// Keeps the first `size` key columns.
    void resize(size_t size);
};

using KeyRangeLayoutCandidates = VectorWithMemoryTracking<KeyRangeLayoutCandidate>;

/// The candidate of a table read: the first columns of the primary key that are plain columns of the
/// table in `output_header`, with types that are safe to compare with borders and with one sort direction.
std::optional<KeyRangeLayoutCandidate> getKeyRangeLayoutCandidate(
    const KeyDescription & primary_key, const KeyDescription & sorting_key, const Block & output_header, size_t read_group);

/// Renames the key columns of the candidates through a step that computes `actions` over its input.
/// A candidate keeps the key columns before the first one that does not reach the output unchanged.
KeyRangeLayoutCandidates remapKeyRangeLayoutCandidates(const KeyRangeLayoutCandidates & candidates, const ActionsDAG & actions);

/// The candidates through a step that brings the rows with equal values of `columns` together and returns
/// these columns unchanged: a final aggregation by them, or a `DISTINCT` on them. When the key columns are
/// some of `columns`, such rows are in one range already, and each result row stays in the range of its key.
/// A candidate keeps the key columns before the first one that is not one of `columns`.
KeyRangeLayoutCandidates restrictKeyRangeLayoutCandidates(const KeyRangeLayoutCandidates & candidates, const Names & columns);

}
