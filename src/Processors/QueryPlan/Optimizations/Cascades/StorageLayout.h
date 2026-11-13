#pragma once

#include <Core/Names.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ActionsDAG.h>
#include <base/types.h>
#include <memory>
#include <vector>

namespace DB
{

struct KeyDescription;

/// A function that a table uses to put its rows into groups of parts: one component of a partition key,
/// for example `cityHash64(user_id) % 16`. All rows of a part have the same value of the function, so a
/// read can give whole parts to nodes by that value. Two reads with equal functions then put rows with
/// equal arguments on the same node, and a join on the arguments needs no exchange.
///
/// The function is an `ActionsDAG` with one output; its inputs are the arguments. Two functions are equal
/// when the DAGs have the same structure (functions, constants and types), with the arguments compared by
/// position and not by name: `cityHash64(user_id) % 16` of one table is equal to `cityHash64(id) % 16`
/// of another table when the two columns have the same type.
class StorageLayoutFunction
{
public:
    /// The function of component `component_index` of the partition key. Returns nullptr when the
    /// component is not supported.
    ///
    /// A supported component holds only columns, literals and functions whose result depends only on the
    /// values and the types of the arguments: `cityHash64`, `modulo`, `intDiv`, `toYYYYMM`,
    /// `toStartOfMonth` and `toDate`, over integers, strings, `Date` and `DateTime` with a time zone in
    /// its type. For each other component two tables with equal functions could hold different partition
    /// values for equal arguments.
    static std::shared_ptr<const StorageLayoutFunction> tryCreate(const KeyDescription & partition_key, size_t component_index);

    const ActionsDAG & getActionsDAG() const { return dag; }
    /// The argument columns of the table, in the order used to compare two functions.
    const Names & getArgumentNames() const { return argument_names; }
    const DataTypes & getArgumentTypes() const { return argument_types; }

    bool equals(const StorageLayoutFunction & other) const;
    UInt64 getHash() const { return structure_hash; }
    /// For example `moduloLegacy(cityHash64(#0), 16)`.
    String dump() const;

private:
    StorageLayoutFunction() = default;

    ActionsDAG dag;
    Names argument_names;
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
    std::vector<NameSet> argument_columns;
};

using StorageLayouts = std::vector<StorageLayout>;

/// The layouts of a table read, one for each usable component of the partition key.
StorageLayouts getStorageLayouts(const KeyDescription & partition_key);

/// Renames the argument columns of the layouts through a step that computes `actions` over its input.
/// A layout is dropped when one of its arguments does not reach the output unchanged.
StorageLayouts remapStorageLayouts(const StorageLayouts & layouts, const ActionsDAG & actions);

}
