#pragma once

#include <base/types.h>
#include <DataTypes/IDataType.h>

#include <unordered_map>
#include <vector>

namespace DB
{

/// What writers derived from the column types of their blocks: the type after the state versions of
/// aggregate functions were set, its name, and the serializations for the first kinds of column seen, for
/// example a sparse column or a `Tuple` with a sparse element. A writer that gets this map takes these from
/// it and does not make them again.
/// The key is the type object of a block column, so pass blocks whose columns have the type objects of the
/// header. Only writers of the current protocol revision take this map.
struct NativeWriterTypeCache
{
    /// A `Tuple` can have a different kind for each element, so one type can have very many kinds. The
    /// serializations for the kinds after the first ones are made for each block and not kept.
    static constexpr size_t max_serializations_per_type = 4;

    struct Serialization
    {
        /// The kinds of the column and of its elements, as `SerializationInfo::serialializeKindStackBinary` writes them.
        String kinds;
        SerializationPtr serialization;
    };

    struct Entry
    {
        /// The type of the column in the block. It keeps the key of `entries` valid.
        DataTypePtr source;
        DataTypePtr type;
        String name;
        std::vector<Serialization> serializations;
    };

    std::unordered_map<const IDataType *, Entry> entries;
};

}
