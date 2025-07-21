#pragma once

#include <map>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnObject.h>
#include <DataTypes/DataTypeObject.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/StringRef.h>
#include <Common/Arena.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// Reasonable limits to prevent memory abuse
constexpr size_t MAX_JSON_MERGE_PATHS = 10000;
constexpr size_t MAX_JSON_MERGE_PATH_LENGTH = 1000;
constexpr size_t MAX_JSON_MERGE_TOTAL_SIZE = 100_MiB;

/// JSON unset marker key
constexpr const char * UNSET_KEY = "$unset";

struct DeepMergeJSONAggregateData
{
    struct PathData
    {
        Field value;
        size_t row_order;
        // Track if this path was explicitly deleted
        bool is_deleted = false;
    };

    /// Use std::map to keep paths sorted for consistent output
    /// StringRef will point to Arena-allocated memory
    std::map<StringRef, PathData> paths;
    size_t row_count = 0;

    /// Check if a path represents an object (has children)
    bool isObjectPath(const StringRef & path) const;

    /// Check if a value is an unset marker
    static bool isUnsetMarker(const Field & value);

    /// Add or update a path
    void addPath(const StringRef & path, const Field & value, size_t order, Arena * arena);

private:
    void removeChildPaths(const StringRef & parent_path);

    /// Handle deletion of a path (returns true if deletion was processed)
    bool handleDeletion(const StringRef & target_path, size_t order, Arena * arena);
};

class AggregateFunctionDeepMergeJSON final : public IAggregateFunctionDataHelper<DeepMergeJSONAggregateData, AggregateFunctionDeepMergeJSON>
{
public:
    explicit AggregateFunctionDeepMergeJSON(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<DeepMergeJSONAggregateData, AggregateFunctionDeepMergeJSON>(argument_types_, {}, argument_types_[0])
    {
        if (!isObject(argument_types_[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument for aggregate function {} must be JSON, got {}",
                getName(),
                argument_types_[0]->getName());
    }

    String getName() const override { return "deepMergeJSON"; }

    bool allocatesMemoryInArena() const override { return true; }

    void ALWAYS_INLINE add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override;

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override;

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override;

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override;

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override;

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos) const override;

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t length, Arena * arena) const override;

private:
    /// Helper to intern strings in Arena
    static StringRef internString(const StringRef & str, Arena * arena)
    {
        char * data = arena->alloc(str.size);
        memcpy(data, str.data, str.size);
        return StringRef(data, str.size);
    }

    /// Process all paths from ColumnObject
    void processColumnObject(const ColumnObject & col_object, size_t row_num, DeepMergeJSONAggregateData & data, Arena * arena) const;

    /// Helper to process a single path-value pair
    void processPath(const StringRef & path, const Field & value, DeepMergeJSONAggregateData & aggregate_data, Arena * arena) const;
};

}
