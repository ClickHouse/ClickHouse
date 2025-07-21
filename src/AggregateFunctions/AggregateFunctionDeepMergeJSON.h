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
extern const int TOO_LARGE_ARRAY_SIZE;
extern const int TOO_LARGE_STRING_SIZE;
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
    bool isObjectPath(const StringRef & path) const
    {
        /// A path is an object if there are other paths that have it as prefix
        auto it = paths.upper_bound(path);
        if (it != paths.end())
        {
            /// Check if the next path starts with current path + "."
            if (it->first.size > path.size && memcmp(it->first.data, path.data, path.size) == 0 && it->first.data[path.size] == '.')
            {
                return true;
            }
        }
        return false;
    }

    /// Check if a value is an unset marker
    static bool isUnsetMarker(const Field & value)
    {
        if (value.getType() == Field::Types::Object)
        {
            const auto & obj = value.safeGet<Object>();
            return obj.size() == 1 && obj.contains(UNSET_KEY) && obj.at(UNSET_KEY).getType() == Field::Types::Bool
                && obj.at(UNSET_KEY).safeGet<bool>() == true;
        }
        return false;
    }

    /// Add or update a path
    void addPath(const StringRef & path, const Field & value, size_t order, [[maybe_unused]] Arena * arena)
    {
        auto it = paths.find(path);

        /// Check if this path ends with ".$unset" and the value is true
        std::string path_str = path.toString();
        std::string unset_suffix = std::string(".") + UNSET_KEY;
        if (path_str.size() > unset_suffix.size() && path_str.substr(path_str.size() - unset_suffix.size()) == unset_suffix
            && value.getType() == Field::Types::Bool && value.safeGet<bool>() == true)
        {
            /// This is an unset marker - remove the ".$unset" suffix to get the actual path to delete
            std::string target_path = path_str.substr(0, path_str.size() - unset_suffix.size());
            StringRef target_path_ref(target_path);

            auto target_it = paths.find(target_path_ref);
            if (target_it != paths.end())
            {
                /// Only process deletion if this is a newer operation
                if (order > target_it->second.row_order)
                {
                    target_it->second.value = Field(); // Clear value
                    target_it->second.row_order = order;
                    target_it->second.is_deleted = true;

                    /// Remove all child paths
                    removeChildPaths(target_path_ref);
                }
            }
            else
            {
                /// Mark the path as deleted even if it doesn't exist yet
                /// Need to intern the string in arena to ensure it persists
                char * data = arena->alloc(target_path_ref.size);
                memcpy(data, target_path_ref.data, target_path_ref.size);
                StringRef interned_target(data, target_path_ref.size);
                paths[interned_target] = PathData{Field(), order, true};
                removeChildPaths(interned_target);
            }
            return;
        }

        /// Check for deletion marker (for backward compatibility)
        if (isUnsetMarker(value))
        {
            if (it != paths.end())
            {
                /// Only process deletion if this is a newer operation
                if (order > it->second.row_order)
                {
                    it->second.value = Field(); // Clear value
                    it->second.row_order = order;
                    it->second.is_deleted = true;

                    /// Remove all child paths
                    removeChildPaths(path);
                }
            }
            else
            {
                /// New deletion marker
                paths[path] = PathData{Field(), order, true};
                removeChildPaths(path);
            }
            return;
        }

        if (it != paths.end())
        {
            /// Path exists - check if we should update
            if (order > it->second.row_order)
            {
                it->second.value = value;
                it->second.row_order = order;
                it->second.is_deleted = false; // Clear deletion flag if re-inserting
            }
        }
        else
        {
            /// New path
            paths[path] = PathData{value, order, false};
        }

        /// Remove any child paths if this is now a leaf value
        if (!value.isNull() && !isObjectPath(path))
        {
            removeChildPaths(path);
        }
    }

private:
    void removeChildPaths(const StringRef & parent_path)
    {
        /// Find all paths that start with parent_path + "."
        String prefix = parent_path.toString() + ".";
        auto it = paths.lower_bound(StringRef(prefix));

        while (it != paths.end())
        {
            /// Check if current path starts with prefix
            if (it->first.size >= prefix.size() && memcmp(it->first.data, prefix.data(), prefix.size()) == 0)
            {
                it = paths.erase(it);
            }
            else
            {
                break;
            }
        }
    }
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
};

}
