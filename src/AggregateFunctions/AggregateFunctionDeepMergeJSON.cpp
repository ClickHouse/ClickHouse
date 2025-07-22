#include <AggregateFunctions/AggregateFunctionDeepMergeJSON.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Core/Field.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int TOO_LARGE_ARRAY_SIZE;
extern const int TOO_LARGE_STRING_SIZE;
}

namespace
{
/// Helper to validate path length
void validatePathLength(size_t path_size)
{
    if (path_size > MAX_JSON_MERGE_PATH_LENGTH)
        throw Exception(
            ErrorCodes::TOO_LARGE_STRING_SIZE, "JSON path too long: {} bytes (maximum: {})", path_size, MAX_JSON_MERGE_PATH_LENGTH);
}

/// Helper to validate paths count
void validatePathsCount(size_t paths_count)
{
    if (paths_count > MAX_JSON_MERGE_PATHS)
        throw Exception(
            ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too many paths in JSON merge: {} (maximum: {})", paths_count, MAX_JSON_MERGE_PATHS);
}

/// Helper to validate total size
void validateTotalSize(size_t total_size)
{
    if (total_size > MAX_JSON_MERGE_TOTAL_SIZE)
        throw Exception(
            ErrorCodes::TOO_LARGE_STRING_SIZE,
            "JSON merge state size too large: {} bytes (maximum: {} bytes)",
            total_size,
            MAX_JSON_MERGE_TOTAL_SIZE);
}
}

bool DeepMergeJSONAggregateData::isObjectPath(const StringRef & path) const
{
    auto it = paths.upper_bound(path);
    return it != paths.end() && it->first.size > path.size && memcmp(it->first.data, path.data, path.size) == 0
        && it->first.data[path.size] == '.';
}

bool DeepMergeJSONAggregateData::handleDeletion(const StringRef & target_path, Arena * arena)
{
    auto it = paths.find(target_path);

    if (it == paths.end())
    {
        // Need to intern the string for new deletion marker
        char * data = arena->alloc(target_path.size);
        memcpy(data, target_path.data, target_path.size);
        paths[StringRef(data, target_path.size)] = PathData{Field(), true};
    }
    else
    {
        it->second.value = Field();
        it->second.is_deleted = true;
    }

    removeChildPaths(target_path);
    return true;
}

void DeepMergeJSONAggregateData::addPath(const StringRef & path, const Field & value, Arena *)
{
    auto it = paths.find(path);
    if (it != paths.end())
    {
        it->second.value = value;
        it->second.is_deleted = false;
    }
    else
    {
        paths[path] = PathData{value, false};
    }

    /// Remove child paths if this is now a leaf value
    if (!value.isNull() && !isObjectPath(path))
        removeChildPaths(path);
}

void DeepMergeJSONAggregateData::removeChildPaths(const StringRef & parent_path)
{
    String prefix = parent_path.toString() + ".";
    auto it = paths.lower_bound(StringRef(prefix));

    while (it != paths.end() && it->first.size >= prefix.size() && memcmp(it->first.data, prefix.data(), prefix.size()) == 0)
    {
        it = paths.erase(it);
    }
}

void AggregateFunctionDeepMergeJSON::add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const
{
    auto & aggregate_data = data(place);
    const auto & col_object = assert_cast<const ColumnObject &>(*columns[0]);

    processColumnObject(col_object, row_num, aggregate_data, arena);
}

void AggregateFunctionDeepMergeJSON::processPath(
    const StringRef & path, const Field & value, DeepMergeJSONAggregateData & aggregate_data, Arena * arena) const
{
    validatePathLength(path.size);

    /// Check for deletion suffix if deletion key is configured
    if (deletion_key.has_value())
    {
        std::string path_str = path.toString();
        if (path_str.ends_with(std::string(".") + *deletion_key) && value.getType() == Field::Types::Bool && value.safeGet<bool>())
        {
            std::string target_path = path_str.substr(0, path_str.size() - (*deletion_key).size() - 1);
            aggregate_data.handleDeletion(StringRef(target_path), arena);
            return;
        }
    }

    auto interned_path = internString(path, arena);
    aggregate_data.addPath(interned_path, value, arena);
}

void AggregateFunctionDeepMergeJSON::processColumnObject(
    const ColumnObject & col_object, size_t row_num, DeepMergeJSONAggregateData & aggregate_data, Arena * arena) const
{
    /// Process typed paths
    for (const auto & [path, column] : col_object.getTypedPaths())
    {
        if (!column->isDefaultAt(row_num))
        {
            Field value;
            column->get(row_num, value);
            processPath(StringRef(path), value, aggregate_data, arena);
        }
    }

    /// Process dynamic paths
    for (const auto & [path, dynamic_column] : col_object.getDynamicPathsPtrs())
    {
        if (!dynamic_column->isNullAt(row_num))
        {
            Field value;
            dynamic_column->get(row_num, value);
            processPath(StringRef(path), value, aggregate_data, arena);
        }
    }

    /// Process shared data
    const auto [shared_data_paths, shared_data_values] = col_object.getSharedDataPathsAndValues();
    const auto & shared_data_offsets = col_object.getSharedDataOffsets();
    size_t start = shared_data_offsets[static_cast<ssize_t>(row_num) - 1];
    size_t end = shared_data_offsets[static_cast<ssize_t>(row_num)];

    for (size_t i = start; i < end; ++i)
    {
        auto path = shared_data_paths->getDataAt(i);
        validatePathLength(path.size);

        /// Deserialize value from shared data
        auto value_data = shared_data_values->getDataAt(i);
        ReadBufferFromMemory buf(value_data.data, value_data.size);
        auto type = decodeDataType(buf);

        if (!isNothing(type))
        {
            const auto column = type->createColumn();
            type->getDefaultSerialization()->deserializeBinary(*column, buf, FormatSettings());

            Field value;
            column->get(0, value);
            processPath(path, value, aggregate_data, arena);
        }
    }

    validatePathsCount(aggregate_data.paths.size());
}

void AggregateFunctionDeepMergeJSON::merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const
{
    auto & aggregate_data = data(place);
    const auto & rhs_data = data(rhs);

    /// Merge paths from rhs, treating them as latest values
    for (const auto & [path, path_data] : rhs_data.paths)
    {
        auto interned_path = internString(path, arena);
        if (path_data.is_deleted)
        {
            aggregate_data.handleDeletion(interned_path, arena);
        }
        else
        {
            aggregate_data.addPath(interned_path, path_data.value, arena);
        }
    }

    validatePathsCount(aggregate_data.paths.size());
}

void AggregateFunctionDeepMergeJSON::serialize(
    ConstAggregateDataPtr __restrict place, WriteBuffer & buf, [[maybe_unused]] std::optional<size_t> version) const
{
    const auto & aggregate_data = data(place);

    writeVarUInt(aggregate_data.paths.size(), buf);

    size_t total_size = 0;

    for (const auto & [path, path_data] : aggregate_data.paths)
    {
        writeStringBinary(path, buf);
        writeBinary(path_data.is_deleted, buf);

        /// Serialize Field
        WriteBufferFromOwnString field_buf;
        writeFieldBinary(path_data.value, field_buf);
        writeStringBinary(field_buf.str(), buf);

        total_size += path.size + field_buf.str().size();
        validateTotalSize(total_size);
    }
}

void AggregateFunctionDeepMergeJSON::deserialize(
    AggregateDataPtr __restrict place, ReadBuffer & buf, [[maybe_unused]] std::optional<size_t> version, Arena * arena) const
{
    auto & aggregate_data = data(place);
    aggregate_data.paths.clear();

    size_t num_paths;
    readVarUInt(num_paths, buf);
    validatePathsCount(num_paths);

    size_t total_size = 0;

    for (size_t i = 0; i < num_paths; ++i)
    {
        String path_str;
        readStringBinary(path_str, buf);
        validatePathLength(path_str.size());

        bool is_deleted;
        readBinary(is_deleted, buf);

        String value_str;
        readStringBinary(value_str, buf);

        total_size += path_str.size() + value_str.size();
        validateTotalSize(total_size);

        /// Deserialize Field
        ReadBufferFromString value_buf(value_str);
        Field value = readFieldBinary(value_buf);

        auto interned_path = internString(path_str, arena);
        aggregate_data.paths[interned_path] = DeepMergeJSONAggregateData::PathData{value, is_deleted};
    }
}

void AggregateFunctionDeepMergeJSON::insertResultInto(AggregateDataPtr __restrict place, IColumn & to, [[maybe_unused]] Arena * arena) const
{
    const auto & aggregate_data = data(place);
    Object result_object;

    /// Helper function to set a value in a nested object structure
    auto set_nested_value = [](Object & obj, const std::string & path, const Field & value)
    {
        size_t pos = 0;
        Object * current = &obj;

        while (pos < path.length())
        {
            size_t dot_pos = path.find('.', pos);
            std::string key = (dot_pos == std::string::npos) ? path.substr(pos) : path.substr(pos, dot_pos - pos);

            if (dot_pos == std::string::npos)
            {
                (*current)[key] = value;
                break;
            }

            // Intermediate component - ensure it's an object
            if (!current->contains(key) || current->at(key).getType() != Field::Types::Object)
                (*current)[key] = Object();

            Field & field = (*current)[key];
            current = &field.safeGet<Object>();
            pos = dot_pos + 1;
        }
    };

    /// Convert flat paths back to nested object structure
    for (const auto & [path_ref, path_data] : aggregate_data.paths)
    {
        if (!path_data.is_deleted && !aggregate_data.isObjectPath(path_ref))
            set_nested_value(result_object, path_ref.toString(), path_data.value);
    }

    to.insert(result_object);
}

void AggregateFunctionDeepMergeJSON::addBatchSinglePlace(
    size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos)
    const
{
    if (if_argument_pos >= 0)
    {
        IAggregateFunctionDataHelper<DeepMergeJSONAggregateData, AggregateFunctionDeepMergeJSON>::addBatchSinglePlace(
            row_begin, row_end, place, columns, arena, if_argument_pos);
        return;
    }

    const auto & col_object = assert_cast<const ColumnObject &>(*columns[0]);
    auto & aggregate_data = data(place);

    for (size_t row = row_begin; row < row_end; ++row)
    {
        processColumnObject(col_object, row, aggregate_data, arena);
    }
}

void AggregateFunctionDeepMergeJSON::addManyDefaults(
    AggregateDataPtr __restrict /*place*/, const IColumn ** /*columns*/, size_t /*length*/, Arena * /*arena*/) const
{
    /// Default value for JSON is empty object, so nothing to add
}

namespace
{

AggregateFunctionPtr
createAggregateFunctionDeepMergeJSON(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    if (argument_types.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires exactly one argument", name);

    return std::make_shared<AggregateFunctionDeepMergeJSON>(argument_types, parameters);
}

}

void registerAggregateFunctionDeepMergeJSON(AggregateFunctionFactory & factory)
{
    factory.registerFunction("deepMergeJSON", createAggregateFunctionDeepMergeJSON);
}

}
