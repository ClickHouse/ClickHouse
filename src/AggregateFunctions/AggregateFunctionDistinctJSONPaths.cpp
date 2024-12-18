#include <unordered_set>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnMap.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

constexpr static size_t DISTINCT_JSON_PATHS_MAX_ARRAY_SIZE = 0xFFFFFF;


struct AggregateFunctionDistinctJSONPathsData
{
    static constexpr auto name = "distinctJSONPaths";

    std::unordered_set<String> data;

    void add(const ColumnObject & column, size_t row_num, const std::unordered_map<String, String> &)
    {
        for (const auto & [path, _] : column.getTypedPaths())
            data.insert(path);
        for (const auto & [path, dynamic_column] : column.getDynamicPathsPtrs())
        {
            /// Add path from dynamic paths only if it's not NULL in this row.
            if (!dynamic_column->isNullAt(row_num))
                data.insert(path);
        }

        /// Iterate over paths in shared data in this row.
        const auto [shared_data_paths, _] = column.getSharedDataPathsAndValues();
        const auto & shared_data_offsets = column.getSharedDataOffsets();
        const size_t start = shared_data_offsets[static_cast<ssize_t>(row_num) - 1];
        const size_t end = shared_data_offsets[static_cast<ssize_t>(row_num)];
        for (size_t i = start; i != end; ++i)
            data.insert(shared_data_paths->getDataAt(i).toString());
    }

    void addWholeColumn(const ColumnObject & column, const std::unordered_map<String, String> &)
    {
        for (const auto & [path, _] : column.getTypedPaths())
            data.insert(path);
        for (const auto & [path, dynamic_column] : column.getDynamicPathsPtrs())
        {
            /// Add dynamic path only if it has at least one non-null value.
            /// getNumberOfDefaultRows for Dynamic column is O(1).
            if (dynamic_column->getNumberOfDefaultRows() != dynamic_column->size())
                data.insert(path);
        }

        /// Iterate over all paths in shared data.
        const auto [shared_data_paths, _] = column.getSharedDataPathsAndValues();
        for (size_t i = 0; i != shared_data_paths->size(); ++i)
            data.insert(shared_data_paths->getDataAt(i).toString());
    }

    void merge(const AggregateFunctionDistinctJSONPathsData & other)
    {
        data.insert(other.data.begin(), other.data.end());
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(data.size(), buf);
        for (const auto & path : data)
            writeStringBinary(path, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t size;
        readVarUInt(size, buf);
        if (size > DISTINCT_JSON_PATHS_MAX_ARRAY_SIZE)
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size (maximum: {}): {}", DISTINCT_JSON_PATHS_MAX_ARRAY_SIZE, size);

        String path;
        for (size_t i = 0; i != size; ++i)
        {
            readStringBinary(path, buf);
            data.insert(path);
        }
    }

    void insertResultInto(IColumn & column)
    {
        /// Insert paths in sorted order for better output.
        auto & array_column = assert_cast<ColumnArray &>(column);
        auto & string_column = assert_cast<ColumnString &>(array_column.getData());
        std::vector<String> sorted_data(data.begin(), data.end());
        std::sort(sorted_data.begin(), sorted_data.end());
        for (const auto & path : sorted_data)
            string_column.insertData(path.data(), path.size());
        array_column.getOffsets().push_back(string_column.size());
    }

    static DataTypePtr getResultType()
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }
};

struct AggregateFunctionDistinctJSONPathsAndTypesData
{
    static constexpr auto name = "distinctJSONPathsAndTypes";

    std::unordered_map<String, std::unordered_set<String>> data;

    void add(const ColumnObject & column, size_t row_num, const std::unordered_map<String, String> & typed_paths_type_names)
    {
        for (const auto & [path, _] : column.getTypedPaths())
            data[path].insert(typed_paths_type_names.at(path));
        for (const auto & [path, dynamic_column] : column.getDynamicPathsPtrs())
        {
            if (!dynamic_column->isNullAt(row_num))
                data[path].insert(dynamic_column->getTypeNameAt(row_num));
        }

        /// Iterate over paths om shared data in this row and decode the data types.
        const auto [shared_data_paths, shared_data_values] = column.getSharedDataPathsAndValues();
        const auto & shared_data_offsets = column.getSharedDataOffsets();
        const size_t start = shared_data_offsets[static_cast<ssize_t>(row_num) - 1];
        const size_t end = shared_data_offsets[static_cast<ssize_t>(row_num)];
        for (size_t i = start; i != end; ++i)
        {
            auto path = shared_data_paths->getDataAt(i).toString();
            auto value = shared_data_values->getDataAt(i);
            ReadBufferFromMemory buf(value.data, value.size);
            auto type = decodeDataType(buf);
            /// We should not have Nulls here but let's check just in case.
            chassert(!isNothing(type));
            data[path].insert(type->getName());
        }
    }

    void addWholeColumn(const ColumnObject & column, const std::unordered_map<String, String> & typed_paths_type_names)
    {
        for (const auto & [path, _] : column.getTypedPaths())
            data[path].insert(typed_paths_type_names.at(path));
        for (const auto & [path, dynamic_column] : column.getDynamicPathsPtrs())
        {
            /// Add dynamic path only if it has at least one non-null value.
            /// getNumberOfDefaultRows for Dynamic column is O(1).
            if (dynamic_column->getNumberOfDefaultRows() != dynamic_column->size())
                dynamic_column->getAllTypeNamesInto(data[path]);
        }

        /// Iterate over all paths in shared data and decode the data types.
        const auto [shared_data_paths, shared_data_values] = column.getSharedDataPathsAndValues();
        for (size_t i = 0; i != shared_data_paths->size(); ++i)
        {
            auto path = shared_data_paths->getDataAt(i).toString();
            auto value = shared_data_values->getDataAt(i);
            ReadBufferFromMemory buf(value.data, value.size);
            auto type = decodeDataType(buf);
            /// We should not have Nulls here but let's check just in case.
            chassert(!isNothing(type));
            data[path].insert(type->getName());
        }
    }

    void merge(const AggregateFunctionDistinctJSONPathsAndTypesData & other)
    {
        for (const auto & [path, types] : other.data)
            data[path].insert(types.begin(), types.end());
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(data.size(), buf);
        for (const auto & [path, types] : data)
        {
            writeStringBinary(path, buf);
            writeVarUInt(types.size(), buf);
            for (const auto & type : types)
                writeStringBinary(type, buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t paths_size, types_size;
        readVarUInt(paths_size, buf);
        if (paths_size > DISTINCT_JSON_PATHS_MAX_ARRAY_SIZE)
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size for paths (maximum: {}): {}", DISTINCT_JSON_PATHS_MAX_ARRAY_SIZE, paths_size);

        data.reserve(paths_size);
        String path, type;
        for (size_t i = 0; i != paths_size; ++i)
        {
            readStringBinary(path, buf);
            readVarUInt(types_size, buf);
            if (types_size > DISTINCT_JSON_PATHS_MAX_ARRAY_SIZE)
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size for types (maximum: {}): {}", DISTINCT_JSON_PATHS_MAX_ARRAY_SIZE, types_size);

            data[path].reserve(types_size);
            for (size_t j = 0; j != types_size; ++j)
            {
                readStringBinary(type, buf);
                data[path].insert(type);
            }
        }
    }

    void insertResultInto(IColumn & column)
    {
        /// Insert sorted paths and types for better output.
        auto & array_column = assert_cast<ColumnMap &>(column).getNestedColumn();
        auto & tuple_column = assert_cast<ColumnTuple &>(array_column.getData());
        auto & key_column = assert_cast<ColumnString &>(tuple_column.getColumn(0));
        auto & value_column = assert_cast<ColumnArray &>(tuple_column.getColumn(1));
        auto & value_column_data = assert_cast<ColumnString &>(value_column.getData());
        std::vector<std::pair<String, std::vector<String>>> sorted_data;
        sorted_data.reserve(data.size());
        for (const auto & [path, types] : data)
        {
            std::vector<String> sorted_types(types.begin(), types.end());
            std::sort(sorted_types.begin(), sorted_types.end());
            sorted_data.emplace_back(path, std::move(sorted_types));
        }
        std::sort(sorted_data.begin(), sorted_data.end());

        for (const auto & [path, types] : sorted_data)
        {
            key_column.insertData(path.data(), path.size());
            for (const auto & type : types)
                value_column_data.insertData(type.data(), type.size());
            value_column.getOffsets().push_back(value_column_data.size());
        }

        array_column.getOffsets().push_back(key_column.size());
    }

    static DataTypePtr getResultType()
    {
        return std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()));
    }
};

/// Calculates the list of distinct paths or pairs (path, type) in JSON column.
template <typename Data>
class AggregateFunctionDistinctJSONPathsAndTypes final : public IAggregateFunctionDataHelper<Data, AggregateFunctionDistinctJSONPathsAndTypes<Data>>
{
public:
    explicit AggregateFunctionDistinctJSONPathsAndTypes(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionDistinctJSONPathsAndTypes<Data>>(
            argument_types_, {}, Data::getResultType())
    {
        const auto & typed_paths_types = assert_cast<const DataTypeObject &>(*argument_types_[0]).getTypedPaths();
        typed_paths_type_names.reserve(typed_paths_types.size());
        for (const auto & [path, type] : typed_paths_types)
            typed_paths_type_names[path] = type->getName();
    }

    String getName() const override { return Data::name; }

    bool allocatesMemoryInArena() const override { return false; }

    void ALWAYS_INLINE add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & object_column = assert_cast<const ColumnObject & >(*columns[0]);
        this->data(place).add(object_column, row_num, typed_paths_type_names);
    }

    void ALWAYS_INLINE addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos)
        const override
    {
        if (if_argument_pos >= 0 || row_begin != 0 || row_end != columns[0]->size())
            IAggregateFunctionDataHelper<Data, AggregateFunctionDistinctJSONPathsAndTypes<Data>>::addBatchSinglePlace(row_begin, row_end, place, columns, arena, if_argument_pos);
        /// Optimization for case when we add all rows from the column into single place.
        /// In this case we can avoid iterating over all rows because we can get all paths
        /// and types in JSON column in a more efficient way.
        else
            this->data(place).addWholeColumn(assert_cast<const ColumnObject & >(*columns[0]), typed_paths_type_names);
    }

    void addManyDefaults(
        AggregateDataPtr __restrict /*place*/,
        const IColumn ** /*columns*/,
        size_t /*length*/,
        Arena * /*arena*/) const override
    {
        /// Default value for JSON is empty object, so nothing to add.
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to);
    }

private:
    std::unordered_map<String, String> typed_paths_type_names;
};

template <typename Data>
AggregateFunctionPtr createAggregateFunctionDistinctJSONPathsAndTypes(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    if (argument_types.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Incorrect number of arguments for aggregate function {}. Expected single argument with type JSON, got {} arguments", name, argument_types.size());

    if (!isObject(argument_types[0]))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}. Expected type JSON", argument_types[0]->getName(), name);

    return std::make_shared<AggregateFunctionDistinctJSONPathsAndTypes<Data>>(argument_types);
}

void registerAggregateFunctionDistinctJSONPathsAndTypes(AggregateFunctionFactory & factory)
{
    factory.registerFunction("distinctJSONPaths", createAggregateFunctionDistinctJSONPathsAndTypes<AggregateFunctionDistinctJSONPathsData>);
    factory.registerFunction("distinctJSONPathsAndTypes", createAggregateFunctionDistinctJSONPathsAndTypes<AggregateFunctionDistinctJSONPathsAndTypesData>);
}

}
