#include <unordered_set>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Columns/ColumnDynamic.h>

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

struct AggregateFunctionDistinctDynamicTypesData
{
    constexpr static size_t MAX_ARRAY_SIZE = 0xFFFFFF;

    std::unordered_set<String> data;

    void add(const String & type)
    {
        data.insert(type);
    }

    void merge(const AggregateFunctionDistinctDynamicTypesData & other)
    {
        data.insert(other.data.begin(), other.data.end());
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(data.size(), buf);
        for (const auto & type : data)
            writeStringBinary(type, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t size;
        readVarUInt(size, buf);
        if (size > MAX_ARRAY_SIZE)
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size (maximum: {}): {}", MAX_ARRAY_SIZE, size);

        data.reserve(size);
        String type;
        for (size_t i = 0; i != size; ++i)
        {
            readStringBinary(type, buf);
            data.insert(type);
        }
    }

    void insertResultInto(IColumn & column)
    {
        /// Insert types in sorted order for better output.
        auto & array_column = assert_cast<ColumnArray &>(column);
        auto & string_column = assert_cast<ColumnString &>(array_column.getData());
        std::vector<String> sorted_data(data.begin(), data.end());
        std::sort(sorted_data.begin(), sorted_data.end());
        for (const auto & type : sorted_data)
            string_column.insertData(type.data(), type.size());
        array_column.getOffsets().push_back(string_column.size());
    }
};

/// Calculates the list of distinct data types in Dynamic column.
class AggregateFunctionDistinctDynamicTypes final : public IAggregateFunctionDataHelper<AggregateFunctionDistinctDynamicTypesData, AggregateFunctionDistinctDynamicTypes>
{
public:
    explicit AggregateFunctionDistinctDynamicTypes(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<AggregateFunctionDistinctDynamicTypesData, AggregateFunctionDistinctDynamicTypes>(argument_types_, {}, std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()))
    {
    }

    String getName() const override { return "distinctDynamicTypes"; }

    bool allocatesMemoryInArena() const override { return false; }

    void ALWAYS_INLINE add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & dynamic_column = assert_cast<const ColumnDynamic & >(*columns[0]);
        if (dynamic_column.isNullAt(row_num))
            return;

        data(place).add(dynamic_column.getTypeNameAt(row_num));
    }

    void ALWAYS_INLINE addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos)
        const override
    {
        if (if_argument_pos >= 0 || row_begin != 0 || row_end != columns[0]->size())
            IAggregateFunctionDataHelper::addBatchSinglePlace(row_begin, row_end, place, columns, arena, if_argument_pos);
        /// Optimization for case when we add all rows from the column into single place.
        /// In this case we can avoid iterating over all rows because we can get all types
        /// in Dynamic column in a more efficient way.
        else
            assert_cast<const ColumnDynamic & >(*columns[0]).getAllTypeNamesInto(data(place).data);
    }

    void addManyDefaults(
        AggregateDataPtr __restrict /*place*/,
        const IColumn ** /*columns*/,
        size_t /*length*/,
        Arena * /*arena*/) const override
    {
        /// Default value for Dynamic is NULL, so nothing to add.
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).merge(data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        data(place).insertResultInto(to);
    }
};

AggregateFunctionPtr createAggregateFunctionDistinctDynamicTypes(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    if (argument_types.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Incorrect number of arguments for aggregate function {}. Expected single argument with type Dynamic, got {} arguments", name, argument_types.size());

    if (!isDynamic(argument_types[0]))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}. Expected type Dynamic", argument_types[0]->getName(), name);

    return std::make_shared<AggregateFunctionDistinctDynamicTypes>(argument_types);
}

void registerAggregateFunctionDistinctDynamicTypes(AggregateFunctionFactory & factory)
{
    factory.registerFunction("distinctDynamicTypes", createAggregateFunctionDistinctDynamicTypes);
}

}
