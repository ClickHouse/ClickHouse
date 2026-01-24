#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/HLLSketchData.h>
#include <Columns/ColumnString.h>

#if USE_DATASKETCHES

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
template <typename T>
class AggregationFunctionMergeSerializedHLL final
    : public IAggregateFunctionDataHelper<HLLSketchData<T>, AggregationFunctionMergeSerializedHLL<T>>
{
public:
    AggregationFunctionMergeSerializedHLL(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<HLLSketchData<T>, AggregationFunctionMergeSerializedHLL<T>>{arguments, params, createResultType()}
    {}

    AggregationFunctionMergeSerializedHLL()
        : IAggregateFunctionDataHelper<HLLSketchData<T>, AggregationFunctionMergeSerializedHLL<T>>{}
    {}

    String getName() const override { return "mergeSerializedHLL"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeString>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColumnString &>(*columns[0]);
        auto serialized_data = column.getDataAt(row_num);
        this->data(place).insertSerialized(serialized_data);
    }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto serialized_data = this->data(place).serializedData();
        assert_cast<ColumnString &>(to).insertData(serialized_data.c_str(), serialized_data.size());
    }
};

AggregateFunctionPtr createAggregateFunctionMergeSerializedHLL(
    const String & name,
    const DataTypes & argument_types,
    const Array & params,
    const Settings *)
{
    assertNoParameters(name, params);

    if (argument_types.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of arguments for aggregate function {}", name);

    const DataTypePtr & data_type = argument_types[0];

    WhichDataType which(*data_type);
    if (!which.isStringOrFixedString())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, 
            "Illegal type {} of argument for aggregate function {}. Expected String or FixedString.", 
            argument_types[0]->getName(), name);

    // Use uint64_t as template parameter since we're working with serialized data, not raw values
    return std::make_shared<AggregationFunctionMergeSerializedHLL<uint64_t>>(argument_types, params);
}

}

void registerAggregateFunctionMergeSerializedHLL(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };

    factory.registerFunction("mergeSerializedHLL", {createAggregateFunctionMergeSerializedHLL, properties});
}

}

#endif
