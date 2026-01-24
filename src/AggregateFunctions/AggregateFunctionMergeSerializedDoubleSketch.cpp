#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/DoubleSketchData.h>
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
class AggregationFunctionMergeSerializedDoubleSketch final
    : public IAggregateFunctionDataHelper<DoubleSketchData<T>, AggregationFunctionMergeSerializedDoubleSketch<T>>
{
public:
    AggregationFunctionMergeSerializedDoubleSketch(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<DoubleSketchData<T>, AggregationFunctionMergeSerializedDoubleSketch<T>>{arguments, params, createResultType()}
    {}

    AggregationFunctionMergeSerializedDoubleSketch()
        : IAggregateFunctionDataHelper<DoubleSketchData<T>, AggregationFunctionMergeSerializedDoubleSketch<T>>{}
    {}

    String getName() const override { return "mergeSerializedDoubleSketch"; }

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

AggregateFunctionPtr createAggregateFunctionMergeSerializedDoubleSketch(
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

    // Use Float64 as template parameter since DoubleSketch works with doubles
    return std::make_shared<AggregationFunctionMergeSerializedDoubleSketch<Float64>>(argument_types, params);
}

}

void registerAggregateFunctionMergeSerializedDoubleSketch(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };

    factory.registerFunction("mergeSerializedDoubleSketch", {createAggregateFunctionMergeSerializedDoubleSketch, properties});
}

}

#endif
