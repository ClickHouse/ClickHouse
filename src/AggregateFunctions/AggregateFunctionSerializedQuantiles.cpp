#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/QuantilesSketchData.h>
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
class AggregationFunctionSerializedQuantiles final
    : public IAggregateFunctionDataHelper<QuantilesSketchData<T>, AggregationFunctionSerializedQuantiles<T>>
{
public:
    AggregationFunctionSerializedQuantiles(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<QuantilesSketchData<T>, AggregationFunctionSerializedQuantiles<T>>{arguments, params, createResultType()}
    {}

    AggregationFunctionSerializedQuantiles()
        : IAggregateFunctionDataHelper<QuantilesSketchData<T>, AggregationFunctionSerializedQuantiles<T>>{}
    {}

    String getName() const override { return "serializedQuantiles"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeString>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = *columns[0];
        this->data(place).insertOriginal(column.getFloat64(row_num));
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

AggregateFunctionPtr createAggregateFunctionSerializedQuantiles(
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
    if (which.isNumber())
        return AggregateFunctionPtr(createWithNumericType<AggregationFunctionSerializedQuantiles>(
            *data_type, argument_types, params));
    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}", argument_types[0]->getName(), name);
}

}

/// serializedQuantiles - Creates a serialized quantiles sketch for percentile/quantile calculations
///
/// DataSketches Quantiles is a probabilistic data structure for computing approximate
/// quantiles and percentiles over a dataset. This function creates a serialized binary representation
/// of the sketch that can be stored, transmitted, or merged with other sketches.
///
/// Syntax: serializedQuantiles(column)
///
/// Arguments:
///   - column: Numeric (Int8/16/32/64, UInt8/16/32/64, Float32/64)
///             The numeric values to add to the quantiles sketch
///
/// Returns: String
///   A serialized binary quantiles sketch. This can be stored in a table, used with
///   mergeSerializedQuantiles(), or passed to percentileFromQuantiles() to extract percentiles.
///
/// Examples:
///   -- Create quantiles sketch for response times
///   SELECT serializedQuantiles(response_time_ms) AS latency_sketch FROM requests;
///
///   -- Store sketches by service and hour
///   CREATE TABLE hourly_latency_sketches (
///       service String,
///       hour DateTime,
///       sketch String
///   ) ENGINE = MergeTree() ORDER BY (service, hour);
///
///   INSERT INTO hourly_latency_sketches
///   SELECT service, toStartOfHour(timestamp) AS hour, serializedQuantiles(latency)
///   FROM requests
///   GROUP BY service, hour;
///
/// Performance:
///   - Memory: ~2-4KB per sketch (compact representation)
///   - Accuracy: ~1-2% relative error for quantile estimation
///   - Speed: Fast insertion and sketching, suitable for real-time analytics
///   - Compression: Sketch size is independent of data size
///
/// Use Cases:
///   - Computing percentiles (p50, p95, p99) for latency monitoring
///   - Tracking distribution of values over time
///   - Distributed quantile estimation across multiple nodes
///
/// See also:
///   - mergeSerializedQuantiles() - Merge multiple quantiles sketches
///   - percentileFromQuantiles() - Extract specific percentile from sketch
///   - quantile() - Direct percentile calculation (non-sketched)
void registerAggregateFunctionSerializedQuantiles(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };

    FunctionDocumentation::Description description = R"(
Creates a serialized Quantiles sketch for approximate percentile/quantile estimation.
)";
    FunctionDocumentation::Syntax syntax = "serializedQuantiles(expression)";
    FunctionDocumentation::Arguments arguments = {
        {"expression", "Numeric expression.", {"Int*", "UInt*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Serialized binary Quantiles sketch.", {"String"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, {}, introduced_in, category};

    factory.registerFunction("serializedQuantiles", {createAggregateFunctionSerializedQuantiles, properties, documentation});
}

}

#endif
