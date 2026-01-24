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
class AggregationFunctionSerializedHLL final
    : public IAggregateFunctionDataHelper<HLLSketchData<T>, AggregationFunctionSerializedHLL<T>>
{
public:
    AggregationFunctionSerializedHLL(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<HLLSketchData<T>, AggregationFunctionSerializedHLL<T>>{arguments, params, createResultType()}
    {}

    AggregationFunctionSerializedHLL()
        : IAggregateFunctionDataHelper<HLLSketchData<T>, AggregationFunctionSerializedHLL<T>>{}
    {}

    String getName() const override { return "serializedHLL"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeString>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = *columns[0];
        this->data(place).insertOriginal(column.getDataAt(row_num));
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

AggregateFunctionPtr createAggregateFunctionSerializedHLL(
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
        return AggregateFunctionPtr(createWithNumericType<AggregationFunctionSerializedHLL>(
            *data_type, argument_types, params));
    if (which.isStringOrFixedString())
        return AggregateFunctionPtr(createWithStringType<AggregationFunctionSerializedHLL>(
                    *data_type, argument_types, params));
    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}", argument_types[0]->getName(), name);
}

}

/// serializedHLL - Creates a serialized HyperLogLog (HLL) sketch for cardinality estimation
///
/// HyperLogLog is a probabilistic data structure for estimating the cardinality (number of distinct elements)
/// of a dataset. This function creates a serialized binary representation of the HLL sketch that can be
/// stored, transmitted, or merged with other sketches.
///
/// Syntax: serializedHLL(column)
///
/// Arguments:
///   - column: Numeric (Int8/16/32/64, UInt8/16/32/64, Float32/64) or String
///             The values to add to the HLL sketch
///
/// Returns: String
///   A serialized binary HLL sketch. This can be stored in a table, used with mergeSerializedHLL(),
///   or passed to cardinalityFromHLL() to extract the cardinality estimate.
///
/// Example:
///   -- Create HLL sketch for unique user IDs
///   SELECT serializedHLL(user_id) AS user_sketch FROM events;
///
///   -- Store sketches by date
///   CREATE TABLE daily_sketches (date Date, sketch String) ENGINE = MergeTree() ORDER BY date;
///   INSERT INTO daily_sketches SELECT date, serializedHLL(user_id) FROM events GROUP BY date;
///
/// Performance:
///   - Memory: ~1.5KB per sketch (fixed size, regardless of cardinality)
///   - Accuracy: ~1.6% standard error for cardinality estimation
///   - Speed: Very fast, suitable for real-time analytics
///
/// See also:
///   - mergeSerializedHLL() - Merge multiple HLL sketches
///   - cardinalityFromHLL() - Extract cardinality estimate from sketch
///   - uniq() - Direct cardinality estimation (non-serialized)
void registerAggregateFunctionSerializedHLL(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = true };

    factory.registerFunction("serializedHLL", {createAggregateFunctionSerializedHLL, properties});
}

}

#endif

