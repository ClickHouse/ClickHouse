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
private:
    bool assume_raw_binary;  /// If true, skip base64 decoding (default for performance)

public:
    AggregationFunctionMergeSerializedDoubleSketch(const DataTypes & arguments, const Array & params, bool assume_raw_binary_)
        : IAggregateFunctionDataHelper<DoubleSketchData<T>, AggregationFunctionMergeSerializedDoubleSketch<T>>{arguments, params, createResultType()}
        , assume_raw_binary(assume_raw_binary_)
    {}

    AggregationFunctionMergeSerializedDoubleSketch()
        : IAggregateFunctionDataHelper<DoubleSketchData<T>, AggregationFunctionMergeSerializedDoubleSketch<T>>{}
        , assume_raw_binary(true)
    {}

    String getName() const override { return "mergeSerializedDoubleSketch"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeString>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColumnString &>(*columns[0]);
        auto serialized_data = column.getDataAt(row_num);
        this->data(place).insertSerialized(serialized_data, assume_raw_binary);
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
    /// Optional parameter: assume_raw_binary (default: true)
    /// - true (1): Skip base64 decoding, treat as raw binary (faster, for ClickHouse-generated data)
    /// - false (0): Check for base64 encoding and decode if detected (for external data)
    bool assume_raw_binary = true;  // Default: assume raw binary for performance
    
    if (params.size() > 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} takes at most 1 parameter (assume_raw_binary: 0 or 1)", name);
    
    if (params.size() == 1)
    {
        assume_raw_binary = params[0].safeGet<bool>();
    }

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
    return std::make_shared<AggregationFunctionMergeSerializedDoubleSketch<Float64>>(argument_types, params, assume_raw_binary);
}

}

/// mergeSerializedDoubleSketch - Merges multiple serialized quantiles sketches into a single sketch
///
/// This function combines multiple quantiles sketches that were created by serializedDoubleSketch() into 
/// one unified sketch. This enables distributed percentile/quantile estimation where sketches are computed 
/// on different nodes or time periods and then merged together to get accurate percentiles across the 
/// entire dataset.
///
/// Syntax: mergeSerializedDoubleSketch([assume_raw_binary])(sketch_column)
///
/// Parameters (optional):
///   - assume_raw_binary: UInt8 (0 or 1, default: 1)
///     * 1 (true): Assumes input is raw binary data, skips base64 detection (fastest, recommended for ClickHouse data)
///     * 0 (false): Checks for base64 encoding and decodes if detected (for external/imported data)
///
/// Arguments:
///   - sketch_column: String - Serialized quantiles sketches (from serializedDoubleSketch() or previous merges)
///
/// Returns: String
///   A merged serialized quantiles sketch. Can be further merged or passed to percentileFromDoubleSketch().
///
/// Examples:
///   -- Merge hourly latency sketches to get daily percentiles (default, optimized)
///   SELECT 
///       date,
///       percentileFromDoubleSketch(mergeSerializedDoubleSketch(sketch), 0.5) AS p50_latency,
///       percentileFromDoubleSketch(mergeSerializedDoubleSketch(sketch), 0.95) AS p95_latency,
///       percentileFromDoubleSketch(mergeSerializedDoubleSketch(sketch), 0.99) AS p99_latency
///   FROM hourly_latency_sketches
///   GROUP BY toDate(hour) AS date;
///
///   -- Explicit parameter (same as default)
///   SELECT mergeSerializedDoubleSketch(1)(sketch) FROM hourly_sketches;
///
///   -- Enable base64 decoding for external data
///   SELECT mergeSerializedDoubleSketch(0)(sketch) FROM imported_sketches;
///
///   -- Merge across multiple dimensions
///   SELECT 
///       service,
///       toStartOfWeek(hour) AS week,
///       percentileFromDoubleSketch(mergeSerializedDoubleSketch(sketch), 0.95) AS weekly_p95
///   FROM hourly_latency_sketches
///   GROUP BY service, week;
///
/// Performance:
///   - Efficient merging: O(k) where k is sketch size (typically ~1000 items)
///   - Memory efficient: only holds merged sketch in memory
///   - Use assume_raw_binary=1 (default) for best performance with ClickHouse-generated sketches
///   - Significantly faster than re-computing percentiles from raw data
///
/// Use Cases:
///   - Rollup latency percentiles from minute → hour → day → month
///   - Combine sketches from distributed shards for global percentiles
///   - Time-series percentile analysis with pre-computed sketches
///
/// See also:
///   - serializedDoubleSketch() - Create quantiles sketches
///   - percentileFromDoubleSketch() - Extract percentile from merged sketch
///   - quantileMerge() - Alternative percentile aggregation method
void registerAggregateFunctionMergeSerializedDoubleSketch(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };

    factory.registerFunction("mergeSerializedDoubleSketch", {createAggregateFunctionMergeSerializedDoubleSketch, properties});
}

}

#endif
