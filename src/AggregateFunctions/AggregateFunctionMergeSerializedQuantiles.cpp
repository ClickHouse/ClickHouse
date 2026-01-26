#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeString.h>
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
class AggregationFunctionMergeSerializedQuantiles final
    : public IAggregateFunctionDataHelper<QuantilesSketchData<T>, AggregationFunctionMergeSerializedQuantiles<T>>
{
private:
    bool base64_encoded;  /// If true, data may be base64 encoded and needs decoding check

public:
    AggregationFunctionMergeSerializedQuantiles(const DataTypes & arguments, const Array & params, bool base64_encoded_)
        : IAggregateFunctionDataHelper<QuantilesSketchData<T>, AggregationFunctionMergeSerializedQuantiles<T>>{arguments, params, createResultType()}
        , base64_encoded(base64_encoded_)
    {}

    AggregationFunctionMergeSerializedQuantiles()
        : IAggregateFunctionDataHelper<QuantilesSketchData<T>, AggregationFunctionMergeSerializedQuantiles<T>>{}
        , base64_encoded(false)
    {}

    String getName() const override { return "mergeSerializedQuantiles"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeString>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColumnString &>(*columns[0]);
        auto serialized_data = column.getDataAt(row_num);
        this->data(place).insertSerialized(serialized_data, base64_encoded);
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

AggregateFunctionPtr createAggregateFunctionMergeSerializedQuantiles(
    const String & name,
    const DataTypes & argument_types,
    const Array & params,
    const Settings *)
{
    /// Optional parameter: base64_encoded (default: false)
    /// - false (0): Data is raw binary, skip base64 decoding (faster, for ClickHouse-generated data)
    /// - true (1): Data may be base64 encoded, check and decode if detected (for CSV, JSON, external data)
    bool base64_encoded = false;  // Default: raw binary for performance

    if (params.size() > 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} takes at most 1 parameter (base64_encoded: 0 or 1)", name);

    if (params.size() == 1)
    {
        base64_encoded = params[0].safeGet<bool>();
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

    // Use Float64 as template parameter since Quantiles sketch works with doubles
    return std::make_shared<AggregationFunctionMergeSerializedQuantiles<Float64>>(argument_types, params, base64_encoded);
}

}

/// mergeSerializedQuantiles - Merges multiple serialized quantiles sketches into a single sketch
///
/// This function combines multiple quantiles sketches that were created by serializedQuantiles() into
/// one unified sketch. This enables distributed percentile/quantile estimation where sketches are computed
/// on different nodes or time periods and then merged together to get accurate percentiles across the
/// entire dataset.
///
/// Syntax: mergeSerializedQuantiles([base64_encoded])(sketch_column)
///
/// Parameters (optional):
///   - base64_encoded: UInt8 (0 or 1, default: 0)
///     * 0 (false): Data is raw binary, skips base64 detection (fastest, recommended for ClickHouse data)
///     * 1 (true): Data may be base64 encoded, checks and decodes if detected (for CSV, JSON, external data)
///
/// Arguments:
///   - sketch_column: String - Serialized quantiles sketches (from serializedQuantiles() or previous merges)
///
/// Returns: String
///   A merged serialized quantiles sketch. Can be further merged or passed to percentileFromQuantiles().
///
/// Examples:
///   -- Merge hourly latency sketches to get daily percentiles (default, optimized)
///   SELECT
///       date,
///       percentileFromQuantiles(mergeSerializedQuantiles(sketch), 0.5) AS p50_latency,
///       percentileFromQuantiles(mergeSerializedQuantiles(sketch), 0.95) AS p95_latency,
///       percentileFromQuantiles(mergeSerializedQuantiles(sketch), 0.99) AS p99_latency
///   FROM hourly_latency_sketches
///   GROUP BY toDate(hour) AS date;
///
///   -- Explicit parameter (same as default)
///   SELECT mergeSerializedQuantiles(0)(sketch) FROM hourly_sketches;
///
///   -- Enable base64 decoding for external data
///   SELECT mergeSerializedQuantiles(1)(sketch) FROM imported_sketches;
///
///   -- Merge across multiple dimensions
///   SELECT
///       service,
///       toStartOfWeek(hour) AS week,
///       percentileFromQuantiles(mergeSerializedQuantiles(sketch), 0.95) AS weekly_p95
///   FROM hourly_latency_sketches
///   GROUP BY service, week;
///
/// Performance:
///   - Efficient merging: O(k) where k is sketch size (typically ~1000 items)
///   - Memory efficient: only holds merged sketch in memory
///   - Use base64_encoded=0 (default) for best performance with ClickHouse-generated sketches
///   - Significantly faster than re-computing percentiles from raw data
///
/// Use Cases:
///   - Rollup latency percentiles from minute → hour → day → month
///   - Combine sketches from distributed shards for global percentiles
///   - Time-series percentile analysis with pre-computed sketches
///
/// See also:
///   - serializedQuantiles() - Create quantiles sketches
///   - percentileFromQuantiles() - Extract percentile from merged sketch
///   - quantileMerge() - Alternative percentile aggregation method
void registerAggregateFunctionMergeSerializedQuantiles(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };

    FunctionDocumentation::Description description = R"(
Merges multiple serialized Quantiles sketches into a single sketch.
)";
    FunctionDocumentation::Syntax syntax = "mergeSerializedQuantiles([base64_encoded])(sketch_column)";
    FunctionDocumentation::Arguments arguments = {
        {"sketch_column", "Serialized Quantiles sketch as a String.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Merged serialized Quantiles sketch.", {"String"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, {}, introduced_in, category};

    factory.registerFunction("mergeSerializedQuantiles", {createAggregateFunctionMergeSerializedQuantiles, properties, documentation});
}

}

#endif
