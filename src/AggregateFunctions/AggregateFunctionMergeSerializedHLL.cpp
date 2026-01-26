#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/FieldVisitorConvertToNumber.h>

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
private:
    bool base64_encoded;  /// If true, data may be base64 encoded and needs decoding check
    uint8_t lg_k;
    datasketches::target_hll_type type;

public:
    AggregationFunctionMergeSerializedHLL(const DataTypes & arguments, const Array & params, bool base64_encoded_, uint8_t lg_k_, datasketches::target_hll_type type_)
        : IAggregateFunctionDataHelper<HLLSketchData<T>, AggregationFunctionMergeSerializedHLL<T>>{arguments, params, createResultType()}
        , base64_encoded(base64_encoded_)
        , lg_k(lg_k_)
        , type(type_)
    {}

    AggregationFunctionMergeSerializedHLL()
        : IAggregateFunctionDataHelper<HLLSketchData<T>, AggregationFunctionMergeSerializedHLL<T>>{}
        , base64_encoded(false)
        , lg_k(DEFAULT_LG_K)
        , type(DEFAULT_HLL_TYPE)
    {}

    String getName() const override { return "mergeSerializedHLL"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeString>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void create(AggregateDataPtr __restrict place) const override // NOLINT(readability-non-const-parameter)
    {
        new (place) HLLSketchData<T>(lg_k, type);
    }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColumnString &>(*columns[0]);
        auto serialized_data = column.getDataAt(row_num);
        this->data(place).insertSerialized(serialized_data, !base64_encoded);
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
    /// Optional parameters: base64_encoded, lg_k, type
    /// - base64_encoded (default: false): Check for base64 encoding (for CSV, JSON, external data)
    /// - lg_k (default: 10): Log-base-2 of K (4-21)
    /// - type (default: 'HLL_4'): Storage format ('HLL_4', 'HLL_6', or 'HLL_8')
    bool base64_encoded = false;
    uint8_t lg_k = DEFAULT_LG_K;
    datasketches::target_hll_type type = DEFAULT_HLL_TYPE;

    if (params.size() > 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} takes at most 3 parameters: base64_encoded (0 or 1), lg_k (4-21), and type ('HLL_4', 'HLL_6', or 'HLL_8')", name);

    if (!params.empty())
    {
        base64_encoded = params[0].safeGet<bool>();
    }

    if (params.size() >= 2)
    {
        UInt64 lg_k_param = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[1]);
        if (lg_k_param < 4 || lg_k_param > 21)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Parameter lg_k for aggregate function {} must be between 4 and 21, got {}", name, lg_k_param);
        lg_k = static_cast<uint8_t>(lg_k_param);
    }

    if (params.size() == 3)
    {
        String type_str = params[2].safeGet<String>();
        if (type_str == "HLL_4")
            type = datasketches::HLL_4;
        else if (type_str == "HLL_6")
            type = datasketches::HLL_6;
        else if (type_str == "HLL_8")
            type = datasketches::HLL_8;
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Parameter type for aggregate function {} must be 'HLL_4', 'HLL_6', or 'HLL_8', got '{}'", name, type_str);
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

    // Use uint64_t as template parameter since we're working with serialized data, not raw values
    return std::make_shared<AggregationFunctionMergeSerializedHLL<uint64_t>>(argument_types, params, base64_encoded, lg_k, type);
}

}

/// mergeSerializedHLL - Merges multiple serialized HLL (HyperLogLog) sketches into a single sketch
///
/// This function combines multiple HLL sketches that were created by serializedHLL() into one unified sketch.
/// This enables distributed cardinality estimation where sketches are computed on different nodes or time periods
/// and then merged together.
///
/// Syntax: mergeSerializedHLL([base64_encoded, lg_k, type])(sketch_column)
///
/// Parameters (all optional):
///   - base64_encoded: UInt8 (0 or 1, default: 0)
///     * 0 (false): Data is raw binary, skips base64 detection (fastest, recommended for ClickHouse data)
///     * 1 (true): Data may be base64 encoded, checks and decodes if detected (for CSV, JSON, external data)
///
///   - lg_k: Integer between 4 and 21 (default: 10)
///     Log-base-2 of K, where K is the number of buckets (K = 2^lg_k)
///     Should match the lg_k used when creating the sketches with serializedHLL()
///     Higher values = better accuracy but more memory
///
///   - type: String, one of 'HLL_4', 'HLL_6', or 'HLL_8' (default: 'HLL_4')
///     Storage format for the merged sketch (should match serializedHLL() settings)
///
/// Arguments:
///   - sketch_column: String - Serialized HLL sketches (from serializedHLL() or previous mergeSerializedHLL())
///
/// Returns: String
///   A merged serialized HLL sketch. Can be further merged or passed to cardinalityFromHLL().
///
/// Examples:
///   -- Merge sketches from different partitions (default parameters)
///   SELECT mergeSerializedHLL(sketch) FROM daily_sketches;
///
///   -- Explicit default parameters
///   SELECT mergeSerializedHLL(0, 10, 'HLL_4')(sketch) FROM daily_sketches;
///
///   -- Enable base64 decoding for external data
///   SELECT mergeSerializedHLL(1)(sketch) FROM imported_sketches;
///
///   -- Merge sketches created with higher precision
///   SELECT mergeSerializedHLL(0, 12, 'HLL_4')(sketch) FROM high_precision_sketches;
///
///   -- Complete workflow: daily to monthly cardinality
///   SELECT
///       toStartOfMonth(date) AS month,
///       cardinalityFromHLL(mergeSerializedHLL(sketch)) AS monthly_unique_users
///   FROM daily_sketches
///   GROUP BY month;
///
/// Performance:
///   - Very fast merging operation (logarithmic in sketch size)
///   - Memory efficient: only needs to hold one merged sketch in memory
///   - Use base64_encoded=0 (default) for best performance with ClickHouse-generated sketches
///
/// Note:
///   - The lg_k and type parameters should match those used in serializedHLL()
///   - Merging sketches with different lg_k values will use the smaller lg_k
///
/// See also:
///   - serializedHLL() - Create HLL sketches
///   - cardinalityFromHLL() - Extract cardinality from merged sketch
void registerAggregateFunctionMergeSerializedHLL(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };

    factory.registerFunction("mergeSerializedHLL", {createAggregateFunctionMergeSerializedHLL, properties});
}

}

#endif
