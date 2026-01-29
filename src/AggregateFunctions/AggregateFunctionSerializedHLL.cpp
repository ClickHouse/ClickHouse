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
#include <Common/FieldVisitorConvertToNumber.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/HLLSketchData.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

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
private:
    uint8_t lg_k;
    datasketches::target_hll_type type;

public:
    AggregationFunctionSerializedHLL(const DataTypes & arguments, const Array & params, uint8_t lg_k_, datasketches::target_hll_type type_)
        : IAggregateFunctionDataHelper<HLLSketchData<T>, AggregationFunctionSerializedHLL<T>>{arguments, params, createResultType()}
        , lg_k(lg_k_)
        , type(type_)
    {}

    AggregationFunctionSerializedHLL()
        : IAggregateFunctionDataHelper<HLLSketchData<T>, AggregationFunctionSerializedHLL<T>>{}
        , lg_k(DEFAULT_LG_K)
        , type(DEFAULT_HLL_TYPE)
    {}

    String getName() const override { return "serializedHLL"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeString>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void create(AggregateDataPtr __restrict place) const override // NOLINT(readability-non-const-parameter)
    {
        new (place) HLLSketchData<T>(lg_k, type);
    }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = *columns[0];
        if constexpr (std::is_same_v<T, String>)
        {
            this->data(place).insertOriginal(column.getDataAt(row_num));
        }
        else
        {
            /// Use the typed value for numeric inputs (better cross-system compatibility than hashing raw bytes).
            this->data(place).insert(assert_cast<const ColumnVector<T> &>(column).getData()[row_num]);
        }
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
    // Parse optional parameters: lg_k and HLL type
    uint8_t lg_k = DEFAULT_LG_K;
    datasketches::target_hll_type type = DEFAULT_HLL_TYPE;

    if (params.size() > 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} takes at most 2 parameters: lg_k (4-21) and type ('HLL_4', 'HLL_6', or 'HLL_8')", name);

    if (!params.empty())
    {
        UInt64 lg_k_param = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);
        if (lg_k_param < 4 || lg_k_param > 21)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Parameter lg_k for aggregate function {} must be between 4 and 21, got {}", name, lg_k_param);
        lg_k = static_cast<uint8_t>(lg_k_param);
    }

    if (params.size() == 2)
    {
        String type_str = params[1].safeGet<String>();
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
    if (which.isNumber())
        return AggregateFunctionPtr(createWithNumericType<AggregationFunctionSerializedHLL>(*data_type, argument_types, params, lg_k, type));
    if (which.isStringOrFixedString())
        return std::make_shared<AggregationFunctionSerializedHLL<String>>(argument_types, params, lg_k, type);
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
/// Syntax: serializedHLL([lg_k, type])(column)
///
/// Arguments:
///   - column: Numeric (Int8/16/32/64, UInt8/16/32/64, Float32/64) or String
///             The values to add to the HLL sketch
///
/// Optional Parameters (passed before column in function call):
///   - lg_k: Integer between 4 and 21 (default: 10)
///           Log-base-2 of K, where K is the number of buckets (K = 2^lg_k)
///           Higher values = better accuracy but more memory
///           - lg_k=10 (K=1024): ~3.2% error, ~512 bytes (HLL_4)
///           - lg_k=12 (K=4096): ~1.6% error, ~2KB (HLL_4)
///           - lg_k=14 (K=16384): ~0.8% error, ~8KB (HLL_4)
///
///   - type: String, one of 'HLL_4', 'HLL_6', or 'HLL_8' (default: 'HLL_4')
///           Storage format:
///           - 'HLL_4': 4 bits/bucket, most compact (~K/2 bytes), slowest
///           - 'HLL_6': 6 bits/bucket, medium (~3K/4 bytes), medium speed
///           - 'HLL_8': 8 bits/bucket, largest (~K bytes), fastest
///           All types produce identical accuracy for same lg_k
///
/// Returns: String
///   A serialized binary HLL sketch. This can be stored in a table, used with mergeSerializedHLL(),
///   or passed to cardinalityFromHLL() to extract the cardinality estimate.
///
/// Examples:
///   -- Default parameters (lg_k=10, type='HLL_4')
///   SELECT serializedHLL(user_id) AS user_sketch FROM events;
///
///   -- Higher accuracy (lg_k=12 for ~1.6% error)
///   SELECT serializedHLL(12)(user_id) AS user_sketch FROM events;
///
///   -- Maximum accuracy with faster updates (lg_k=14, type='HLL_8')
///   SELECT serializedHLL(14, 'HLL_8')(user_id) AS user_sketch FROM events;
///
///   -- Compact storage for very large scale (lg_k=16)
///   SELECT serializedHLL(16, 'HLL_4')(user_id) AS user_sketch FROM events;
///
///   -- Store sketches by date
///   CREATE TABLE daily_sketches (date Date, sketch String) ENGINE = MergeTree() ORDER BY date;
///   INSERT INTO daily_sketches SELECT date, serializedHLL(12)(user_id) FROM events GROUP BY date;
///
/// Performance:
///   - Memory: Varies by lg_k and type (see parameter descriptions)
///   - Accuracy: ~1.04/√K relative error (K = 2^lg_k)
///   - Speed: Very fast, suitable for real-time analytics
///
/// See also:
///   - mergeSerializedHLL() - Merge multiple HLL sketches
///   - cardinalityFromHLL() - Extract cardinality estimate from sketch
///   - uniq() - Direct cardinality estimation (non-serialized)
void registerAggregateFunctionSerializedHLL(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };

    FunctionDocumentation::Description description = R"(
Creates a serialized HyperLogLog (HLL) sketch for approximate cardinality estimation.
)";
    FunctionDocumentation::Syntax syntax = "serializedHLL([lg_k, type])(expression)";
    FunctionDocumentation::Arguments arguments = {
        {"expression", "Column expression.", {"Int*", "UInt*", "Float*", "String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Serialized binary HLL sketch.", {"String"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, {}, introduced_in, category};

    factory.registerFunction("serializedHLL", {createAggregateFunctionSerializedHLL, properties, documentation});
}

}

#endif

