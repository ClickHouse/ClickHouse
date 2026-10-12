#include <AggregateFunctions/AggregateFunctionUniqApacheHLL.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Common/FieldVisitorConvertToNumber.h>

#if USE_DATASKETCHES

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

static AggregateFunctionPtr createAggregateFunctionUniqApacheHLL(
    const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    uint8_t lg_config_k = 12;
    datasketches::target_hll_type target_type = datasketches::HLL_4;
    bool use_composite_estimate = false;

    if (params.size() > 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} accepts at most three parameters (lg_k, type, estimator).", name);

    if (!params.empty())
    {
        const UInt64 lg_k_param = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);
        if (lg_k_param < 4 || lg_k_param > 21)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "Parameter lg_k for aggregate function {} is out of range: [4, 21].", name);
        lg_config_k = static_cast<uint8_t>(lg_k_param);
    }

    if (params.size() >= 2)
    {
        if (params[1].getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Parameter type for aggregate function {} must be a string.", name);

        const String type_param = params[1].safeGet<String>();
        if (type_param == "HLL_4")
            target_type = datasketches::HLL_4;
        else if (type_param == "HLL_6")
            target_type = datasketches::HLL_6;
        else if (type_param == "HLL_8")
            target_type = datasketches::HLL_8;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Parameter type for aggregate function {} must be one of 'HLL_4', 'HLL_6', 'HLL_8'.", name);
    }

    if (params.size() == 3)
    {
        if (params[2].getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Parameter estimator for aggregate function {} must be a string.", name);

        const String estimator_param = params[2].safeGet<String>();
        if (estimator_param == "DEFAULT")
            use_composite_estimate = false;
        else if (estimator_param == "COMPOSITE")
            use_composite_estimate = true;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Parameter estimator for aggregate function {} must be one of 'DEFAULT', 'COMPOSITE'.", name);
    }

    assertUnary(name, argument_types);

    const IDataType & argument_type = *argument_types[0];
    WhichDataType which(argument_type);

    /// Unlike other decimals, `DateTime64` has an interoperable representation as epoch ticks.
    if (which.isDateTime64())
        return std::make_shared<AggregateFunctionUniqApacheHLL<DataTypeDateTime64::FieldType>>(lg_config_k, target_type, use_composite_estimate, argument_types, params);

    /// Exclude wide integers: DataSketches has no portable representation for them.
    if (!which.isInt128() && !which.isInt256() && !which.isUInt128() && !which.isUInt256())
    {
        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionUniqApacheHLL>(
            argument_type, lg_config_k, target_type, use_composite_estimate, argument_types, params));
        if (res)
            return res;
    }

    if (which.isDate())
        return std::make_shared<AggregateFunctionUniqApacheHLL<DataTypeDate::FieldType>>(lg_config_k, target_type, use_composite_estimate, argument_types, params);
    if (which.isDate32())
        return std::make_shared<AggregateFunctionUniqApacheHLL<DataTypeDate32::FieldType>>(lg_config_k, target_type, use_composite_estimate, argument_types, params);
    if (which.isDateTime())
        return std::make_shared<AggregateFunctionUniqApacheHLL<DataTypeDateTime::FieldType>>(lg_config_k, target_type, use_composite_estimate, argument_types, params);
    if (which.isStringOrFixedString())
        return std::make_shared<AggregateFunctionUniqApacheHLL<String>>(lg_config_k, target_type, use_composite_estimate, argument_types, params);
    if (which.isUUID())
        return std::make_shared<AggregateFunctionUniqApacheHLL<DataTypeUUID::FieldType>>(lg_config_k, target_type, use_composite_estimate, argument_types, params);
    if (which.isIPv4())
        return std::make_shared<AggregateFunctionUniqApacheHLL<DataTypeIPv4::FieldType>>(lg_config_k, target_type, use_composite_estimate, argument_types, params);
    if (which.isIPv6())
        return std::make_shared<AggregateFunctionUniqApacheHLL<DataTypeIPv6::FieldType>>(lg_config_k, target_type, use_composite_estimate, argument_types, params);

    /// For `Nullable(Nothing)` the `Null` combinator replaces this function with `nothing`, but it must be created first.
    if (argument_type.onlyNull())
        return std::make_shared<AggregateFunctionUniqApacheHLL<UInt8>>(lg_config_k, target_type, use_composite_estimate, argument_types, params);

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Illegal type {} of argument for aggregate function {}. Use uniq, uniqCombined or uniqHLL12 for unsupported types.",
        argument_type.getName(), name);
}

void registerAggregateFunctionUniqApacheHLL(AggregateFunctionFactory & factory);
void registerAggregateFunctionUniqApacheHLL(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Calculates the approximate number of different argument values using an [Apache DataSketches](https://datasketches.apache.org/docs/HLL/HllSketches.html) HyperLogLog sketch.

The `-State` and `-Merge` combinators exchange Apache DataSketches HLL sketches with a varint length prefix.

For interoperability, integers of at most 64 bits are hashed as 8-byte integers, floating-point values as IEEE-754 doubles, strings as raw bytes, `UUID` values as canonical 16 bytes, and `IPv6` addresses in network order.
`Date`, `Date32`, `DateTime` and `DateTime64` use their underlying integer values. External producers must use the same unit; for example, `DateTime64(3)` uses epoch milliseconds.
An unsigned `UInt8`, `UInt16` or `UInt32` value is hashed as the same number as a 64-bit integer, as Java and Spark hash a number that they hold as a `long`.
The C++ library hashes `uint8_t`, `uint16_t` and `uint32_t` through the signed type of the same width, so for the values from 128, 32768 and 2147483648 respectively (and for `Date` from 32768, `DateTime` from 2147483648, and `IPv4` from 128.0.0.0) its sketch is another one.
To get the sketch of the C++ library, cast the argument to the signed type of the same width with a cast that wraps: `toInt8(x)`, `toInt16(x)` or `toInt32(x)`; for `Date` use `toInt16(toUInt16(x))`, for `DateTime` use `toInt32(x)`, and for `IPv4` use `toInt32(toUInt32(x))`.
Do not use `accurateCast`, which rejects these values.
Unsupported types and multiple arguments are rejected. Use `uniq`, `uniqCombined` or `uniqHLL12` for those inputs.

`NULL` values and empty strings are ignored, as in the Java, Python and C++ implementations and in Spark; unlike `uniq`, an empty string is not counted as a value.
A `Nullable` argument gives the same state as a non-`Nullable` one, a bare DataSketches sketch, and the same state type, `AggregateFunction(uniqApacheHLL, T)`.

Merging can switch from the HIP estimator to the less accurate composite estimator, and the HIP estimate of a union of small sketches depends on the order of the merges, so with the default estimator results can depend on partitioning across threads, parts and shards.
The `estimator` parameter `'COMPOSITE'` makes the result depend only on the registers of the sketch and not on the order of the inserts and of the merges, at the cost of a lower accuracy for sketches of a few hundred distinct values. It does not change the state.
Merging a lower-resolution sketch permanently lowers the result's resolution, regardless of the declared `lg_k`.
    )";
    FunctionDocumentation::Syntax syntax = "uniqApacheHLL([lg_k, [type, [estimator]]])(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Column to compute the number of distinct values of.", {"(U)Int8/16/32/64", "Enum", "BFloat16", "Float32", "Float64", "String", "FixedString", "UUID", "IPv4", "IPv6", "Date", "Date32", "DateTime", "DateTime64"}},
    };
    FunctionDocumentation::Parameters parameters = {
        {"lg_k", "Optional. Log-base-2 of the number of buckets, in range [4, 21]. Higher means better accuracy and more memory. Default: 12.", {"UInt8"}},
        {"type", "Optional. Storage format of the sketch: 'HLL_4', 'HLL_6', or 'HLL_8'. Default: 'HLL_4'.", {"String"}},
        {"estimator", "Optional. The estimator used for the result: 'DEFAULT' (the estimator of the library, which can depend on the order of the inserts and of the merges) or 'COMPOSITE' (depends only on the registers of the sketch). Default: 'DEFAULT'.", {"String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the approximate number of distinct values.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT uniqApacheHLL(number) FROM numbers(1000)", "1000"},
        {"With parameters", "SELECT uniqApacheHLL(14, 'HLL_8')(number) FROM numbers(1000)", "1000"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    /// The HIP estimate and the bytes of the sketch depend on the order of the updates and of the merges, so the planner must not
    /// remove an `ORDER BY` below this function.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = true };

    factory.registerFunction("uniqApacheHLL", {createAggregateFunctionUniqApacheHLL, documentation, properties});
}

}

#endif
