#include "config.h"

#if USE_DATASKETCHES

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <AggregateFunctions/SketchDataUtils.h>
#include <Functions/DatasketchesIncludes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class FunctionCardinalityFromHLL : public IFunction
{
public:
    static constexpr auto name = "cardinalityFromHLL";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionCardinalityFromHLL>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg_string = checkAndGetDataType<DataTypeString>(arguments[0].get());
        if (!arg_string)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}, should be String",
                arguments[0]->getName(),
                getName());

        return std::make_shared<DataTypeUInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// Fast path for empty blocks (common in parallel pipeline execution after aggregation)
        if (input_rows_count == 0)
            return ColumnUInt64::create();

        const auto * col_str = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!col_str)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal column {} of argument of function {}, should be ColumnString",
                arguments[0].column->getName(),
                getName());

        auto col_res = ColumnUInt64::create(input_rows_count);
        auto & vec_res = col_res->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view serialized_data = col_str->getDataAt(i);

            if (serialized_data.empty())
            {
                vec_res[i] = 0;
                continue;
            }

            try
            {
                /// ClickHouse aggregate functions (serializedHLL, mergeSerializedHLL) always
                /// return raw binary data, never base64. Skip base64 detection for performance.
                /// If users need to decode base64 sketch data from external sources, they should
                /// use base64Decode() explicitly before calling this function.
                std::string decoded_storage;
                auto [data_ptr, data_size] = decodeSketchData(serialized_data, decoded_storage, /* base64_encoded= */ false);

                if (data_ptr == nullptr || data_size == 0)
                {
                    vec_res[i] = 0;
                    continue;
                }

                auto sketch = datasketches::hll_sketch::deserialize(data_ptr, data_size);
                vec_res[i] = static_cast<UInt64>(sketch.get_estimate());
            }
            catch (...)
            {
                /// If deserialization fails, return 0
                vec_res[i] = 0;
            }
        }

        return col_res;
    }
};

}

REGISTER_FUNCTION(CardinalityFromHLL)
{
    FunctionDocumentation::Description description = R"(
Extracts the cardinality estimate from a serialized HLL (HyperLogLog) sketch.
The function deserializes the HLL sketch and returns the estimated number of unique elements.
If the input is invalid or empty, returns 0.
)";
    FunctionDocumentation::Syntax syntax = "cardinalityFromHLL(serialized_hll)";
    FunctionDocumentation::Arguments arguments = {
        {"serialized_hll", "Serialized HLL sketch as a String.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the estimated cardinality (number of unique elements) from the HLL sketch.",
        {"UInt64"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Basic usage",
            R"(
SELECT cardinalityFromHLL(serializedHLL(number)) AS cardinality
FROM numbers(1000)
            )",
            R"(
┌─cardinality─┐
│        1000 │
└─────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = FunctionDocumentation::VERSION_UNKNOWN;
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionCardinalityFromHLL>(documentation);
}

}

#endif
