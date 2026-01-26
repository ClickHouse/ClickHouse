#include "config.h"

#if USE_DATASKETCHES

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/castColumn.h>
#include <AggregateFunctions/SketchDataUtils.h>
#include <Functions/DatasketchesIncludes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

class FunctionPercentileFromQuantiles : public IFunction
{
public:
    static constexpr auto name = "percentileFromQuantiles";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPercentileFromQuantiles>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg_string = checkAndGetDataType<DataTypeString>(arguments[0].get());
        if (!arg_string)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, should be String",
                arguments[0]->getName(),
                getName());

        const auto * arg_number = checkAndGetDataType<DataTypeNumber<Float64>>(arguments[1].get());
        if (!arg_number)
        {
            /// Also accept other numeric types and convert them
            if (!isNumber(arguments[1]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of second argument of function {}, should be a number (percentile value between 0 and 1)",
                    arguments[1]->getName(),
                    getName());
        }

        return std::make_shared<DataTypeFloat64>();
    }

    bool useDefaultImplementationForConstants() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// Handle both ColumnString and ColumnConst<ColumnString>
        const ColumnString * col_str = nullptr;
        bool is_sketch_const = false;
        std::string_view const_sketch_data;

        if (const auto * col_const_sketch = checkAndGetColumnConst<ColumnString>(arguments[0].column.get()))
        {
            const_sketch_data = col_const_sketch->getDataAt(0);
            is_sketch_const = true;
        }
        else
        {
            col_str = checkAndGetColumn<ColumnString>(arguments[0].column.get());
            if (!col_str)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal column {} of first argument of function {}, should be ColumnString",
                    arguments[0].column->getName(),
                    getName());
        }

        /// Cast percentile argument to Float64 if needed
        Float64 const_percentile_value = 0.0;
        bool is_percentile_const = false;
        const ColumnFloat64 * col_percentile = nullptr;

        // Check if percentile is a constant
        if (const auto * col_const = checkAndGetColumnConst<ColumnFloat64>(arguments[1].column.get()))
        {
            const_percentile_value = col_const->getValue<Float64>();
            is_percentile_const = true;
        }
        else if (checkAndGetColumn<ColumnFloat64>(arguments[1].column.get()))
        {
            col_percentile = checkAndGetColumn<ColumnFloat64>(arguments[1].column.get());
        }
        else
        {
            // Try to cast the column
            ColumnPtr percentile_column = castColumn(arguments[1], std::make_shared<DataTypeFloat64>());

            if (const auto * col_const_casted = checkAndGetColumnConst<ColumnFloat64>(percentile_column.get()))
            {
                const_percentile_value = col_const_casted->getValue<Float64>();
                is_percentile_const = true;
            }
            else
            {
                col_percentile = checkAndGetColumn<ColumnFloat64>(percentile_column.get());
                if (!col_percentile)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Failed to cast second argument of function {} to Float64",
                        getName());
            }
        }

        /// Validate constant percentile value if applicable
        if (is_percentile_const && (const_percentile_value < 0.0 || const_percentile_value > 1.0))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Percentile value must be between 0 and 1, got {}",
                const_percentile_value);
        }

        auto col_res = ColumnFloat64::create(input_rows_count);
        auto & vec_res = col_res->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view serialized_data = is_sketch_const ? const_sketch_data : col_str->getDataAt(i);
            Float64 percentile = is_percentile_const ? const_percentile_value : col_percentile->getFloat64(i);

            /// Validate percentile range (skip if already validated constant)
            if (!is_percentile_const && (percentile < 0.0 || percentile > 1.0))
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Percentile value must be between 0 and 1, got {}",
                    percentile);
            }

            if (serialized_data.empty())
            {
                vec_res[i] = std::numeric_limits<Float64>::quiet_NaN();
                continue;
            }

            try
            {
                /// ClickHouse aggregate functions (serializedQuantiles, mergeSerializedQuantiles)
                /// always return raw binary data, never base64. Skip base64 detection for performance.
                /// If users need to decode base64 sketch data from external sources, they should
                /// use base64Decode() explicitly before calling this function.
                std::string decoded_storage;
                auto [data_ptr, data_size] = decodeSketchData(serialized_data, decoded_storage, /* base64_encoded= */ false);

                if (data_ptr == nullptr || data_size == 0)
                {
                    vec_res[i] = std::numeric_limits<Float64>::quiet_NaN();
                    continue;
                }

                auto sketch = datasketches::quantiles_sketch<double>::deserialize(data_ptr, data_size);

                if (sketch.is_empty())
                {
                    vec_res[i] = std::numeric_limits<Float64>::quiet_NaN();
                }
                else
                {
                    vec_res[i] = sketch.get_quantile(percentile);
                }
            }
            catch (...)
            {
                /// If deserialization fails, return NaN
                vec_res[i] = std::numeric_limits<Float64>::quiet_NaN();
            }
        }

        return col_res;
    }
};

}

REGISTER_FUNCTION(PercentileFromQuantiles)
{
    FunctionDocumentation::Description description = R"(
Extracts the percentile value from a serialized Quantiles sketch.
The function deserializes the sketch and returns the value at the specified percentile.
If the input is invalid or empty, returns NaN.
)";
    FunctionDocumentation::Syntax syntax = "percentileFromQuantiles(serialized_sketch, percentile)";
    FunctionDocumentation::Arguments arguments = {
        {"serialized_sketch", "Serialized Quantiles sketch as a String.", {"String"}},
        {"percentile", "Percentile value between 0.0 and 1.0 (e.g., 0.5 for median, 0.95 for 95th percentile).", {"Float64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the value at the specified percentile from the sketch. Returns NaN if the sketch is empty or invalid.",
        {"Float64"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Get median (50th percentile)",
            R"(
SELECT percentileFromQuantiles(serializedQuantiles(rand()), 0.5) AS median
FROM numbers(1000)
            )",
            ""
        },
        {
            "Get 95th percentile",
            R"(
SELECT percentileFromQuantiles(serializedQuantiles(rand()), 0.95) AS p95
FROM numbers(1000)
            )",
            ""
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = FunctionDocumentation::VERSION_UNKNOWN;
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPercentileFromQuantiles>(documentation);
}

}

#endif
