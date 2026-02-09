#include <limits>
#include <algorithm>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNothing.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNothing.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/NaNUtils.h>
#include <Columns/IColumn.h>
#include <base/types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

class FunctionArrayAutocorrelation : public IFunction
{
public:
    static constexpr auto name = "arrayAutocorrelation";

    static FunctionPtr create(ContextPtr) {return std::make_shared<FunctionArrayAutocorrelation>();}

    String getName() const override {return name;}

    bool isVariadic() const override {return true;}
    size_t getNumberOfArguments() const override {return 0;}

    bool useDefaultImplementationForConstants() const override {return true;}
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} requires 1 or 2 arguments.", getName());

        const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be an array.", getName());

        const auto & nested_type = array_type->getNestedType();

        if (!isNumber(nested_type) && !typeid_cast<const DataTypeNothing *>(nested_type.get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} only accepts arrays of numbers.", getName());

        if (arguments.size() == 2)
        {
            if (!isInteger(arguments[1]))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Second argument (max_lag) of function {} must be an integer.", getName());
        }

        // Always return Float64
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
    }

    template <typename Element>
    static void impl(const Element * __restrict src, size_t size, size_t max_lag, PaddedPODArray<Float64> & res_values)
    {
        if (size == 0)
        {
            return;
        }

        if (size < 2)
        {
            res_values.push_back(std::numeric_limits<Float64>::quiet_NaN());
            return;
        }

        // Calculate Mean
        Float64 sum = 0.0;
        for (size_t i = 0; i < size; ++i)
            sum += static_cast<Float64>(src[i]);

        Float64 mean = sum / static_cast<Float64>(size);

        // Calculate Variance
        Float64 denominator = 0.0;
        for (size_t i = 0; i < size; ++i)
        {
            Float64 diff = static_cast<Float64>(src[i]) - mean;
            denominator += diff * diff;
        }

        //Apply the max_lag limit
        size_t limit = std::min(size, max_lag);

        if (denominator == 0.0)
        {
            for (size_t i = 0; i < limit; ++i)
            {
                res_values.push_back(std::numeric_limits<Float64>::quiet_NaN());
            }
            return;
        }

        // Calculate Correlation for every Lag up to limit
        for (size_t lag = 0; lag < limit; ++lag)
        {
            Float64 numerator = 0.0;
            for (size_t t = 0; t < size - lag; ++t)
            {
                Float64 val_t = static_cast<Float64>(src[t]);
                Float64 val_lag = static_cast<Float64>(src[t + lag]);
                numerator += (val_t - mean) * (val_lag - mean);
            }
            res_values.push_back(numerator / denominator);
        }
    }

    template <typename Element>
    ColumnPtr executeType(const ColumnArray & col_array, const IColumn * col_max_lag) const
    {
        const IColumn & col_data_raw = col_array.getData();
        const auto * col_data_specific = checkAndGetColumn<ColumnVector<Element>>(&col_data_raw);
        const PaddedPODArray<Element> & data = col_data_specific->getData();
        const ColumnArray::Offsets & offsets = col_array.getOffsets();

        auto res_data_col = ColumnFloat64::create();
        auto & res_data = res_data_col->getData();
        auto res_offsets_col = ColumnArray::ColumnOffsets::create();
        auto & res_offsets = res_offsets_col->getData();

        res_data.reserve(data.size());
        res_offsets.reserve(offsets.size());

        size_t current_offset = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t next_offset = offsets[i];
            size_t array_size = next_offset - current_offset;

            // Determine max_lag for this row
            size_t max_lag = array_size;
            if (col_max_lag)
            {
                Int64 lag_arg = col_max_lag->getInt(i);
                lag_arg = std::max<Int64>(lag_arg, 0);
                max_lag = static_cast<size_t>(lag_arg);
            }

            impl(data.data() + current_offset, array_size, max_lag, res_data);

            res_offsets.push_back(res_data.size());
            current_offset = next_offset;
        }

        return ColumnArray::create(std::move(res_data_col), std::move(res_offsets_col));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!col_array) throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument must be an array");

        const IColumn * col_max_lag = nullptr;
        if (arguments.size() > 1)
            col_max_lag = arguments[1].column.get();

        const IColumn & nested_column = col_array->getData();

        if (checkColumn<ColumnNothing>(nested_column))
        {
            return ColumnArray::create(ColumnFloat64::create(), col_array->getOffsetsPtr());
        }

        // Dispatch based on nested element type
        if      (checkColumn<ColumnVector<UInt8>>(nested_column))    return executeType<UInt8>(*col_array, col_max_lag);
        else if (checkColumn<ColumnVector<UInt16>>(nested_column))   return executeType<UInt16>(*col_array, col_max_lag);
        else if (checkColumn<ColumnVector<UInt32>>(nested_column))   return executeType<UInt32>(*col_array, col_max_lag);
        else if (checkColumn<ColumnVector<UInt64>>(nested_column))   return executeType<UInt64>(*col_array, col_max_lag);
        else if (checkColumn<ColumnVector<UInt128>>(nested_column))  return executeType<UInt128>(*col_array, col_max_lag);
        else if (checkColumn<ColumnVector<UInt256>>(nested_column))  return executeType<UInt256>(*col_array, col_max_lag);
        else if (checkColumn<ColumnVector<Int8>>(nested_column))     return executeType<Int8>(*col_array, col_max_lag);
        else if (checkColumn<ColumnVector<Int16>>(nested_column))    return executeType<Int16>(*col_array, col_max_lag);
        else if (checkColumn<ColumnVector<Int32>>(nested_column))    return executeType<Int32>(*col_array, col_max_lag);
        else if (checkColumn<ColumnVector<Int64>>(nested_column))    return executeType<Int64>(*col_array, col_max_lag);
        else if (checkColumn<ColumnVector<Int128>>(nested_column))   return executeType<Int128>(*col_array, col_max_lag);
        else if (checkColumn<ColumnVector<Int256>>(nested_column))   return executeType<Int256>(*col_array, col_max_lag);
        else if (checkColumn<ColumnVector<Float32>>(nested_column))  return executeType<Float32>(*col_array, col_max_lag);
        else if (checkColumn<ColumnVector<Float64>>(nested_column))  return executeType<Float64>(*col_array, col_max_lag);
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "arrayAutocorrelation only accepts arrays of standard integers or floating-point numbers (UInt*, Int*, Float*). "
                "For Decimal types, please cast to Float64. Unsupported type: {}", nested_column.getFamilyName());
    }
};

REGISTER_FUNCTION(ArrayAutocorrelation)
{
    FunctionDocumentation::Description description = R"(
Calculates the autocorrelation of an array.
If `max_lag` is provided, calculates correlation only for lags in range `[0, max_lag)`.
If `max_lag` is not provided, calculates for all possible lags.
    )";
    FunctionDocumentation::Syntax syntax = "arrayAutocorrelation(arr, [max_lag])";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "Array of numbers.", {"Array(T)"}},
        {"max_lag", "Optional. Maximum lag to calculate.", {"Integer"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of Float64. Returns NaN if variance is 0.", {"Array(Float64)"}};
    FunctionDocumentation::Examples examples = {
        {"Linear", "SELECT arrayAutocorrelation([1, 2, 3, 4, 5]);", "[1, 0.4, -0.1, -0.4, -0.4]"},
        {"Symmetric", "SELECT arrayAutocorrelation([10, 20, 10]);", "[1, -0.6666666666666669, 0.16666666666666674]"},
        {"Constant", "SELECT arrayAutocorrelation([5, 5, 5]);", "[nan, nan, nan]"},
        {"Limited", "SELECT arrayAutocorrelation([1, 2, 3, 4, 5], 2);", "[1, 0.4]"}
    };

    FunctionDocumentation::IntroducedIn introduced_in = {26, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;

    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayAutocorrelation>(documentation);
}

}
