#include <limits>
#include <algorithm>
#include <vector>
#include <cmath>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnConst.h>
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

class FunctionArrayPartialAutocorrelation : public IFunction
{
public:
    static constexpr auto name = "arrayPartialAutocorrelation";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayPartialAutocorrelation>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

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

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
    }

    // Internal helper function to calculate autocorrelation
    template <typename Element>
    static bool calculateACF(const Element * __restrict src, size_t size, size_t max_lag, std::vector<Float64> & acf_out)
    {
        // Calculate mean
        Float64 sum = 0.0;
        for (size_t i = 0; i < size; ++i)
            sum += static_cast<Float64>(src[i]);

        Float64 mean = sum / static_cast<Float64>(size);

        // Calculate variance
        Float64 denominator = 0.0;
        for (size_t i = 0; i < size; ++i)
        {
            Float64 diff = static_cast<Float64>(src[i]) - mean;
            denominator += diff * diff;
        }

        if (denominator == 0.0)
            return false;

        acf_out.resize(max_lag + 1);

        for (size_t lag = 0; lag <= max_lag; ++lag)
        {
            Float64 numerator = 0.0;
            for (size_t t = 0; t < size - lag; ++t)
            {
                Float64 val_t = static_cast<Float64>(src[t]);
                Float64 val_lag = static_cast<Float64>(src[t + lag]);
                numerator += (val_t - mean) * (val_lag - mean);
            }
            acf_out[lag] = numerator / denominator;
        }
        return true;
    }

    template <typename Element>
    static void impl(const Element * __restrict src, size_t size, size_t max_lag, PaddedPODArray<Float64> & res_values)
    {
        if (size < 2)
        {
            if (size > 0) res_values.push_back(std::numeric_limits<Float64>::quiet_NaN());
            return;
        }

        size_t limit = std::min(size - 1, max_lag);

        // Calculate ACF
        std::vector<Float64> acf;
        if (!calculateACF(src, size, limit, acf))
        {
            // Variance was 0, return NaNs
            for (size_t i = 0; i <= limit; ++i)
                res_values.push_back(std::numeric_limits<Float64>::quiet_NaN());
            return;
        }

        // Levinson-Durbin Recursion
        std::vector<Float64> phi(limit + 1, 0.0);
        std::vector<Float64> prev_phi(limit + 1, 0.0);

        // PACF at lag 0 is always 1.0
        res_values.push_back(1.0);

        // Initial error variance (sigma^2)
        Float64 sigma_sq = 1.0; 

        for (size_t k = 1; k <= limit; ++k)
        {
            // Calculate numerator: acf[k] - sum(prev_phi[j] * acf[k-j])
            Float64 numerator = acf[k];
            for (size_t j = 1; j < k; ++j)
            {
                numerator -= prev_phi[j] * acf[k - j];
            }

            // Reflection coefficient; this is the PACF value at lag k
            Float64 reflection_coeff = numerator / sigma_sq;
            res_values.push_back(reflection_coeff);

            // Update AR coefficients for next step
            phi[k] = reflection_coeff;
            for (size_t j = 1; j < k; ++j)
            {
                phi[j] = prev_phi[j] - reflection_coeff * prev_phi[k - j];
            }

            // Update prediction error variance
            sigma_sq *= (1.0 - reflection_coeff * reflection_coeff);

            // Swap buffers; prev_phi becomes the current phi for next iteration
            std::copy(phi.begin() + 1, phi.begin() + k + 1, prev_phi.begin() + 1);
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

        bool is_max_lag_const = false;
        Int64 const_max_lag_val = 0;
        if (col_max_lag && isColumnConst(*col_max_lag))
        {
            is_max_lag_const = true;
            const_max_lag_val = col_max_lag->getInt(0);
        }

        size_t current_offset = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t next_offset = offsets[i];
            size_t array_size = next_offset - current_offset;

            size_t max_lag = (array_size > 0) ? array_size - 1 : 0;
            
            if (col_max_lag)
            {
                Int64 lag_arg;
                if (is_max_lag_const)
                    lag_arg = const_max_lag_val;
                else
                    lag_arg = col_max_lag->getInt(i);

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
                "arrayPartialAutocorrelation only accepts arrays of standard integers or floating-point numbers. "
                "Unsupported type: {}", nested_column.getFamilyName());
    }
};

REGISTER_FUNCTION(ArrayPartialAutocorrelation)
{
    FunctionDocumentation::Description description = R"(
Calculates the Partial Autocorrelation Function (PACF) of an array.
Uses the Levinson-Durbin recursion.
If `max_lag` is provided, calculates correlation only for lags in range `[0, max_lag]`.
    )";
    FunctionDocumentation::Syntax syntax = "arrayPartialAutocorrelation(arr, [max_lag])";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "Array of numbers.", {"Array(T)"}},
        {"max_lag", "Optional. Maximum lag to calculate.", {"Integer"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of Float64. Returns NaN if variance is 0.", {"Array(Float64)"}};
    FunctionDocumentation::Examples examples = {
        {"Linear", "SELECT arrayPartialAutocorrelation([1, 2, 3, 4, 5]);", "[1, 0.4, -0.076923, -0.01923, -0.00769]"},
        {"Constant", "SELECT arrayPartialAutocorrelation([5, 5, 5]);", "[nan, nan, nan]"}
    };

    FunctionDocumentation::IntroducedIn introduced_in = {26, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    
    factory.registerFunction<FunctionArrayPartialAutocorrelation>(documentation);
}

}
