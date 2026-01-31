#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/NaNUtils.h>

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
    size_t getNumberOfArguments() const override {return 1;}
    bool isVariadic() const override {return false;}
    bool useDefaultImplementationForConstants() const override {return true;}
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument must be an array.");
        }

        // Always return Float64
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
    }

    template <typename Element>
    static void impl(const Element * __restrict src, size_t size, PaddedPODArray<Float64> & res_values)
    {
        // Array too small
        if (size < 2)
        {
            res_values.push_back(1.0);
            return;
        }

        // Calculate Mean
        Float64 sum = 0.0;
        for (size_t i = 0; i < size; ++i)
            sum += static_cast<Float64>(src[i]);

        Float64 mean = sum / size;

        // Calculate Variance
        Float64 denominator = 0.0;
        for (size_t i = 0; i < size; ++i)
        {
            Float64 diff = static_cast<Float64>(src[i]) - mean;
            denominator += diff * diff;
        }

        // Calculate Correlation for every Lag
        for (size_t lag = 0; lag < size; ++lag)
        {
            Float64 numerator = 0.0;
            for (size_t t = 0; t < size - lag; ++t)
            {
                Float64 val_t = static_cast<Float64>(src[t]);
                Float64 val_lag = static_cast<Float64>(src[t + lag]);
                numerator += (val_t - mean) * (val_lag - mean);
            }

            if (denominator != 0)
                res_values.push_back(numerator / denominator);
            else
                res_values.push_back(0.0);
        }
    }

    template <typename Element>
    ColumnPtr executeType(const ColumnArray & col_array) const
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
        for (const auto next_offset : offsets)
        {
            size_t array_size = next_offset - current_offset;

            impl(data.data() + current_offset, array_size, res_data);

            res_offsets.push_back(res_data.size());
            current_offset = next_offset;
        }

        return ColumnArray::create(std::move(res_data_col), std::move(res_offsets_col));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!col_array) throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument must be an array");

        const IColumn & nested_column = col_array->getData();

        // Dispatch and RETURN the result directly
        if      (checkColumn<ColumnVector<UInt8>>(nested_column))    return executeType<UInt8>(*col_array);
        else if (checkColumn<ColumnVector<UInt16>>(nested_column))   return executeType<UInt16>(*col_array);
        else if (checkColumn<ColumnVector<UInt32>>(nested_column))   return executeType<UInt32>(*col_array);
        else if (checkColumn<ColumnVector<UInt64>>(nested_column))   return executeType<UInt64>(*col_array);
        else if (checkColumn<ColumnVector<Int8>>(nested_column))     return executeType<Int8>(*col_array);
        else if (checkColumn<ColumnVector<Int16>>(nested_column))    return executeType<Int16>(*col_array);
        else if (checkColumn<ColumnVector<Int32>>(nested_column))    return executeType<Int32>(*col_array);
        else if (checkColumn<ColumnVector<Int64>>(nested_column))    return executeType<Int64>(*col_array);
        else if (checkColumn<ColumnVector<Float32>>(nested_column))  return executeType<Float32>(*col_array);
        else if (checkColumn<ColumnVector<Float64>>(nested_column))  return executeType<Float64>(*col_array);
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "arrayAutocorrelation only accepts arrays of numbers.");
    }
};

REGISTER_FUNCTION(ArrayAutocorrelation)
{
    FunctionDocumentation::Description description = R"(
Calculates the autocorrelation of an array at all lags.
The result is an array of Float64 where the i-th element is the correlation coefficient at lag i.
    )";
    FunctionDocumentation::Syntax syntax = "arrayAutocorrelation(arr)";
    FunctionDocumentation::Arguments argument = {
        {"arr", "Array of numbers (integers or floats).", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of Float64 values between -1 and 1.", {"Array(Float64)"}};
    FunctionDocumentation::Examples examples = {
        {"Linear", "SELECT arrayAutocorrelation([1, 2, 3, 4, 5]);", "[1, 0.4, -0.1, -0.4, -0.4]"},
        {"Symmetric", "SELECT arrayAutocorrelation([10, 20, 10]);", "[1, 0.5, -1]"}
    };

    FunctionDocumentation::IntroducedIn introduced_in = {26, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;

    FunctionDocumentation documentation = {description, syntax, argument, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayAutocorrelation>(documentation);
}

}
