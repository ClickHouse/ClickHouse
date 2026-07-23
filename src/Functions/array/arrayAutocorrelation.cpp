#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionArrayAutocorrelation : public IFunction
{
public:
    static constexpr auto name = "arrayAutocorrelation";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayAutocorrelation>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires 1 or 2 arguments.", getName());

        const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must be an array.", getName());

        const auto & nested_type = array_type->getNestedType();

        if (!isInteger(nested_type) && !isNativeFloat(nested_type) && !isDecimal(nested_type)
            && !typeid_cast<const DataTypeNothing *>(nested_type.get()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} only accepts arrays of integers, floating-point numbers, or decimals.",
                getName());

        if (arguments.size() == 2)
        {
            if (!isNativeInteger(arguments[1]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Second argument (max_lag) of function {} must be an integer (up to 64-bit), got {}.",
                    getName(),
                    arguments[1]->getName());
        }

        /// Always return Array(Float64)
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
    }

    static constexpr size_t MAX_AUTOCORRELATION_OPERATIONS = 100'000'000;

    /// Validate that a signed max_lag column contains no negative values.
    template <typename T>
    static void validateMaxLagTyped(const IColumn * col_max_lag, size_t rows)
    {
        const auto & data = assert_cast<const ColumnVector<T> &>(*col_max_lag).getData();
        for (size_t i = 0; i < rows; ++i)
        {
            if (data[i] < 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: max_lag must be non-negative", name);
        }
    }

    static void validateMaxLag(const IColumn * col_max_lag, size_t rows)
    {
        /// For ColumnConst, unwrap and validate the single inner value.
        if (const auto * col_const = typeid_cast<const ColumnConst *>(col_max_lag))
        {
            validateMaxLag(&col_const->getDataColumn(), 1);
            return;
        }

        if (checkColumn<ColumnVector<Int8>>(*col_max_lag))
            validateMaxLagTyped<Int8>(col_max_lag, rows);
        else if (checkColumn<ColumnVector<Int16>>(*col_max_lag))
            validateMaxLagTyped<Int16>(col_max_lag, rows);
        else if (checkColumn<ColumnVector<Int32>>(*col_max_lag))
            validateMaxLagTyped<Int32>(col_max_lag, rows);
        else if (checkColumn<ColumnVector<Int64>>(*col_max_lag))
            validateMaxLagTyped<Int64>(col_max_lag, rows);
        else
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument (max_lag) of function {} has unsupported column type: {}",
                name,
                col_max_lag->getFamilyName());
    }

    template <typename Element>
    static void impl(const Element * __restrict src, size_t size, size_t max_lag, PaddedPODArray<Float64> & res_values)
    {
        if (size == 0)
            return;

        size_t limit = std::min(size, max_lag);

        if (limit == 0)
            return;

        bool all_equal = true;
        for (size_t i = 1; i < size; ++i)
        {
            if (src[i] != src[0])
            {
                all_equal = false;
                break;
            }
        }

        if (all_equal)
        {
            for (size_t i = 0; i < limit; ++i)
                res_values.push_back(std::numeric_limits<Float64>::quiet_NaN());
            return;
        }

        /// Calculate mean
        Float64 sum = 0.0;
        for (size_t i = 0; i < size; ++i)
            sum += static_cast<Float64>(src[i]);

        Float64 mean = sum / static_cast<Float64>(size);

        /// Calculate variance (denominator for normalization)
        Float64 denominator = 0.0;
        for (size_t i = 0; i < size; ++i)
        {
            Float64 diff = static_cast<Float64>(src[i]) - mean;
            denominator += diff * diff;
        }

        if (denominator == 0.0)
        {
            for (size_t i = 0; i < limit; ++i)
                res_values.push_back(std::numeric_limits<Float64>::quiet_NaN());
            return;
        }

        /// Guard against O(n * limit) computation for large inputs.
        if (size > MAX_AUTOCORRELATION_OPERATIONS / limit)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "{}: estimated computation ({} * {}) exceeds the safety limit of {}. "
                "Use the max_lag argument to reduce the number of lags.",
                name,
                size,
                limit,
                MAX_AUTOCORRELATION_OPERATIONS);

        /// Calculate autocorrelation for each lag
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
    ColumnPtr executeWithType(const ColumnArray & col_array, const IColumn * col_max_lag) const
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

            size_t max_lag = col_max_lag ? static_cast<size_t>(col_max_lag->getUInt(i)) : array_size;

            impl(data.data() + current_offset, array_size, max_lag, res_data);

            res_offsets.push_back(res_data.size());
            current_offset = next_offset;
        }

        return ColumnArray::create(std::move(res_data_col), std::move(res_offsets_col));
    }

    /// Convert Decimal array to Float64 using the column's scale, then delegate to executeWithType<Float64>.
    template <typename DecimalType>
    ColumnPtr executeWithDecimalType(const ColumnArray & col_array, const IColumn * col_max_lag) const
    {
        const auto & col_data = assert_cast<const ColumnDecimal<DecimalType> &>(col_array.getData());
        const auto & data = col_data.getData();
        UInt32 scale = col_data.getScale();

        Float64 scale_factor = static_cast<Float64>(DecimalUtils::scaleMultiplier<typename DecimalType::NativeType>(scale));
        auto float_col = ColumnFloat64::create(data.size());
        auto & float_data = float_col->getData();
        for (size_t i = 0; i < data.size(); ++i)
            float_data[i] = static_cast<Float64>(data[i].value) / scale_factor;

        auto converted_array = ColumnArray::create(std::move(float_col), col_array.getOffsetsPtr());
        return executeWithType<Float64>(*converted_array, col_max_lag);
    }

    /// Const array path: operate on the single array for each row without materializing.
    template <typename Element>
    ColumnPtr executeWithConstArray(const ColumnArray & single_array, const IColumn * col_max_lag, size_t input_rows_count) const
    {
        const auto & data = assert_cast<const ColumnVector<Element> &>(single_array.getData()).getData();
        size_t array_size = data.size();

        auto res_data_col = ColumnFloat64::create();
        auto & res_data = res_data_col->getData();
        auto res_offsets_col = ColumnArray::ColumnOffsets::create();
        auto & res_offsets = res_offsets_col->getData();

        res_offsets.reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t max_lag = col_max_lag ? static_cast<size_t>(col_max_lag->getUInt(i)) : array_size;

            impl(data.data(), array_size, max_lag, res_data);
            res_offsets.push_back(res_data.size());
        }

        return ColumnArray::create(std::move(res_data_col), std::move(res_offsets_col));
    }

    template <typename DecimalType>
    ColumnPtr executeWithConstDecimalArray(const ColumnArray & single_array, const IColumn * col_max_lag, size_t input_rows_count) const
    {
        const auto & col_data = assert_cast<const ColumnDecimal<DecimalType> &>(single_array.getData());
        const auto & data = col_data.getData();
        UInt32 scale = col_data.getScale();
        size_t array_size = data.size();

        Float64 scale_factor = static_cast<Float64>(DecimalUtils::scaleMultiplier<typename DecimalType::NativeType>(scale));
        auto float_col = ColumnFloat64::create(array_size);
        auto & float_data = float_col->getData();
        for (size_t i = 0; i < array_size; ++i)
            float_data[i] = static_cast<Float64>(data[i].value) / scale_factor;

        auto converted = ColumnArray::create(std::move(float_col), single_array.getOffsetsPtr());
        return executeWithConstArray<Float64>(*converted, col_max_lag, input_rows_count);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn * col_max_lag = nullptr;
        if (arguments.size() > 1)
        {
            if (!isNativeInteger(arguments[1].type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Second argument (max_lag) of function {} must be an integer (up to 64-bit), got {}.",
                    getName(),
                    arguments[1].type->getName());

            col_max_lag = arguments[1].column.get();
            if (!arguments[1].type->isValueRepresentedByUnsignedInteger())
                validateMaxLag(col_max_lag, input_rows_count);
        }

        /// When the array argument is constant, extract the single array to avoid materializing it.
        if (const auto * col_const = typeid_cast<const ColumnConst *>(arguments[0].column.get()))
        {
            const auto & single_array = assert_cast<const ColumnArray &>(col_const->getDataColumn());
            return executeConstArray(single_array, col_max_lag, input_rows_count);
        }

        const auto * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!col_array)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must be an array", getName());

        return executeNonConstArray(*col_array, col_max_lag);
    }

    ColumnPtr executeConstArray(const ColumnArray & single_array, const IColumn * col_max_lag, size_t input_rows_count) const
    {
        const IColumn & nested = single_array.getData();

        if (checkColumn<ColumnNothing>(nested))
        {
            auto offsets = ColumnArray::ColumnOffsets::create(input_rows_count, 0);
            return ColumnArray::create(ColumnFloat64::create(), std::move(offsets));
        }

        if (checkColumn<ColumnVector<UInt8>>(nested))
            return executeWithConstArray<UInt8>(single_array, col_max_lag, input_rows_count);
        else if (checkColumn<ColumnVector<UInt16>>(nested))
            return executeWithConstArray<UInt16>(single_array, col_max_lag, input_rows_count);
        else if (checkColumn<ColumnVector<UInt32>>(nested))
            return executeWithConstArray<UInt32>(single_array, col_max_lag, input_rows_count);
        else if (checkColumn<ColumnVector<UInt64>>(nested))
            return executeWithConstArray<UInt64>(single_array, col_max_lag, input_rows_count);
        else if (checkColumn<ColumnVector<UInt128>>(nested))
            return executeWithConstArray<UInt128>(single_array, col_max_lag, input_rows_count);
        else if (checkColumn<ColumnVector<UInt256>>(nested))
            return executeWithConstArray<UInt256>(single_array, col_max_lag, input_rows_count);
        else if (checkColumn<ColumnVector<Int8>>(nested))
            return executeWithConstArray<Int8>(single_array, col_max_lag, input_rows_count);
        else if (checkColumn<ColumnVector<Int16>>(nested))
            return executeWithConstArray<Int16>(single_array, col_max_lag, input_rows_count);
        else if (checkColumn<ColumnVector<Int32>>(nested))
            return executeWithConstArray<Int32>(single_array, col_max_lag, input_rows_count);
        else if (checkColumn<ColumnVector<Int64>>(nested))
            return executeWithConstArray<Int64>(single_array, col_max_lag, input_rows_count);
        else if (checkColumn<ColumnVector<Int128>>(nested))
            return executeWithConstArray<Int128>(single_array, col_max_lag, input_rows_count);
        else if (checkColumn<ColumnVector<Int256>>(nested))
            return executeWithConstArray<Int256>(single_array, col_max_lag, input_rows_count);
        else if (checkColumn<ColumnVector<Float32>>(nested))
            return executeWithConstArray<Float32>(single_array, col_max_lag, input_rows_count);
        else if (checkColumn<ColumnVector<Float64>>(nested))
            return executeWithConstArray<Float64>(single_array, col_max_lag, input_rows_count);
        else if (checkColumn<ColumnDecimal<Decimal32>>(nested))
            return executeWithConstDecimalArray<Decimal32>(single_array, col_max_lag, input_rows_count);
        else if (checkColumn<ColumnDecimal<Decimal64>>(nested))
            return executeWithConstDecimalArray<Decimal64>(single_array, col_max_lag, input_rows_count);
        else if (checkColumn<ColumnDecimal<Decimal128>>(nested))
            return executeWithConstDecimalArray<Decimal128>(single_array, col_max_lag, input_rows_count);
        else if (checkColumn<ColumnDecimal<Decimal256>>(nested))
            return executeWithConstDecimalArray<Decimal256>(single_array, col_max_lag, input_rows_count);

        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Function {} only accepts arrays of integers, floating-point numbers, or decimals. Unsupported type: {}",
            getName(),
            nested.getFamilyName());
    }

    ColumnPtr executeNonConstArray(const ColumnArray & col_array, const IColumn * col_max_lag) const
    {
        const IColumn & nested = col_array.getData();

        if (checkColumn<ColumnNothing>(nested))
            return ColumnArray::create(ColumnFloat64::create(), col_array.getOffsetsPtr());

        if (checkColumn<ColumnVector<UInt8>>(nested))
            return executeWithType<UInt8>(col_array, col_max_lag);
        else if (checkColumn<ColumnVector<UInt16>>(nested))
            return executeWithType<UInt16>(col_array, col_max_lag);
        else if (checkColumn<ColumnVector<UInt32>>(nested))
            return executeWithType<UInt32>(col_array, col_max_lag);
        else if (checkColumn<ColumnVector<UInt64>>(nested))
            return executeWithType<UInt64>(col_array, col_max_lag);
        else if (checkColumn<ColumnVector<UInt128>>(nested))
            return executeWithType<UInt128>(col_array, col_max_lag);
        else if (checkColumn<ColumnVector<UInt256>>(nested))
            return executeWithType<UInt256>(col_array, col_max_lag);
        else if (checkColumn<ColumnVector<Int8>>(nested))
            return executeWithType<Int8>(col_array, col_max_lag);
        else if (checkColumn<ColumnVector<Int16>>(nested))
            return executeWithType<Int16>(col_array, col_max_lag);
        else if (checkColumn<ColumnVector<Int32>>(nested))
            return executeWithType<Int32>(col_array, col_max_lag);
        else if (checkColumn<ColumnVector<Int64>>(nested))
            return executeWithType<Int64>(col_array, col_max_lag);
        else if (checkColumn<ColumnVector<Int128>>(nested))
            return executeWithType<Int128>(col_array, col_max_lag);
        else if (checkColumn<ColumnVector<Int256>>(nested))
            return executeWithType<Int256>(col_array, col_max_lag);
        else if (checkColumn<ColumnVector<Float32>>(nested))
            return executeWithType<Float32>(col_array, col_max_lag);
        else if (checkColumn<ColumnVector<Float64>>(nested))
            return executeWithType<Float64>(col_array, col_max_lag);
        else if (checkColumn<ColumnDecimal<Decimal32>>(nested))
            return executeWithDecimalType<Decimal32>(col_array, col_max_lag);
        else if (checkColumn<ColumnDecimal<Decimal64>>(nested))
            return executeWithDecimalType<Decimal64>(col_array, col_max_lag);
        else if (checkColumn<ColumnDecimal<Decimal128>>(nested))
            return executeWithDecimalType<Decimal128>(col_array, col_max_lag);
        else if (checkColumn<ColumnDecimal<Decimal256>>(nested))
            return executeWithDecimalType<Decimal256>(col_array, col_max_lag);

        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Function {} only accepts arrays of integers, floating-point numbers, or decimals. Unsupported type: {}",
            getName(),
            nested.getFamilyName());
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
    FunctionDocumentation::Arguments arguments
        = {{"arr", "Array of numbers.", {"Array(T)"}},
           {"max_lag", "Optional. Maximum number of lags to compute. Must be a non-negative integer.", {"Integer"}}};
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns an array of Float64. Returns NaN if variance is 0.", {"Array(Float64)"}};
    FunctionDocumentation::Examples examples
        = {{"Linear", "SELECT arrayAutocorrelation([1, 2, 3, 4, 5]);", "[1, 0.4, -0.1, -0.4, -0.4]"},
           {"Symmetric", "SELECT arrayAutocorrelation([10, 20, 10]);", "[1, -0.6666666666666669, 0.16666666666666674]"},
           {"Constant", "SELECT arrayAutocorrelation([5, 5, 5]);", "[nan, nan, nan]"},
           {"Limited", "SELECT arrayAutocorrelation([1, 2, 3, 4, 5], 2);", "[1, 0.4]"}};

    FunctionDocumentation::IntroducedIn introduced_in = {26, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayAutocorrelation>(documentation);
}

}
