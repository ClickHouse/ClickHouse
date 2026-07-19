#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <Common/Exception.h>
#include <Common/digits10.h>
#include <Common/intExp10.h>

#include <limits>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
extern const int ZERO_ARRAY_OR_TUPLE_INDEX;
extern const int ILLEGAL_COLUMN;
extern const int LOGICAL_ERROR;
}

namespace
{

struct DigitRange
{
    Int64 first; // 1-based
    Int64 count; // Number of digits to take from position first inclusive
};

std::optional<DigitRange> getDigitRange(Int64 total_digits, Int64 offset, Int128 length, bool has_length)
{
    if (offset == 0)
        throw Exception(ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX, "Indices in number are 1-based");

    Int64 first = offset > 0 ? offset : total_digits + offset + 1;
    if (first > total_digits) // Index is greater than the right boundary
        return std::nullopt;

    if (!has_length)
    {
        first = std::max<Int64>(first, 1);
        return DigitRange{first, total_digits - first + 1};
    }

    if (length == 0)
        return std::nullopt;

    if (length < 0)
    {
        const Int128 last = total_digits + length; // Negative length: absolute end position
        if (last < 1)
            return std::nullopt;

        first = std::max<Int64>(first, 1);
        if (last < first)
            return std::nullopt;

        return DigitRange{first, static_cast<Int64>(last - first + 1)};
    }

    if (first <= 0)
    {
        // Length consumed by the off-edge positions left of index 1 that the window covers.
        // `1 - first` cannot overflow: total_digits >= 1 guarantees first >= INT64_MIN + 2 at this point.
        Int64 skipped = 1 - first;
        if (length <= skipped)
            return std::nullopt;

        length -= skipped;
        first = 1;
    }

    return DigitRange{first, static_cast<Int64>(std::min<Int128>(length, total_digits - first + 1))};
}

UInt64 extractDigits(UInt64 num, Int64 offset, Int128 length, bool has_length)
{
    const Int64 total_digits = common::digits10(num);
    const auto range = getDigitRange(total_digits, offset, length, has_length);
    if (!range)
        return 0;
    const Int64 suffix = total_digits - range->first - range->count + 1; // Suffix to remove
    // getDigitRange guarantees first in [1, total_digits] and count in [1, total_digits - first + 1],
    // so suffix in [0, 19] and count in [1, 20]; intExp10 is never called with a negative argument
    // and never returns 0 here. Validate defensively, if a future change breaks this.
    if (suffix < 0 || range->count < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Negative argument to intExp10 in function 'digits'");
    const UInt64 shifted = num / intExp10(static_cast<int>(suffix));
    return range->count >= 20 ? shifted : shifted % intExp10(static_cast<int>(range->count));
}

class FunctionDigits final : public IFunction
{
public:
    static constexpr auto name = "digits";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDigits>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool isDeterministic() const override { return true; }
    bool canBeExecutedOnDefaultArguments() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const bool has_nullable = getNullPresense(arguments).has_nullable;

        ColumnsWithTypeAndName args_without_nullable = arguments;
        for (auto & arg : args_without_nullable)
            arg.type = removeNullable(arg.type);

        auto validateArgType = [](const IDataType & type)
        {
            return isNativeInteger(type) || isNothing(type);
        };

        FunctionArgumentDescriptors mandatory_args{
            {"number",
             static_cast<FunctionArgumentDescriptor::TypeValidator>(validateArgType),
             nullptr,
             "Int8/Int16/Int32/Int64/UInt8/UInt16/UInt32/UInt64"},
            {"offset",
             static_cast<FunctionArgumentDescriptor::TypeValidator>(validateArgType),
             nullptr,
             "Int8/Int16/Int32/Int64/UInt8/UInt16/UInt32/UInt64"}};
        FunctionArgumentDescriptors optional_args{
            {"length",
             static_cast<FunctionArgumentDescriptor::TypeValidator>(validateArgType),
             nullptr,
             "Int8/Int16/Int32/Int64/UInt8/UInt16/UInt32/UInt64"}};
        validateFunctionArguments(*this, args_without_nullable, mandatory_args, optional_args);

        DataTypePtr result = std::make_shared<DataTypeUInt64>();
        if (has_nullable)
            return makeNullable(result);
        return result;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (arguments[0].column->onlyNull())
            return makeNullable(std::make_shared<DataTypeUInt64>())->createColumnConstWithDefaultValue(input_rows_count);

        auto combined_null_map = ColumnUInt8::create(input_rows_count, static_cast<UInt8>(0));
        auto & null_map_data = combined_null_map->getData();
        bool any_nullable = false;

        auto combineNullMap = [&](ColumnPtr & col)
        {
            if (const auto * n = checkAndGetColumn<ColumnNullable>(col.get()))
            {
                any_nullable = true;
                const auto & src = n->getNullMapData();
                for (size_t i = 0; i < input_rows_count; ++i)
                    null_map_data[i] |= src[i];
                col = n->getNestedColumnPtr();
            }
        };
        ColumnPtr number_column = arguments[0].column->convertToFullColumnIfConst();
        combineNullMap(number_column);
        ColumnPtr offset_column = arguments[1].column->convertToFullColumnIfConst();
        combineNullMap(offset_column);
        ColumnPtr length_column = nullptr;
        bool has_length = false;
        if (arguments.size() == 3)
        {
            has_length = true;
            length_column = arguments[2].column->convertToFullColumnIfConst();
            combineNullMap(length_column);
        }
        auto result = ColumnUInt64::create(input_rows_count);
        auto & result_data = result->getData();

        if (!castTypeToEither<ColumnInt8, ColumnInt16, ColumnInt32, ColumnInt64, ColumnUInt8, ColumnUInt16, ColumnUInt32, ColumnUInt64>(
                number_column.get(),
                [&](const auto & col)
                {
                    const auto & data = col.getData();
                    using T = typename std::decay_t<decltype(col)>::ValueType;

                    for (size_t i = 0; i < input_rows_count; ++i)
                    {
                        if (any_nullable && null_map_data[i])
                        {
                            result_data[i] = 0;
                            continue;
                        }
                        T num = data[i];
                        UInt64 magnitude = 0;
                        if constexpr (std::is_signed_v<T>)
                            magnitude = num < 0 ? -static_cast<UInt64>(num) : static_cast<UInt64>(num);
                        else
                            magnitude = static_cast<UInt64>(num);
                        Int64 offset = getClampedInt64(*offset_column, i);
                        Int128 length = 0;
                        if (has_length)
                            length = getWideInt(*length_column, i);
                        result_data[i] = extractDigits(magnitude, offset, length, has_length);
                    }
                    return true;
                }))
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", number_column->getName(), getName());

        if (any_nullable)
            return wrapInNullable(std::move(result), std::move(combined_null_map));
        return result;
    }

    static Int64 getClampedInt64(const IColumn & col, size_t index)
    {
        if (col.getDataType() == TypeIndex::UInt64
            && col.getUInt(index) > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            return std::numeric_limits<Int64>::max();
        return col.getInt(index);
    }

    /// Preserves the full range of a 64-bit argument without a lossy clamp,
    /// Needed for `length`, which is reduced by up to ~2^63 off-edge
    /// positions when the offset is very negative, so its exact value above INT64_MAX is observable
    static Int128 getWideInt(const IColumn & col, size_t index)
    {
        if (col.getDataType() == TypeIndex::UInt64)
            return static_cast<Int128>(col.getUInt(index));
        return static_cast<Int128>(col.getInt(index));
    }
};

}
REGISTER_FUNCTION(Digits)
{
    FunctionDocumentation::Description description = R"(
Returns the digits of a number `n` which starts at the specified index `offset`.
Counting starts from 1 with the following logic:
- If `offset` is `0`, an exception is thrown, as `offset` is 1-based.
- If `offset` is negative, counting starts `offset` digits from the end of the number, rather than from the beginning.
- If `offset` is greater than the number of digits in `n`, `0` is returned.

An optional argument `length` uses the following logic:
- If `length` is positive, it means number of digits to take from offset
- If `length` is negative, it means number of digits from the right of the number to exclude
    )";
    FunctionDocumentation::Syntax syntax = "digits(n, offset[, length])";
    FunctionDocumentation::Arguments arguments
        = {{"n", "The number to calculate digits from.", {"(U)Int8", "(U)Int16", "(U)Int32", "(U)Int64"}},
           {"offset", "The starting position of the digit in `n`.", {"(U)Int8", "(U)Int16", "(U)Int32", "(U)Int64"}},
           {"length", "Optional. The maximum length of the digits.", {"(U)Int8", "(U)Int16", "(U)Int32", "(U)Int64"}}};
    FunctionDocumentation::ReturnedValue returned_value
        = {"The selected digits of `n`, interpreted as a `UInt64`. Returns `0` if the selected range is empty. Leading zeros are not "
           "preserved.",
           {"UInt64"}};
    FunctionDocumentation::Examples examples
        = {{"Positive offset", "SELECT digits(1234567890, 7)", "7890"},
           {"Positive offset and length", "SELECT digits(1234567890, 7, 2)", "78"},
           {"Negative offset counts from the right", "SELECT digits(1234567890, -3)", "890"},
           {"Negative length excludes digits from the right", "SELECT digits(1234567890, 3, -2)", "345678"},
           {"Offset past the end returns 0", "SELECT digits(1234567890, 11)", "0"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionDigits>(documentation);
}

}
