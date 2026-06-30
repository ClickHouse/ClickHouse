#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <Common/Exception.h>
#include <Common/intExp10.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ZERO_ARRAY_OR_TUPLE_INDEX;
extern const int ILLEGAL_COLUMN;
}

namespace
{
template <typename T> // Taken from countDigits.cpp
int digits10(T x)
{
    if (x < 10ULL)
        return 1;
    if (x < 100ULL)
        return 2;
    if (x < 1000ULL)
        return 3;

    if (x < 1000000000000ULL)
    {
        if (x < 100000000ULL)
        {
            if (x < 1000000ULL)
            {
                if (x < 10000ULL)
                    return 4;
                return 5 + (x >= 100000ULL);
            }

            return 7 + (x >= 10000000ULL);
        }

        if (x < 10000000000ULL)
            return 9 + (x >= 1000000000ULL);

        return 11 + (x >= 100000000000ULL);
    }

    return 12 + digits10(x / 1000000000000ULL);
}

UInt64 extractDigits(UInt64 num, Int64 offset, Int64 length, bool has_length)
{
    if (offset == 0)
        throw Exception(ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX, "Indices in number are 1-based");
    if (has_length && length == 0) // No digits to return
        return 0ULL;

    const Int64 total_digits = digits10(num);

    if (offset < 0)
        offset = total_digits + offset + 1; // Index from the left

    if (offset > total_digits) // Index is greater than the right boundary
        return 0ULL;

    Int64 count; // Number of digits to take from offset inclusive
    if (!has_length)
    {
        offset = std::max<Int64>(offset, 1);
        count = total_digits - offset + 1;
    }
    else
    {
        if (length < 0)
        {
            const Int64 end = total_digits + length; // negative length: absolute end position
            offset = std::max<Int64>(offset, 1);
            count = end - offset + 1;
        }
        else
        {
            // Length consumed by the off-edge positions left of index 1 that the window covers.
            // `1 - offset` cannot overflow: digits10 >= 1 guarantees offset >= INT64_MIN + 2 at this point.
            Int64 required = (offset <= 0 ? 1 - offset : 0);
            length = length - required;
            offset = std::max<Int64>(offset, 1);
            count = std::min<Int64>(length, total_digits - offset + 1);
        }
    }
    if (count <= 0)
        return 0ULL;
    const Int64 suffix = total_digits - (offset + count - 1); // Suffix to remove
    const UInt64 shifted = num / intExp10(static_cast<int>(suffix)); // Okay to cast suffix to int because suffix range is [0, 20)
    return count >= 20 ? shifted : shifted % intExp10(static_cast<int>(count)); // Okay to cast count to int because count range is [1, 20]
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

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"number",
             static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeInteger),
             nullptr,
             "Int8/Int16/Int32/Int64/UInt8/UInt16/UInt32/UInt64"},
            {"offset",
             static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeInteger),
             nullptr,
             "Int8/Int16/Int32/Int64/UInt8/UInt16/UInt32/UInt64"}};
        FunctionArgumentDescriptors optional_args{
            {"length",
             static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeInteger),
             nullptr,
             "Int8/Int16/Int32/Int64/UInt8/UInt16/UInt32/UInt64"}};
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn * number_column = arguments[0].column.get();
        auto result = ColumnUInt64::create(input_rows_count);
        auto & result_data = result->getData();

        if (!castTypeToEither<ColumnInt8, ColumnInt16, ColumnInt32, ColumnInt64, ColumnUInt8, ColumnUInt16, ColumnUInt32, ColumnUInt64>(
                number_column,
                [&](const auto & col)
                {
                    const auto & data = col.getData();
                    using T = typename std::decay_t<decltype(col)>::ValueType;
                    bool has_length = false;
                    if (arguments.size() == 3)
                        has_length = true;

                    for (size_t i = 0; i < input_rows_count; ++i)
                    {
                        T num = data[i];
                        UInt64 magnitude;
                        if constexpr (std::is_signed_v<T>)
                            magnitude = num < 0 ? -static_cast<UInt64>(num) : static_cast<UInt64>(num);
                        else
                            magnitude = static_cast<UInt64>(num);
                        Int64 offset = arguments[1].column->getInt(i);
                        Int64 length = 0;
                        if (has_length)
                            length = arguments[2].column->getInt(i);
                        result_data[i] = extractDigits(magnitude, offset, length, has_length);
                    }
                    return true;
                }))
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", number_column->getName(), getName());

        return result;
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
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionDigits>(documentation);
}

}
