#include <base/types.h>
#include <boost/algorithm/string/case_conv.hpp>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Common/FunctionDocumentation.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <cmath>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_TEXT;
    extern const int ILLEGAL_COLUMN;
    extern const int UNEXPECTED_DATA_AFTER_PARSED_VALUE;
}

enum class ErrorHandling : uint8_t
{
    Exception,
    Zero,
    Null
};

using ScaleFactors = std::unordered_map<std::string_view, size_t>;

/** parseReadableSize* - Returns the number of bytes corresponding to a given readable binary or decimal size.
  * Examples:
  *  - `parseReadableSize('123 MiB')`
  *  - `parseReadableSize('123 MB')`
  * Meant to be the inverse of `formatReadable*Size` with the following exceptions:
  *  - Number of bytes is returned as an unsigned integer amount instead of a float. Decimal points are rounded up to the nearest integer.
  *  - Negative numbers are not allowed as negative sizes don't make sense.
  * Flavours:
  *  - parseReadableSize
  *  - parseReadableSizeOrNull
  *  - parseReadableSizeOrZero
  */
template <typename Name, ErrorHandling error_handling>
class FunctionParseReadable : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionParseReadable<Name, error_handling>>(); }

    String getName() const override { return name; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args
        {
            {"readable_size", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
        };
        validateFunctionArguments(*this, arguments, args);
        DataTypePtr return_type = std::make_shared<DataTypeUInt64>();
        if constexpr (error_handling == ErrorHandling::Null)
            return std::make_shared<DataTypeNullable>(return_type);
        else
            return return_type;
    }


    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_str = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!col_str)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first ('str') argument of function {}. Must be string.",
                arguments[0].column->getName(),
                getName()
            );
        }

        auto col_res = ColumnUInt64::create(input_rows_count);

        ColumnUInt8::MutablePtr col_null_map;
        if constexpr (error_handling == ErrorHandling::Null)
            col_null_map = ColumnUInt8::create(input_rows_count, 0);

        auto & res_data = col_res->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view value = col_str->getDataAt(i).toView();
            try
            {
                UInt64 num_bytes = parseReadableFormat(value);
                res_data[i] = num_bytes;
            }
            catch (const Exception &)
            {
                if constexpr (error_handling == ErrorHandling::Exception)
                {
                    throw;
                }
                else
                {
                    res_data[i] = 0;
                    if constexpr (error_handling == ErrorHandling::Null)
                        col_null_map->getData()[i] = 1;
                }
            }
        }
        if constexpr (error_handling == ErrorHandling::Null)
            return ColumnNullable::create(std::move(col_res), std::move(col_null_map));
        else
            return col_res;
    }

private:

    UInt64 parseReadableFormat(const std::string_view & value) const
    {
        static const ScaleFactors scale_factors =
        {
            {"b", 1ull},
            // ISO/IEC 80000-13 binary units
            {"kib", 1024ull},
            {"mib", 1024ull * 1024ull},
            {"gib", 1024ull * 1024ull * 1024ull},
            {"tib", 1024ull * 1024ull * 1024ull * 1024ull},
            {"pib", 1024ull * 1024ull * 1024ull * 1024ull * 1024ull},
            {"eib", 1024ull * 1024ull * 1024ull * 1024ull * 1024ull * 1024ull},
            // Decimal units
            {"kb", 1000ull},
            {"mb", 1000ull * 1000ull},
            {"gb", 1000ull * 1000ull * 1000ull},
            {"tb", 1000ull * 1000ull * 1000ull * 1000ull},
            {"pb", 1000ull * 1000ull * 1000ull * 1000ull * 1000ull},
            {"eb", 1000ull * 1000ull * 1000ull * 1000ull * 1000ull * 1000ull},
        };
        ReadBufferFromString buf(value);

        // tryReadFloatText does seem to not raise any error when there is leading whitespace so we check it explicitly
        skipWhitespaceIfAny(buf);
        if (buf.getPosition() > 0)
        {
            throw Exception(
                ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED,
                "Invalid expression for function {} - Leading whitespace is not allowed (\"{}\")",
                getName(),
                value
            );
        }

        Float64 base = 0;
        if (!tryReadFloatTextPrecise(base, buf))    // If we use the default (fast) tryReadFloatText this returns True on garbage input so we use the Precise version
        {
            throw Exception(
                ErrorCodes::CANNOT_PARSE_NUMBER,
                "Invalid expression for function {} - Unable to parse readable size numeric component (\"{}\")",
                getName(),
                value
            );
        }
        if (std::isnan(base) || !std::isfinite(base))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Invalid expression for function {} - Invalid numeric component: {}", getName(), base);
        }
        if (base < 0)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Invalid expression for function {} - Negative sizes are not allowed ({})", getName(), base);
        }

        skipWhitespaceIfAny(buf);

        String unit;
        readStringUntilWhitespace(unit, buf);
        boost::algorithm::to_lower(unit);
        auto iter = scale_factors.find(unit);
        if (iter == scale_factors.end())
        {
            throw Exception(
                ErrorCodes::CANNOT_PARSE_TEXT,
                "Invalid expression for function {} - Unknown readable size unit (\"{}\")",
                getName(),
                unit
            );
        }
        if (!buf.eof())
        {
            throw Exception(
                ErrorCodes::UNEXPECTED_DATA_AFTER_PARSED_VALUE,
                "Invalid expression for function {} - Found trailing characters after readable size string (\"{}\")",
                getName(),
                value);
        }

        Float64 num_bytes_with_decimals = base * iter->second;
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wimplicit-const-int-float-conversion"
        if (num_bytes_with_decimals > std::numeric_limits<UInt64>::max())
#pragma clang diagnostic pop
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid expression for function {} - Result is too big for output type (\"{}\")",
                getName(),
                num_bytes_with_decimals
            );
        }
        // As the input might be an arbitrary decimal number we might end up with a non-integer amount of bytes when parsing binary (eg MiB) units.
        // This doesn't make sense so we round up to indicate the byte size that can fit the passed size.
        return static_cast<UInt64>(std::ceil(num_bytes_with_decimals));
    }
};

struct NameParseReadableSize
{
    static constexpr auto name = "parseReadableSize";
};

struct NameParseReadableSizeOrNull
{
    static constexpr auto name = "parseReadableSizeOrNull";
};

struct NameParseReadableSizeOrZero
{
    static constexpr auto name = "parseReadableSizeOrZero";
};

using FunctionParseReadableSize = FunctionParseReadable<NameParseReadableSize, ErrorHandling::Exception>;
using FunctionParseReadableSizeOrNull = FunctionParseReadable<NameParseReadableSizeOrNull, ErrorHandling::Null>;
using FunctionParseReadableSizeOrZero = FunctionParseReadable<NameParseReadableSizeOrZero, ErrorHandling::Zero>;

FunctionDocumentation parseReadableSize_documentation {
    .description = "Given a string containing a byte size and `B`, `KiB`, `KB`, `MiB`, `MB`, etc. as a unit (i.e. [ISO/IEC 80000-13](https://en.wikipedia.org/wiki/ISO/IEC_80000) or decimal byte unit), this function returns the corresponding number of bytes. If the function is unable to parse the input value, it throws an exception.",
    .syntax = "parseReadableSize(x)",
    .arguments = {{"x", "Readable size with ISO/IEC 80000-13 or decimal byte unit ([String](../../sql-reference/data-types/string.md))"}},
    .returned_value = "Number of bytes, rounded up to the nearest integer ([UInt64](../../sql-reference/data-types/int-uint.md))",
    .examples = {
        {
            "basic",
            "SELECT arrayJoin(['1 B', '1 KiB', '3 MB', '5.314 KiB']) AS readable_sizes, parseReadableSize(readable_sizes) AS sizes;",
            R"(
┌─readable_sizes─┬───sizes─┐
│ 1 B            │       1 │
│ 1 KiB          │    1024 │
│ 3 MB           │ 3000000 │
│ 5.314 KiB      │    5442 │
└────────────────┴─────────┘)"
        },
    },
    .categories = {"OtherFunctions"},
};

FunctionDocumentation parseReadableSizeOrNull_documentation {
    .description = "Given a string containing a byte size and `B`, `KiB`, `KB`, `MiB`, `MB`, etc. as a unit (i.e. [ISO/IEC 80000-13](https://en.wikipedia.org/wiki/ISO/IEC_80000) or decimal byte unit), this function returns the corresponding number of bytes. If the function is unable to parse the input value, it returns `NULL`",
    .syntax = "parseReadableSizeOrNull(x)",
    .arguments = {{"x", "Readable size with ISO/IEC 80000-13  or decimal byte unit ([String](../../sql-reference/data-types/string.md))"}},
    .returned_value = "Number of bytes, rounded up to the nearest integer, or NULL if unable to parse the input (Nullable([UInt64](../../sql-reference/data-types/int-uint.md)))",
    .examples = {
        {
            "basic",
            "SELECT arrayJoin(['1 B', '1 KiB', '3 MB', '5.314 KiB', 'invalid']) AS readable_sizes, parseReadableSizeOrNull(readable_sizes) AS sizes;",
            R"(
┌─readable_sizes─┬───sizes─┐
│ 1 B            │       1 │
│ 1 KiB          │    1024 │
│ 3 MB           │ 3000000 │
│ 5.314 KiB      │    5442 │
│ invalid        │    ᴺᵁᴸᴸ │
└────────────────┴─────────┘)"
        },
    },
    .categories = {"OtherFunctions"},
};

FunctionDocumentation parseReadableSizeOrZero_documentation {
    .description = "Given a string containing a byte size and `B`, `KiB`, `KB`, `MiB`, `MB`, etc. as a unit (i.e. [ISO/IEC 80000-13](https://en.wikipedia.org/wiki/ISO/IEC_80000) or decimal byte unit), this function returns the corresponding number of bytes. If the function is unable to parse the input value, it returns `0`",
    .syntax = "parseReadableSizeOrZero(x)",
    .arguments = {{"x", "Readable size with ISO/IEC 80000-13 or decimal byte unit ([String](../../sql-reference/data-types/string.md))"}},
    .returned_value = "Number of bytes, rounded up to the nearest integer, or 0 if unable to parse the input ([UInt64](../../sql-reference/data-types/int-uint.md))",
    .examples = {
        {
            "basic",
            "SELECT arrayJoin(['1 B', '1 KiB', '3 MB', '5.314 KiB', 'invalid']) AS readable_sizes, parseReadableSizeOrZero(readable_sizes) AS sizes;",
            R"(
┌─readable_sizes─┬───sizes─┐
│ 1 B            │       1 │
│ 1 KiB          │    1024 │
│ 3 MB           │ 3000000 │
│ 5.314 KiB      │    5442 │
│ invalid        │       0 │
└────────────────┴─────────┘)",
        },
    },
    .categories = {"OtherFunctions"},
};

REGISTER_FUNCTION(ParseReadableSize)
{
    factory.registerFunction<FunctionParseReadableSize>(parseReadableSize_documentation);
    factory.registerFunction<FunctionParseReadableSizeOrNull>(parseReadableSizeOrNull_documentation);
    factory.registerFunction<FunctionParseReadableSizeOrZero>(parseReadableSizeOrZero_documentation);
}
}
