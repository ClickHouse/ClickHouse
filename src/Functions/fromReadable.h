#pragma once

#include <base/types.h>
#include <boost/algorithm/string/case_conv.hpp>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
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

/** fromReadble*Size - Returns the number of bytes corresponding to a given readable binary or decimal size.
  * Examples:
  *  - `fromReadableSize('123 MiB')`
  *  - `fromReadableDecimalSize('123 MB')`
  * Meant to be the inverse of `formatReadable*Size` with the following exceptions:
  *  - Number of bytes is returned as an unsigned integer amount instead of a float. Decimal points are rounded up to the nearest integer.
  *  - Negative numbers are not allowed as negative sizes don't make sense.
  * Flavours:
  *  - fromReadableSize
  *  - fromReadableSizeOrNull
  *  - fromReadableSizeOrZero
  *  - fromReadableDecimalSize
  *  - fromReadableDecimalSizeOrNull
  *  - fromReadableDecimalSizeOrZero
  */
template <typename Name, typename Impl, ErrorHandling error_handling>
class FunctionFromReadable : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFromReadable<Name, Impl, error_handling>>(); }

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
        validateFunctionArgumentTypes(*this, arguments, args);
        DataTypePtr return_type = std::make_shared<DataTypeUInt64>();
        if (error_handling == ErrorHandling::Null)
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

        const ScaleFactors & scale_factors = Impl::getScaleFactors();

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
                UInt64 num_bytes = parseReadableFormat(scale_factors, value);
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

    UInt64 parseReadableFormat(const ScaleFactors & scale_factors, const std::string_view & value) const
    {
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
        else if (std::isnan(base) || !std::isfinite(base))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid expression for function {} - Invalid numeric component: {}",
                getName(),
                base
            );
        }
        else if (base < 0)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid expression for function {} - Negative sizes are not allowed ({})",
                getName(),
                base
            );
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
        else if (!buf.eof())
        {
            throw Exception(
                ErrorCodes::UNEXPECTED_DATA_AFTER_PARSED_VALUE,
                "Invalid expression for function {} - Found trailing characters after readable size string (\"{}\")",
                getName(),
                value
            );
        }

        Float64 num_bytes_with_decimals = base * iter->second;
        if (num_bytes_with_decimals > std::numeric_limits<UInt64>::max())
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
}
