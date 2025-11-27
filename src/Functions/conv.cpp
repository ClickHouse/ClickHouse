#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>
#include <base/types.h>
#include <Common/Exception.h>

#include <algorithm>
#include <cctype>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionConv : public IFunction
{
public:
    static constexpr auto name = "conv";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionConv>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]) && !isNativeNumber(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}. Must be String, FixedString or Number",
                arguments[0]->getName(),
                getName());
        if (!isNativeInteger(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}. Must be Integer",
                arguments[1]->getName(),
                getName());
        if (!isNativeInteger(arguments[2]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of third argument of function {}. Must be Integer",
                arguments[2]->getName(),
                getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr number_column = arguments[0].column;
        if (!isStringOrFixedString(arguments[0].type))
        {
            ColumnWithTypeAndName string_arg = {number_column, arguments[0].type, arguments[0].name};
            number_column = castColumn(string_arg, std::make_shared<DataTypeString>());
        }
        const auto * number_col = checkAndGetColumn<ColumnString>(number_column.get());
        const auto * number_const_col = checkAndGetColumnConst<ColumnString>(number_column.get());
        if (!number_col && !number_const_col)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(),
                getName());
        const auto & from_base_column = arguments[1].column;
        const auto & to_base_column = arguments[2].column;
        auto result_column = ColumnString::create();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string number_str;
            if (number_col)
                number_str = number_col->getDataAt(i).toString();
            else
                number_str = number_const_col->getValue<String>();
            const Int64 from_base = from_base_column->getInt(i);
            const Int64 to_base = to_base_column->getInt(i);
            std::string result = convertNumber(number_str, static_cast<int>(from_base), static_cast<int>(to_base));
            result_column->insertData(result.data(), result.size());
        }

        return result_column;
    }

private:
    static std::string convertNumber(const std::string & number, const int from_base, const int to_base)
    {
        if (from_base < 2 || from_base > 36 || to_base < 2 || to_base > 36)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Base is less than 2 or greater than 36 which is not supported");
        if (number.empty())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Number for conversion is empty");
        std::string clean_number = number;
        bool is_negative = false;
        // Trim leading whitespace
        clean_number.erase(0, clean_number.find_first_not_of(" \t\n\r\f\v"));
        // Trim trailing whitespace
        if (!clean_number.empty())
        {
            clean_number.erase(clean_number.find_last_not_of(" \t\n\r\f\v") + 1);
        }
        if (clean_number.empty())
            return "0";

        if (clean_number[0] == '-')
        {
            is_negative = true;
            clean_number = clean_number.substr(1);
        }
        else if (clean_number[0] == '+')
        {
            clean_number = clean_number.substr(1);
        }

        if (clean_number.empty())
            return "0";

        try
        {
            UInt64 value = parseFromBase(clean_number, from_base);
            if (is_negative)
            {
                value = 0ULL - value;
            }
            return formatToBase(value, to_base);
        }
        catch (...)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Invalid number format for base conversion in function conv");

        }
    }

    static UInt64 parseFromBase(const std::string & str, int base)
    {
        if (str.empty())
            return 0;

        UInt64 result = 0;

        // (MySQL behavior)
        for (size_t i = 0; i < str.length(); ++i)
        {
            char c = str[i];
            int digit = charToDigit(c);
            if (digit < 0 || digit >= base)
            {
                // Stop at first invalid character
                break;
            }

            // Check for overflow before multiplication
            if (result > (UINT64_MAX - digit) / base)
            {
                return UINT64_MAX;
            }

            result = result * base + digit;
        }

        return result;
    }

    static std::string formatToBase(UInt64 value, int base)
    {
        if (value == 0)
            return "0";
        std::string result;
        while (value > 0)
        {
            int digit = value % base;
            result.push_back(digitToChar(digit));
            value /= base;
        }
        std::reverse(result.begin(), result.end());
        return result;
    }

    static int charToDigit(char c)
    {
        if (c >= '0' && c <= '9')
            return c - '0';
        if (c >= 'A' && c <= 'Z')
            return c - 'A' + 10;
        if (c >= 'a' && c <= 'z')
            return c - 'a' + 10;
        return -1; // Invalid character
    }

    static char digitToChar(int digit)
    {
        if (digit >= 0 && digit <= 9)
            return '0' + digit;
        if (digit >= 10 && digit <= 35)
            return 'A' + digit - 10;
        return '0'; // Should never happen
    }
};

REGISTER_FUNCTION(Conv)
{
    factory.registerFunction<FunctionConv>(FunctionDocumentation{
        .description = R"(
Converts numbers between different number bases.

The function converts a number from one base to another. It supports bases from 2 to 36.
For bases higher than 10, letters A-Z (case insensitive) are used to represent digits 10-35.

This function is compatible with MySQL's CONV() function.
)",
        .syntax = "conv(number, from_base, to_base)",
        .arguments
        = {{"number", "The number to convert. Can be a string or numeric type."},
           {"from_base", "The source base (2-36). Must be an integer."},
           {"to_base", "The target base (2-36). Must be an integer."}},
        .returned_value = {"String representation of the number in the target base."},
        .examples
        = {{"Convert decimal to binary", "SELECT conv('10', 10, 2)", "1010"},
           {"Convert hexadecimal to decimal", "SELECT conv('FF', 16, 10)", "255"},
           {"Convert with negative number", "SELECT conv('-1', 10, 16)", "FFFFFFFFFFFFFFFF"},
           {"Convert binary to octal", "SELECT conv('1010', 2, 8)", "12"}},
        .introduced_in = {1, 1},
        .category = FunctionDocumentation::Category::String});
}

}
