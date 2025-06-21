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
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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
        auto & result_data = result_column->getChars();
        auto & result_offsets = result_column->getOffsets();
        result_offsets.resize(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string number_str;
            if (number_col)
                number_str = number_col->getDataAt(i).toString();
            else
                number_str = number_const_col->getValue<String>();
            Int64 from_base = from_base_column->getInt(i);
            Int64 to_base = to_base_column->getInt(i);
            std::string result = convertNumber(number_str, static_cast<int>(from_base), static_cast<int>(to_base));
            result_data.insert(result.begin(), result.end());
            result_data.push_back(0);
            result_offsets[i] = result_data.size();
        }

        return result_column;
    }

private:
    static std::string convertNumber(const std::string & number, int from_base, int to_base)
    {
        if (from_base < 2 || from_base > 36 || to_base < 2 || to_base > 36)
            return "";
        if (number.empty())
            return "";
        std::string clean_number = number;
        bool is_negative = false;
        clean_number.erase(0, clean_number.find_first_not_of(" \t\n\r\f\v"));

        if (clean_number.empty())
            return "";

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
            return "";

        try
        {
            UInt64 value = parseFromBase(clean_number, from_base);
            if (is_negative)
            {
                value = static_cast<UInt64>(-static_cast<Int64>(value));
            }
            return formatToBase(value, to_base);
        }
        catch (...)
        {
            return "";
        }
    }

    static UInt64 parseFromBase(const std::string & str, int base)
    {
        if (str.empty())
            return 0;

        UInt64 result = 0;
        UInt64 base_power = 1;
        for (int i = str.length() - 1; i >= 0; --i)
        {
            char c = str[i];
            int digit = charToDigit(c);
            if (digit < 0 || digit >= base)
            {
                //stop at first invalid character
                break;
            }
            if (result > (UINT64_MAX - digit) / base_power)
            {
                return UINT64_MAX;
            }
            result += digit * base_power;
            // check base overflow for next iteration
            if (i > 0 && base_power > UINT64_MAX / base)
            {
                return UINT64_MAX;
            }

            base_power *= base;
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
            result = digitToChar(digit) + result;
            value /= base;
        }
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
        .returned_value = "String representation of the number in the target base.",
        .examples
        = {{"Convert decimal to binary", "SELECT conv('10', 10, 2)", "1010"},
           {"Convert hexadecimal to decimal", "SELECT conv('FF', 16, 10)", "255"},
           {"Convert with negative number", "SELECT conv('-1', 10, 16)", "FFFFFFFFFFFFFFFF"},
           {"Convert binary to octal", "SELECT conv('1010', 2, 8)", "12"}},
        .introduced_in = {1, 1},
        .category = FunctionDocumentation::Category::String});
}

}
