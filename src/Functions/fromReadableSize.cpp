#include <boost/algorithm/string/case_conv.hpp>
#include <boost/convert.hpp>
#include <boost/convert/strtol.hpp>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

    const std::unordered_map<std::string_view, UInt64> size_unit_to_bytes =
    {
        {"b", 1},
        // ISO/IEC 80000-13 binary units
        {"kib", 1024},                  // 1024
        {"mib", 1048576},               // 1024 * 1024
        {"gib", 1073741824},            // 1024 * 1024 * 1024
        {"tib", 1099511627776},         // 1024 * 1024 * 1024 * 1024
        {"pib", 1125899906842624},      // 1024 * 1024 * 1024 * 1024 * 1024
        {"eib", 1152921504606846976},   // 1024 * 1024 * 1024 * 1024 * 1024 * 1024

        // SI units
        {"kb", 1000},                    // 10e3
        {"mb", 1000000},                 // 10e6
        {"gb", 1000000000},              // 10e9
        {"tb", 1000000000000},          // 10e12
        {"pb", 1000000000000000},       // 10e15
        {"eb", 1000000000000000000},    // 10e18
    };

    class FunctionFromReadableSize : public IFunction
    {
    public:
        static constexpr auto name = "fromReadableSize";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFromReadableSize>(); }

        String getName() const override { return name; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        size_t getNumberOfArguments() const override { return 1; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (arguments.empty())
                throw Exception(
                    ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                    "Number of arguments for function {} doesn't match: passed {}, should be 1.",
                    getName(),
                    arguments.size());

            if (arguments.size() > 1)
                throw Exception(
                    ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                    "Number of arguments for function {} doesn't match: passed {}, should be 1.",
                    getName(),
                    arguments.size());

            const IDataType & type = *arguments[0];

            if (!isString(type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot format {} as time string.", type.getName());

            return std::make_shared<DataTypeUInt64>();
        }

        bool useDefaultImplementationForConstants() const override { return true; }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            auto col_to = ColumnUInt64::create();
            auto & res_data = col_to->getData();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                std::string_view str{arguments[0].column->getDataAt(i)};
                Int64 token_tail = 0;
                Int64 token_front = 0;
                Int64 last_pos = str.length() - 1;
                UInt64 result = 0;

                /// ignore '.' and ' ' at the end of string
                while (last_pos >= 0 && (str[last_pos] == ' ' || str[last_pos] == '.'))
                    --last_pos;

                /// no valid characters
                if (last_pos < 0)
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Invalid expression for function {}, don't find valid characters, str: \"{}\".",
                        getName(),
                        String(str));
                }

                /// last pos character must be character and not be separator or number after ignoring '.' and ' '
                if (!isalpha(str[last_pos]))
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid expression for function {}, str: \"{}\".", getName(), String(str));
                }

                /// scan spaces at the beginning
                scanSpaces(str, token_tail, last_pos);
                token_front = token_tail;
                /// scan unsigned integer
                if (!scanUnsignedInteger(str, token_tail, last_pos))
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Invalid expression for function {}, find number failed, str: \"{}\".",
                        getName(),
                        String(str));
                }

                /// if there is a '.', then scan another integer to get a float number
                if (token_tail <= last_pos && str[token_tail] == '.')
                {
                    token_tail++;
                    if (!scanUnsignedInteger(str, token_tail, last_pos))
                    {
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Invalid expression for function {}, find number after '.' failed, str: \"{}\".",
                            getName(),
                            String(str));
                    }
                }

                /// convert float/integer string to float
                Float64 base = 0;
                std::string_view base_str = str.substr(token_front, token_tail - token_front);
                auto value = boost::convert<Float64>(base_str, boost::cnv::strtol());
                if (!value.has_value())
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Invalid expression for function {}, convert string to float64 failed: \"{}\".",
                        getName(),
                        String(base_str));
                }
                base = value.get();

                scanSpaces(str, token_tail, last_pos);
                token_front = token_tail;

                /// scan a unit
                if (!scanUnit(str, token_tail, last_pos))
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Invalid expression for function {}, find unit failed, str: \"{}\".",
                        getName(),
                        String(str));
                }

                std::string unit = std::string{str.substr(token_front, token_tail - token_front)};
                boost::algorithm::to_lower(unit);
                auto iter = size_unit_to_bytes.find(unit);
                if (iter == size_unit_to_bytes.end()) /// not find unit
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS, "Invalid expression for function {}, parse unit failed: \"{}\".", getName(), unit);
                }
                // Due to a pontentially limited precision on the input value we might end up with a non-integer amount of bytes when parsing binary units.
                // This doesn't make sense so we round up to indicate the byte size that can fit the passed size.
                result = static_cast<UInt64>(std::ceil(base * iter->second));

                res_data.emplace_back(result);
            }

            return col_to;
        }

        /// scan an unsigned integer number
        static bool scanUnsignedInteger(std::string_view & str, Int64 & index, Int64 last_pos)
        {
            int64_t begin_index = index;
            while (index <= last_pos && isdigit(str[index]))
            {
                index++;
            }
            return index != begin_index;
        }

        /// scan a unit
        static bool scanUnit(std::string_view & str, Int64 & index, Int64 last_pos)
        {
            int64_t begin_index = index;
            while (index <= last_pos && !isdigit(str[index]) && !isSeparator(str[index]))
            {
                index++;
            }
            return index != begin_index;
        }

        /// scan spaces
        static void scanSpaces(std::string_view & str, Int64 & index, Int64 last_pos)
        {
            while (index <= last_pos && (str[index] == ' '))
            {
                index++;
            }
        }

        static bool isSeparator(char symbol)
        {
            return symbol == ';' || symbol == '-' || symbol == '+' || symbol == ',' || symbol == ':' || symbol == ' ';
        }
    };

}

REGISTER_FUNCTION(FromReadableSize)
{
    factory.registerFunction<FunctionFromReadableSize>(FunctionDocumentation
        {
            .description=R"(
Given a string containing the readable representation of a byte size, this function returns the corresponding number of bytes:
[example:basic_binary]
[example:basic_decimal]

If the resulting number of bytes has a non-zero decimal part, the result is rounded up to indicate the number of bytes necessary to accommodate the provided size.
[example:round]

Accepts readable sizes up to the Exabyte (EB/EiB).

It always returns an UInt64 value.
)",
            .examples{
                {"basic_binary", "SELECT fromReadableSize('1 KiB')", "1024"},
                {"basic_decimal", "SELECT fromReadableSize('1.523 KB')", "1523"},
                {"round", "SELECT fromReadableSize('1.0001 KiB')", "1025"},
            },
            .categories{"OtherFunctions"}
        }
    );
}

}
