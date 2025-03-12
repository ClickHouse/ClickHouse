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
    const std::unordered_map<std::string_view, Float64> time_unit_to_float =
    {
        {"years", 365 * 24 * 3600},
        {"year", 365 * 24 * 3600},
        {"yr", 365 * 24 * 3600},
        {"y", 365 * 24 * 3600},

        {"months", 30.5 * 24 * 3600},
        {"month", 30.5 * 24 * 3600},
        {"mo", 30.5 * 24 * 3600},

        {"weeks", 7 * 24 * 3600},
        {"week", 7 * 24 * 3600},
        {"w", 7 * 24 * 3600},

        {"days", 24 * 3600},
        {"day", 24 * 3600},
        {"d", 24 * 3600},

        {"hours", 3600},
        {"hour", 3600},
        {"hr", 3600},
        {"h", 3600},

        {"minutes", 60},
        {"minute", 60},
        {"min", 60},
        {"m", 60},

        {"seconds", 1},
        {"second", 1},
        {"sec", 1},
        {"s", 1},

        {"milliseconds", 1e-3},
        {"millisecond", 1e-3},
        {"millisec", 1e-3},
        {"ms", 1e-3},

        {"microseconds", 1e-6},
        {"microsecond", 1e-6},
        {"microsec", 1e-6},
        {"μs", 1e-6},
        {"us", 1e-6},

        {"nanoseconds", 1e-9},
        {"nanosecond", 1e-9},
        {"nanosec", 1e-9},
        {"ns", 1e-9},
    };

    /** Prints amount of seconds in form of:
     * "1 year 2 months 4 weeks 12 days 3 hours 1 minute 33 seconds".
     * ' ', ';', '-', '+', ',', ':' can be used as separator, eg. "1yr-2mo", "2m:6s"
     *
     * valid expressions:
     * SELECT parseTimeDelta('1 min 35 sec');
     * SELECT parseTimeDelta('0m;11.23s.');
     * SELECT parseTimeDelta('11hr 25min 3.1s');
     * SELECT parseTimeDelta('0.00123 seconds');
     * SELECT parseTimeDelta('1yr2mo');
     * SELECT parseTimeDelta('11s+22min');
     * SELECT parseTimeDelta('1yr-2mo-4w + 12 days, 3 hours : 1 minute ; 33 seconds');
     *
     * invalid expressions:
     * SELECT parseTimeDelta();
     * SELECT parseTimeDelta('1yr', 1);
     * SELECT parseTimeDelta(1);
     * SELECT parseTimeDelta(' ');
     * SELECT parseTimeDelta('-1yr');
     * SELECT parseTimeDelta('1yr-');
     * SELECT parseTimeDelta('yr2mo');
     * SELECT parseTimeDelta('1.yr2mo');
     * SELECT parseTimeDelta('1-yr');
     * SELECT parseTimeDelta('1 1yr');
     * SELECT parseTimeDelta('1yyr');
     * SELECT parseTimeDelta('1yr-2mo-4w + 12 days, 3 hours : 1 minute ;. 33 seconds');
     *
     * The length of years and months (and even days in presence of time adjustments) are rough:
     * year is just 365 days, month is 30.5 days, day is 86400 seconds, similarly to what formatReadableTimeDelta is doing.
     */
    class FunctionParseTimeDelta : public IFunction
    {
    public:
        static constexpr auto name = "parseTimeDelta";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionParseTimeDelta>(); }

        String getName() const override { return name; }

        bool isVariadic() const override { return true; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        size_t getNumberOfArguments() const override { return 0; }

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

            return std::make_shared<DataTypeFloat64>();
        }

        DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
        {
            return std::make_shared<DataTypeFloat64>();
        }

        bool useDefaultImplementationForConstants() const override { return true; }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            auto col_to = ColumnFloat64::create();
            auto & res_data = col_to->getData();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                std::string_view str{arguments[0].column->getDataAt(i)};
                Int64 token_tail = 0;
                Int64 token_front = 0;
                Int64 last_pos = str.length() - 1;
                Float64 result = 0;

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

                while (token_tail <= last_pos)
                {
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

                    /// get unit number
                    std::string_view unit = str.substr(token_front, token_tail - token_front);
                    auto iter = time_unit_to_float.find(unit);
                    if (iter == time_unit_to_float.end()) /// not find unit
                    {
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS, "Invalid expression for function {}, parse unit failed: \"{}\".", getName(), unit);
                    }
                    result += base * iter->second;

                    /// scan separator between two tokens
                    scanSeparator(str, token_tail, last_pos);
                    token_front = token_tail;
                }

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

        /// scan for characters to ignore
        static void scanSeparator(std::string_view & str, Int64 & index, Int64 last_pos)
        {
            /// ignore spaces
            scanSpaces(str, index, last_pos);

            /// ignore separator
            if (index <= last_pos && isSeparator(str[index]))
            {
                index++;
            }

            scanSpaces(str, index, last_pos);
        }

        static bool isSeparator(char symbol)
        {
            return symbol == ';' || symbol == '-' || symbol == '+' || symbol == ',' || symbol == ':' || symbol == ' ';
        }
    };

}

REGISTER_FUNCTION(ParseTimeDelta)
{
    factory.registerFunction<FunctionParseTimeDelta>();
}

}
