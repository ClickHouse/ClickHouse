#include <boost/convert.hpp>
#include <boost/convert/strtol.hpp>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

#include <expected>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    enum class ParseTimeDeltaErrorHandling : uint8_t
    {
        Exception,
        Zero,
        Null
    };

    /// Type imposed by PreformattedMessage::format_string_args.
    using ParseTimeDeltaErrorArgs = std::vector<String>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    struct ParseTimeDeltaError
    {
        int error_code;
        String error_message;
        /// Always a string literal, matching Exception::message_format_string's static lifetime.
        std::string_view error_pattern;
        ParseTimeDeltaErrorArgs error_args;
    };

    using Float64OrError = std::expected<Float64, ParseTimeDeltaError>;

    const UnorderedMapWithMemoryTracking<std::string_view, Float64> time_unit_to_float =
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
        {"μs", 1e-6}, // U+03BC = Greek letter mu
        {"µs", 1e-6}, // U+00B5 = micro symbol
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
    class FunctionParseTimeDelta final : public IFunction
    {
    public:
        FunctionParseTimeDelta(const char * name_, ParseTimeDeltaErrorHandling error_handling_)
            : function_name(name_), error_handling(error_handling_) {}

        static FunctionPtr create(ContextPtr, const char * name_, ParseTimeDeltaErrorHandling error_handling_)
        {
            return std::make_shared<FunctionParseTimeDelta>(name_, error_handling_);
        }

        String getName() const override { return function_name; }

        bool isVariadic() const override { return true; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        size_t getNumberOfArguments() const override { return 0; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
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

            FunctionArgumentDescriptors mandatory_args{
                {"timestr", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
            };

            validateFunctionArguments(*this, arguments, mandatory_args);

            DataTypePtr return_type = std::make_shared<DataTypeFloat64>();
            if (error_handling == ParseTimeDeltaErrorHandling::Null)
                return std::make_shared<DataTypeNullable>(return_type);
            return return_type;
        }

        DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
        {
            return std::make_shared<DataTypeFloat64>();
        }

        bool useDefaultImplementationForConstants() const override { return true; }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            auto col_to = ColumnFloat64::create(input_rows_count);
            auto & res_data = col_to->getData();

            ColumnUInt8::MutablePtr col_null_map;
            if (error_handling == ParseTimeDeltaErrorHandling::Null)
                col_null_map = ColumnUInt8::create(input_rows_count, false);

            /// The message is only formatted for the throwing variant, so a recovered row costs no allocation.
            const bool need_message = error_handling == ParseTimeDeltaErrorHandling::Exception;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                Float64OrError result = parse(std::string_view{arguments[0].column->getDataAt(i)}, need_message);

                if (result.has_value())
                {
                    res_data[i] = *result;
                }
                else if (error_handling == ParseTimeDeltaErrorHandling::Exception)
                {
                    throw Exception(
                        PreformattedMessage{
                            std::move(result.error().error_message),
                            result.error().error_pattern,
                            std::move(result.error().error_args)},
                        result.error().error_code);
                }
                else
                {
                    res_data[i] = 0;
                    if (error_handling == ParseTimeDeltaErrorHandling::Null)
                        col_null_map->getData()[i] = 1;
                }
            }

            if (error_handling == ParseTimeDeltaErrorHandling::Null)
                return ColumnNullable::create(std::move(col_to), std::move(col_null_map));
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

    private:
        const char * function_name;
        ParseTimeDeltaErrorHandling error_handling;

/// Reports a parse failure without constructing an Exception, so recovering variants add nothing
/// to system.errors. The pattern is always recorded (it becomes message_format_string); the message
/// text and the formatted args only when throwing, so a recovered row allocates neither.
#define PARSE_TIME_DELTA_ERROR(pattern, ...) \
    do \
    { \
        if (!need_message) \
            return std::unexpected(ParseTimeDeltaError{ErrorCodes::BAD_ARGUMENTS, String{}, pattern, {}}); \
        ParseTimeDeltaErrorArgs error_args; \
        String error_message = tryGetArgsAndFormat(error_args, pattern, __VA_ARGS__); \
        return std::unexpected( \
            ParseTimeDeltaError{ErrorCodes::BAD_ARGUMENTS, std::move(error_message), pattern, std::move(error_args)}); \
    } while (false)

        Float64OrError parse(std::string_view str, bool need_message) const
        {
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
                PARSE_TIME_DELTA_ERROR(
                    "Invalid argument of function {}, no valid characters, str: \"{}\".", getName(), String(str));
            }

            /// last pos character must be character and not be separator or number after ignoring '.' and ' '
            if (!isalpha(str[last_pos]))
            {
                PARSE_TIME_DELTA_ERROR("Invalid argument of function {}, str: \"{}\".", getName(), String(str));
            }

            /// scan spaces at the beginning
            scanSpaces(str, token_tail, last_pos);
            token_front = token_tail;

            while (token_tail <= last_pos)
            {
                /// scan unsigned integer
                if (!scanUnsignedInteger(str, token_tail, last_pos))
                {
                    PARSE_TIME_DELTA_ERROR(
                        "Invalid argument of function {}, number not found, str: \"{}\".", getName(), String(str));
                }

                /// if there is a '.', then scan another integer to get a float number
                if (token_tail <= last_pos && str[token_tail] == '.')
                {
                    token_tail++;
                    if (!scanUnsignedInteger(str, token_tail, last_pos))
                    {
                        PARSE_TIME_DELTA_ERROR(
                            "Invalid argument of function {}, number not found after '.', str: \"{}\".", getName(), String(str));
                    }
                }

                /// convert float/integer string to float
                Float64 base = 0;
                std::string_view base_str = str.substr(token_front, token_tail - token_front);
                auto value = boost::convert<Float64>(base_str, boost::cnv::strtol());
                if (!value.has_value())
                {
                    PARSE_TIME_DELTA_ERROR(
                        "Invalid argument of function {}, can't convert String to Float64: \"{}\".", getName(), String(base_str));
                }
                base = value.get();

                scanSpaces(str, token_tail, last_pos);
                token_front = token_tail;

                /// scan a unit
                if (!scanUnit(str, token_tail, last_pos))
                {
                    PARSE_TIME_DELTA_ERROR(
                        "Invalid argument of function {}, time unit not found, str: \"{}\".", getName(), String(str));
                }

                /// get unit number
                std::string_view unit = str.substr(token_front, token_tail - token_front);
                auto iter = time_unit_to_float.find(unit);
                if (iter == time_unit_to_float.end()) /// not find unit
                {
                    PARSE_TIME_DELTA_ERROR(
                        "Invalid argument of function {}, can't parse the unit: \"{}\".", getName(), unit);
                }
                result += base * iter->second;

                /// scan separator between two tokens
                scanSeparator(str, token_tail, last_pos);
                token_front = token_tail;
            }

            return result;
        }

#undef PARSE_TIME_DELTA_ERROR
    };

}

REGISTER_FUNCTION(ParseTimeDelta)
{
    FunctionDocumentation::Description description = R"(
Parse a sequence of numbers followed by something resembling a time unit.

The time delta string uses these time unit specifications:
- `years`, `year`, `yr`, `y`
- `months`, `month`, `mo`
- `weeks`, `week`, `w`
- `days`, `day`, `d`
- `hours`, `hour`, `hr`, `h`
- `minutes`, `minute`, `min`, `m`
- `seconds`, `second`, `sec`, `s`
- `milliseconds`, `millisecond`, `millisec`, `ms`
- `microseconds`, `microsecond`, `microsec`, `μs`, `µs`, `us`
- `nanoseconds`, `nanosecond`, `nanosec`, `ns`

Multiple time units can be combined with separators (space, `;`, `-`, `+`, `,`, `:`).

The length of years and months are approximations: year is 365 days, month is 30.5 days.

If the function is unable to parse the input value, it throws an exception. Use
[`parseTimeDeltaOrNull`](#parseTimeDeltaOrNull) or [`parseTimeDeltaOrZero`](#parseTimeDeltaOrZero)
to return `NULL` or `0` instead.
    )";
    FunctionDocumentation::Syntax syntax = "parseTimeDelta(timestr)";
    FunctionDocumentation::Arguments arguments = {
        {"timestr", "A sequence of numbers followed by something resembling a time unit.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"The number of seconds.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
SELECT parseTimeDelta('11s+22min')
            )",
            R"(
┌─parseTimeDelta('11s+22min')─┐
│                        1331 │
└─────────────────────────────┘
            )"
        },
        {
            "Complex time units",
            R"(
SELECT parseTimeDelta('1yr2mo')
            )",
            R"(
┌─parseTimeDelta('1yr2mo')─┐
│                 36806400 │
└──────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    FunctionDocumentation::Description description_or_null = R"(
Like [`parseTimeDelta`](#parseTimeDelta), but returns `NULL` instead of throwing when the input
value cannot be parsed. Errors about the call itself (wrong number of arguments, non-`String`
argument type) are still raised; for a `Dynamic` or `Variant` argument this follows
`dynamic_throw_on_type_mismatch` / `variant_throw_on_type_mismatch`, as it does for
`parseTimeDelta` itself.
    )";
    FunctionDocumentation::Syntax syntax_or_null = "parseTimeDeltaOrNull(timestr)";
    FunctionDocumentation::ReturnedValue returned_value_or_null
        = {"The number of seconds, or `NULL` if the input cannot be parsed.", {"Nullable(Float64)"}};
    FunctionDocumentation::Examples examples_or_null = {
        {
            "Usage example",
            R"(
SELECT parseTimeDeltaOrNull('11s+22min'), parseTimeDeltaOrNull('invalid')
            )",
            R"(
┌─parseTimeDeltaOrNull('11s+22min')─┬─parseTimeDeltaOrNull('invalid')─┐
│                              1331 │                            ᴺᵁᴸᴸ │
└───────────────────────────────────┴─────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in_or_null = {26, 8};
    FunctionDocumentation documentation_or_null = {
        description_or_null, syntax_or_null, arguments, {}, returned_value_or_null, examples_or_null, introduced_in_or_null, category};

    FunctionDocumentation::Description description_or_zero = R"(
Like [`parseTimeDelta`](#parseTimeDelta), but returns `0` instead of throwing when the input value
cannot be parsed. Errors about the call itself (wrong number of arguments, non-`String` argument
type) are still raised; for a `Dynamic` or `Variant` argument this follows
`dynamic_throw_on_type_mismatch` / `variant_throw_on_type_mismatch`, as it does for
`parseTimeDelta` itself.
    )";
    FunctionDocumentation::Syntax syntax_or_zero = "parseTimeDeltaOrZero(timestr)";
    FunctionDocumentation::ReturnedValue returned_value_or_zero
        = {"The number of seconds, or `0` if the input cannot be parsed.", {"Float64"}};
    FunctionDocumentation::Examples examples_or_zero = {
        {
            "Usage example",
            R"(
SELECT parseTimeDeltaOrZero('11s+22min'), parseTimeDeltaOrZero('invalid')
            )",
            R"(
┌─parseTimeDeltaOrZero('11s+22min')─┬─parseTimeDeltaOrZero('invalid')─┐
│                              1331 │                               0 │
└───────────────────────────────────┴─────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in_or_zero = {26, 8};
    FunctionDocumentation documentation_or_zero = {
        description_or_zero, syntax_or_zero, arguments, {}, returned_value_or_zero, examples_or_zero, introduced_in_or_zero, category};

    factory.registerFunction(
        "parseTimeDelta",
        [](ContextPtr) { return FunctionParseTimeDelta::create({}, "parseTimeDelta", ParseTimeDeltaErrorHandling::Exception); },
        documentation);
    factory.registerFunction(
        "parseTimeDeltaOrNull",
        [](ContextPtr) { return FunctionParseTimeDelta::create({}, "parseTimeDeltaOrNull", ParseTimeDeltaErrorHandling::Null); },
        documentation_or_null);
    factory.registerFunction(
        "parseTimeDeltaOrZero",
        [](ContextPtr) { return FunctionParseTimeDelta::create({}, "parseTimeDeltaOrZero", ParseTimeDeltaErrorHandling::Zero); },
        documentation_or_zero);
}

}
