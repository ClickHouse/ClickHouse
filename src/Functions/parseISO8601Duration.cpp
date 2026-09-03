#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/StringUtils.h>

#include <cmath>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    /** Parses an ISO 8601 duration string into a number of seconds.
      *
      * SELECT parseISO8601Duration('PT1M');      -- 60
      * SELECT parseISO8601Duration('PT1H30M');   -- 5400
      * SELECT parseISO8601Duration('P1DT12H');   -- 129600
      * SELECT parseISO8601Duration('PT1.5S');    -- 1.5
      * SELECT parseISO8601Duration('P2W');       -- 1209600
      *
      * The year and month designators are deliberately rejected. Neither has a fixed length in
      * seconds, so converting them requires either a reference date or an arbitrary average, and
      * every choice of average disagrees with every other one. This mirrors java.time.Duration,
      * which parses only PnDTnHnMn.nS, and Temporal.Duration.total, which refuses calendar units
      * without a relativeTo reference. XML Schema, which defines the format, likewise orders
      * durations only partially for this reason.
      */
    class FunctionParseISO8601Duration final : public IFunction
    {
    public:
        static constexpr auto name = "parseISO8601Duration";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionParseISO8601Duration>(); }

        String getName() const override { return name; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        size_t getNumberOfArguments() const override { return 1; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
        {
            /// A literal `NULL` carries no argument type to validate, and the result is `NULL` anyway.
            if (!arguments.empty() && arguments[0].type->onlyNull())
                return makeNullable(std::make_shared<DataTypeFloat64>());

            ColumnsWithTypeAndName arguments_without_nullable = arguments;
            for (auto & argument : arguments_without_nullable)
                argument.type = removeNullable(argument.type);

            FunctionArgumentDescriptors mandatory_args{
                {"duration", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
            };

            validateFunctionArguments(*this, arguments_without_nullable, mandatory_args);

            DataTypePtr result_type = std::make_shared<DataTypeFloat64>();
            if (arguments[0].type->isNullable())
                result_type = makeNullable(result_type);
            return result_type;
        }

        DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
        {
            return std::make_shared<DataTypeFloat64>();
        }

        bool useDefaultImplementationForConstants() const override { return true; }

        /// The default implementation applies the function to the whole dictionary, which always holds
        /// the default empty string whether or not any row references it, and an empty string is not a
        /// duration.
        bool canBeExecutedOnDefaultArguments() const override { return false; }

        /// The default implementation would apply the function to the values sitting under the null
        /// map. Those are the nested column's defaults, and parsing them would throw on rows whose
        /// result is discarded anyway.
        bool useDefaultImplementationForNulls() const override { return false; }

        /// The `Dynamic` and `Variant` adaptors default to `useDefaultImplementationForNulls`, so
        /// turning that off above would disable them as a side effect and leave
        /// `getReturnTypeForDefaultImplementationForDynamic` unreachable. They dispatch on the concrete
        /// type held in the column and deal with null rows themselves, so keep them enabled.
        bool useDefaultImplementationForDynamic() const override { return true; }
        bool useDefaultImplementationForVariant() const override { return true; }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            auto col_to = ColumnFloat64::create();
            auto & res_data = col_to->getData();
            res_data.resize(input_rows_count);

            if (const auto * col_nullable = checkAndGetColumn<ColumnNullable>(arguments[0].column.get()))
            {
                const auto & nested_column = col_nullable->getNestedColumn();
                const auto & null_map = col_nullable->getNullMapData();

                auto res_null_map = ColumnUInt8::create(input_rows_count);
                auto & res_null_map_data = res_null_map->getData();

                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    res_null_map_data[i] = null_map[i];
                    /// Whatever the nested column holds under a null is not an argument the caller
                    /// passed, so it must not be parsed and must not decide whether the query fails.
                    res_data[i] = null_map[i] ? 0 : parseDuration(std::string_view{nested_column.getDataAt(i)});
                }

                return ColumnNullable::create(std::move(col_to), std::move(res_null_map));
            }

            for (size_t i = 0; i < input_rows_count; ++i)
                res_data[i] = parseDuration(std::string_view{arguments[0].column->getDataAt(i)});

            return col_to;
        }

    private:
        [[noreturn]] void invalid(std::string_view str, std::string_view reason) const
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Invalid argument of function {}: {}, str: \"{}\".", getName(), reason, String(str));
        }

        Float64 parseDuration(std::string_view str) const
        {
            if (str.empty())
                invalid(str, "the duration is empty");

            if (str[0] != 'P')
                invalid(str, "a duration must start with 'P'");

            size_t pos = 1;
            bool after_t = false;
            /// Designators must appear in canonical order and at most once. The rank of the last one
            /// accepted in the current section guards both rules at the same time.
            size_t date_rank = 0;
            size_t time_rank = 0;
            size_t components = 0;
            size_t time_components = 0;
            Float64 result = 0;

            while (pos < str.length())
            {
                if (str[pos] == 'T')
                {
                    if (after_t)
                        invalid(str, "'T' appears more than once");
                    after_t = true;
                    ++pos;
                    continue;
                }

                const size_t number_begin = pos;
                while (pos < str.length() && isNumericASCII(str[pos]))
                    ++pos;

                if (pos == number_begin)
                    invalid(str, fmt::format("expected a number at position {}", number_begin));

                Float64 value = 0;
                for (size_t j = number_begin; j < pos; ++j)
                    value = value * 10 + (str[j] - '0');

                if (pos < str.length() && str[pos] == '.')
                {
                    ++pos;
                    const size_t fraction_begin = pos;
                    Float64 scale = 0.1;
                    while (pos < str.length() && isNumericASCII(str[pos]))
                    {
                        value += (str[pos] - '0') * scale;
                        scale *= 0.1;
                        ++pos;
                    }

                    if (pos == fraction_begin)
                        invalid(str, "expected digits after the decimal point");
                }

                if (!std::isfinite(value))
                    invalid(str, "the numeric value of a component is too large to represent");

                if (pos == str.length())
                    invalid(str, "a number is not followed by a unit designator");

                const char designator = str[pos];
                ++pos;

                if (!after_t)
                {
                    if (designator == 'Y' || designator == 'M')
                        invalid(
                            str,
                            fmt::format(
                                "the '{}' designator is not supported because {} have no fixed length in seconds; "
                                "use weeks, days, hours, minutes or seconds, or resolve the duration against a "
                                "reference date before converting it",
                                designator,
                                designator == 'Y' ? "years" : "months"));

                    switch (designator)
                    {
                        case 'W':
                            checkOrder(str, date_rank, 1, designator);
                            result += value * 7 * 24 * 3600;
                            break;
                        case 'D':
                            checkOrder(str, date_rank, 2, designator);
                            result += value * 24 * 3600;
                            break;
                        default:
                            invalid(str, fmt::format("unexpected designator '{}' before 'T'", designator));
                    }
                }
                else
                {
                    switch (designator)
                    {
                        case 'H':
                            checkOrder(str, time_rank, 1, designator);
                            result += value * 3600;
                            break;
                        case 'M':
                            checkOrder(str, time_rank, 2, designator);
                            result += value * 60;
                            break;
                        case 'S':
                            checkOrder(str, time_rank, 3, designator);
                            result += value;
                            break;
                        default:
                            invalid(str, fmt::format("unexpected designator '{}' after 'T'", designator));
                    }
                    ++time_components;
                }

                ++components;
            }

            if (components == 0)
                invalid(str, "a duration must contain at least one component");

            if (after_t && time_components == 0)
                invalid(str, "'T' must be followed by at least one time component");

            /// A component can be finite on its own and still overflow once scaled to seconds,
            /// so the total is checked separately from the individual values.
            if (!std::isfinite(result))
                invalid(str, "the duration is too large to represent in seconds");

            return result;
        }

        void checkOrder(std::string_view str, size_t & rank, size_t new_rank, char designator) const
        {
            if (rank >= new_rank)
                invalid(str, fmt::format("the '{}' designator is repeated or out of order", designator));
            rank = new_rank;
        }
    };

}

REGISTER_FUNCTION(ParseISO8601Duration)
{
    FunctionDocumentation::Description description = R"(
Parses an [ISO 8601 duration](https://en.wikipedia.org/wiki/ISO_8601#Durations) string and returns the number of seconds.

The duration starts with `P`, followed by optional date components, and an optional time section
introduced by `T`:

- `W` - weeks
- `D` - days
- `T` - starts the time section
- `H` - hours
- `M` - minutes, only after `T`
- `S` - seconds

Any component may carry a fractional part, not only the lowest-order one as ISO 8601 requires, so
`PT0.5H` is valid and returns 1800.

Designators must appear in the order above and each may appear at most once. The week designator may
be combined with the others, unlike in ISO 8601:2004 where it is exclusive.

The year (`Y`) designator and the month (`M`) designator before `T` are rejected, because neither a
year nor a month has a fixed length in seconds. Convert such durations against a reference date
instead.

Two forms that the standard and its extensions allow are not accepted:

- a comma as the decimal separator, as in `PT1,5S` - use a full stop
- a leading sign, as in `-PT1S`, which comes from RFC 3339 and XML Schema rather than the core grammar

A value that does not fit into [Float64](/reference/data-types/float) is rejected rather than
returned as infinity.
    )";
    FunctionDocumentation::Syntax syntax = "parseISO8601Duration(duration)";
    FunctionDocumentation::Arguments arguments = {
        {"duration", "An ISO 8601 duration string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"The number of seconds.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
SELECT parseISO8601Duration('PT1H30M')
            )",
            R"(
┌─parseISO8601Duration('PT1H30M')─┐
│                            5400 │
└─────────────────────────────────┘
            )"
        },
        {
            "Fractional seconds",
            R"(
SELECT parseISO8601Duration('P1DT12H30M5.5S')
            )",
            R"(
┌─parseISO8601Duration('P1DT12H30M5.5S')─┐
│                               131405.5 │
└────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionParseISO8601Duration>(documentation);
}

}
