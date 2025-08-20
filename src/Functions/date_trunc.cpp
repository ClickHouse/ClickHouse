#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeInterval.h>
#include <Formats/FormatSettings.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace Setting
{
    extern const SettingsUInt64 function_date_trunc_return_type_behavior;
}

namespace
{

class FunctionDateTrunc : public IFunction
{
public:
    static constexpr auto name = "dateTrunc";

    explicit FunctionDateTrunc(ContextPtr context_) : context(context_) {}

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionDateTrunc>(context); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        /// The first argument is a constant string with the name of datepart.

        enum ResultType
        {
            Date,
            Date32,
            DateTime,
            DateTime64,
        };
        ResultType result_type;

        String datepart_param;
        auto check_first_argument = [&] {
            const ColumnConst * datepart_column = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
            if (!datepart_column)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be constant string: "
                    "name of datepart", getName());

            datepart_param = Poco::toLower(datepart_column->getValue<String>());
            if (datepart_param.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "First argument (name of datepart) for function {} cannot be empty",
                    getName());

            if (!IntervalKind::tryParseString(datepart_param, datepart_kind))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} doesn't look like datepart name in {}", datepart_param, getName());

            if ((datepart_kind == IntervalKind::Kind::Year) || (datepart_kind == IntervalKind::Kind::Quarter)
                || (datepart_kind == IntervalKind::Kind::Month) || (datepart_kind == IntervalKind::Kind::Week))
                result_type = ResultType::Date;
            else if ((datepart_kind == IntervalKind::Kind::Day) || (datepart_kind == IntervalKind::Kind::Hour)
                    || (datepart_kind == IntervalKind::Kind::Minute) || (datepart_kind == IntervalKind::Kind::Second))
                result_type = ResultType::DateTime;
            else
                result_type = ResultType::DateTime64;
        };

        bool second_argument_is_date = false;
        auto check_second_argument = [&] {
            if (!isDateOrDate32(arguments[1].type) && !isDateTime(arguments[1].type) && !isDateTime64(arguments[1].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 2nd argument of function {}. "
                    "Should be a date or a date with time", arguments[1].type->getName(), getName());

            second_argument_is_date = isDateOrDate32(arguments[1].type);

            if (second_argument_is_date && ((datepart_kind == IntervalKind::Kind::Hour)
                || (datepart_kind == IntervalKind::Kind::Minute) || (datepart_kind == IntervalKind::Kind::Second)))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for function {}", arguments[1].type->getName(), getName());

            /// If we have a DateTime64 or Date32 as an input, it can be negative.
            /// In this case, we should provide the corresponding return type, which supports negative values.
            /// For compatibility, we do it under a setting.
            if ((isDateTime64(arguments[1].type) || isDate32(arguments[1].type)) && context->getSettingsRef()[Setting::function_date_trunc_return_type_behavior] == 0)
            {
                if (result_type == ResultType::Date)
                    result_type = Date32;
                else if (result_type == ResultType::DateTime)
                    result_type = DateTime64;
            }
        };

        auto check_timezone_argument = [&] {
            if (!WhichDataType(arguments[2].type).isString())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}. "
                    "This argument is optional and must be a constant string with timezone name",
                    arguments[2].type->getName(), getName());

            if (second_argument_is_date && result_type == ResultType::Date)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "The timezone argument of function {} with datepart '{}' "
                                "is allowed only when the 2nd argument has the type DateTime",
                                getName(), datepart_param);
        };

        if (arguments.size() == 2)
        {
            check_first_argument();
            check_second_argument();
        }
        else if (arguments.size() == 3)
        {
            check_first_argument();
            check_second_argument();
            check_timezone_argument();
        }
        else
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2 or 3",
                getName(), arguments.size());
        }

        if (result_type == ResultType::Date)
            return std::make_shared<DataTypeDate>();
        if (result_type == ResultType::Date32)
            return std::make_shared<DataTypeDate32>();
        if (result_type == ResultType::DateTime)
            return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 1, false));

        size_t scale = 0;
        if (datepart_kind == IntervalKind::Kind::Millisecond)
            scale = 3;
        else if (datepart_kind == IntervalKind::Kind::Microsecond)
            scale = 6;
        else if (datepart_kind == IntervalKind::Kind::Nanosecond)
            scale = 9;
        return std::make_shared<DataTypeDateTime64>(scale, extractTimeZoneNameFromFunctionArguments(arguments, 2, 1, false));
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 2}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnsWithTypeAndName temp_columns(arguments.size());
        temp_columns[0] = arguments[1];

        const UInt16 interval_value = 1;
        const ColumnPtr interval_column = ColumnConst::create(ColumnInt64::create(1, interval_value), input_rows_count);
        temp_columns[1] = {interval_column, std::make_shared<DataTypeInterval>(datepart_kind), ""};

        auto to_start_of_interval = FunctionFactory::instance().get("toStartOfInterval", context);

        if (arguments.size() == 2)
            return to_start_of_interval->build(temp_columns)->execute(temp_columns, result_type, input_rows_count, /* dry_run = */ false);

        temp_columns[2] = arguments[2];
        return to_start_of_interval->build(temp_columns)->execute(temp_columns, result_type, input_rows_count, /* dry_run = */ false);
    }

    bool hasInformationAboutMonotonicity() const override
    {
        return true;
    }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        return { .is_monotonic = true, .is_always_monotonic = true };
    }

private:
    ContextPtr context;
    mutable IntervalKind::Kind datepart_kind = IntervalKind::Kind::Second;
};

}


REGISTER_FUNCTION(DateTrunc)
{
    FunctionDocumentation::Description description = R"(
Truncates a date and time value to the specified part of the date.
    )";
    FunctionDocumentation::Syntax syntax = R"(
dateTrunc(unit, datetime[, timezone])
    )";
    FunctionDocumentation::Arguments arguments = {
{"unit",
R"(
The type of interval to truncate the result. `unit` argument is case-insensitive.
| Unit         | Compatibility                   |
|--------------|---------------------------------|
| `nanosecond` | Compatible only with DateTime64 |
| `microsecond`| Compatible only with DateTime64 |
| `millisecond`| Compatible only with DateTime64 |
| `second`     |                                 |
| `minute`     |                                 |
| `hour`       |                                 |
| `day`        |                                 |
| `week`       |                                 |
| `month`      |                                 |
| `quarter`    |                                 |
| `year`       |                                 |
)", {"String"}},
{"datetime", "Date and time.", {"Date", "Date32", "DateTime", "DateTime64"}},
{"timezone", "Optional. Timezone name for the returned datetime. If not specified, the function uses the timezone of the `datetime` parameter.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
      R"(
Returns the truncated date and time value.

| Unit Argument               | `datetime` Argument                   | Return Type                                                                            |
|-----------------------------|---------------------------------------|----------------------------------------------------------------------------------------|
| Year, Quarter, Month, Week  | `Date32` or `DateTime64` or `Date` or `DateTime` | [`Date32`](../data-types/date32.md) or [`Date`](../data-types/date.md)                 |
| Day, Hour, Minute, Second   | `Date32`, `DateTime64`, `Date`, or `DateTime` | [`DateTime64`](../data-types/datetime64.md) or [`DateTime`](../data-types/datetime.md) |
| Millisecond, Microsecond,   | Any                                   | [`DateTime64`](../data-types/datetime64.md)                                            |
| Nanosecond                  |                                       | with scale 3, 6, or 9                                                                  |
      )", {}};
    FunctionDocumentation::Examples examples = {
        {"Truncate without timezone", R"(
SELECT now(), dateTrunc('hour', now());
        )",
        R"(
┌───────────────now()─┬─dateTrunc('hour', now())──┐
│ 2020-09-28 10:40:45 │       2020-09-28 10:00:00 │
└─────────────────────┴───────────────────────────┘
        )"},
        {"Truncate with specified timezone", R"(
SELECT now(), dateTrunc('hour', now(), 'Asia/Istanbul');
        )",
        R"(
┌───────────────now()─┬─dateTrunc('hour', now(), 'Asia/Istanbul')──┐
│ 2020-09-28 10:46:26 │                        2020-09-28 13:00:00 │
└─────────────────────┴────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionDateTrunc>(documentation);

    /// Compatibility alias.
    factory.registerAlias("DATE_TRUNC", "dateTrunc", FunctionFactory::Case::Insensitive);
}

}
