#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeInterval.h>
#include <Formats/FormatSettings.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int BAD_ARGUMENTS;
}

namespace
{

class FunctionDateTrunc : public IFunction
{
public:
    static constexpr auto name = "dateTrunc";

    explicit FunctionDateTrunc(ContextPtr context_) : context(context_) { }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionDateTrunc>(context); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        enum ResultType
        {
            Date,
            DateTime,
            DateTime64,
        };
        ResultType result_type;

        String datepart_param;
        const DataTypeInterval * interval_type = nullptr;

        auto check_first_argument = [&]
        {
            interval_type = checkAndGetDataType<DataTypeInterval>(arguments[0].type.get());

            if (interval_type)
            {
                const DataTypePtr & type_arg2 = arguments[0].type;

                interval_type = checkAndGetDataType<DataTypeInterval>(type_arg2.get());
                switch (interval_type->getKind()) // NOLINT(bugprone-switch-missing-default-case)
                {
                    case IntervalKind::Kind::Nanosecond:
                    case IntervalKind::Kind::Microsecond:
                    case IntervalKind::Kind::Millisecond:
                        result_type = ResultType::DateTime64;
                        break;
                    case IntervalKind::Kind::Second:
                    case IntervalKind::Kind::Minute:
                    case IntervalKind::Kind::Hour:
                    case IntervalKind::Kind::Day:
                        result_type = ResultType::DateTime;
                        break;
                    case IntervalKind::Kind::Week:
                    case IntervalKind::Kind::Month:
                    case IntervalKind::Kind::Quarter:
                    case IntervalKind::Kind::Year:
                        result_type = ResultType::Date;
                        break;
                }
            }
            else
            {
                const ColumnConst * datepart_column = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
                if (!datepart_column)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "First argument for function {} must be constant string: name of datepart or a time interval",
                        getName());
                datepart_param = Poco::toLower(datepart_column->getValue<String>());
                if (datepart_param.empty())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "First argument (name of datepart or a time interval) for function {} cannot be empty",
                        getName());

                if (!IntervalKind::tryParseString(datepart_param, datepart_kind))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "{} doesn't look like datepart name or a time interval in {}",
                        datepart_param,
                        getName());

                if ((datepart_kind == IntervalKind::Kind::Year) || (datepart_kind == IntervalKind::Kind::Quarter)
                    || (datepart_kind == IntervalKind::Kind::Month) || (datepart_kind == IntervalKind::Kind::Week))
                    result_type = ResultType::Date;
                else if (
                    (datepart_kind == IntervalKind::Kind::Day) || (datepart_kind == IntervalKind::Kind::Hour)
                    || (datepart_kind == IntervalKind::Kind::Minute) || (datepart_kind == IntervalKind::Kind::Second))
                    result_type = ResultType::DateTime;
                else
                    result_type = ResultType::DateTime64;
            }
        };

        bool second_argument_is_date = false;
        auto check_second_argument = [&]
        {
            if (!isDate(arguments[1].type) && !isDateTime(arguments[1].type) && !isDateTime64(arguments[1].type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of 2nd argument of function {}. "
                    "Should be a date or a date with time",
                    arguments[1].type->getName(),
                    getName());

            second_argument_is_date = isDate(arguments[1].type);
            if (!interval_type && second_argument_is_date
                && ((datepart_kind == IntervalKind::Kind::Hour) || (datepart_kind == IntervalKind::Kind::Minute)
                    || (datepart_kind == IntervalKind::Kind::Second)))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type Date of argument for function {}", getName());
        };

        auto check_timezone_argument = [&]
        {
            if (!WhichDataType(arguments[2].type).isString())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of function {}. "
                    "This argument is optional and must be a constant string with timezone name",
                    arguments[2].type->getName(),
                    getName());

            if (second_argument_is_date && result_type == ResultType::Date)
            {
                if (interval_type)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "The timezone argument of function {} with interval type {} is allowed only when the 2nd argument has type "
                        "DateTime or DateTimt64",
                        getName(),
                        interval_type->getKind().toString());
                else
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "The timezone argument of function {} with datepart '{}' "
                        "is allowed only when the 2nd argument has the type DateTime",
                        getName(),
                        datepart_param);
            }
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
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2 or 3",
                getName(),
                arguments.size());
        }

        if (result_type == ResultType::Date)
            return std::make_shared<DataTypeDate>();
        else if (result_type == ResultType::DateTime)
            return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 1, false));
        else
        {
            size_t scale;
            if (datepart_kind == IntervalKind::Kind::Millisecond)
                scale = 3;
            else if (datepart_kind == IntervalKind::Kind::Microsecond)
                scale = 6;
            else if (datepart_kind == IntervalKind::Kind::Nanosecond)
                scale = 9;
            return std::make_shared<DataTypeDateTime64>(scale, extractTimeZoneNameFromFunctionArguments(arguments, 2, 1, false));
        }
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 2}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnsWithTypeAndName temp_columns(arguments.size());
        temp_columns[0] = arguments[1];

        if (checkAndGetDataType<DataTypeInterval>(arguments[0].type.get()))
        {
            temp_columns[1] = arguments[0];
        }
        else
        {
            const UInt16 interval_value = 1;
            ColumnPtr temp_interval_column = ColumnConst::create(ColumnInt64::create(1, interval_value), input_rows_count);
            temp_columns[1] = {temp_interval_column, std::make_shared<DataTypeInterval>(datepart_kind), ""};
        }

        auto to_start_of_interval = FunctionFactory::instance().get("toStartOfInterval", context);

        if (arguments.size() == 2)
            return to_start_of_interval->build(temp_columns)->execute(temp_columns, result_type, input_rows_count);

        temp_columns[2] = arguments[2];
        return to_start_of_interval->build(temp_columns)->execute(temp_columns, result_type, input_rows_count);
    }

    bool hasInformationAboutMonotonicity() const override { return true; }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        return {.is_monotonic = true, .is_always_monotonic = true};
    }

private:
    ContextPtr context;
    mutable IntervalKind::Kind datepart_kind = IntervalKind::Kind::Second;
};

}


REGISTER_FUNCTION(DateTrunc)
{
    factory.registerFunction<FunctionDateTrunc>();

    /// Compatibility alias.
    factory.registerAlias("DATE_TRUNC", "dateTrunc", FunctionFactory::CaseInsensitive);
}

}
