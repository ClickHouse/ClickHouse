#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>

#include <Interpreters/Context.h>

#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>
#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_nonconst_timezone_arguments;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
class ExecutableFunctionToTimeZone : public IExecutableFunction
{
public:
    explicit ExecutableFunctionToTimeZone() = default;

    String getName() const override { return "toTimezone"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        return arguments[0].column;
    }
};

class FunctionBaseToTimeZone : public IFunctionBase
{
public:
    FunctionBaseToTimeZone(
        bool is_constant_timezone_,
        DataTypes argument_types_,
        DataTypePtr return_type_)
        : is_constant_timezone(is_constant_timezone_)
        , argument_types(std::move(argument_types_))
        , return_type(std::move(return_type_)) {}

    String getName() const override { return "toTimezone"; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    const DataTypes & getArgumentTypes() const override
    {
        return argument_types;
    }

    const DataTypePtr & getResultType() const override
    {
        return return_type;
    }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName & /*arguments*/) const override
    {
        return std::make_unique<ExecutableFunctionToTimeZone>();
    }

    bool hasInformationAboutMonotonicity() const override { return is_constant_timezone; }

    Monotonicity getMonotonicityForRange(const IDataType & /*type*/, const Field & /*left*/, const Field & /*right*/) const override
    {
        const bool b = is_constant_timezone;
        return { .is_monotonic = b, .is_positive = b, .is_always_monotonic = b, .is_strict = b };
    }

private:
    bool is_constant_timezone;
    DataTypes argument_types;
    DataTypePtr return_type;
};

/// Just changes time zone information for data type. The calculation is free.
class ToTimeZoneOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "toTimezone";

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    static FunctionOverloadResolverPtr create(ContextPtr context) { return std::make_unique<ToTimeZoneOverloadResolver>(context); }
    explicit ToTimeZoneOverloadResolver(ContextPtr context)
        : allow_nonconst_timezone_arguments(context->getSettingsRef()[Setting::allow_nonconst_timezone_arguments])
    {}

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2",
                getName(), arguments.size());

        const auto which_type = WhichDataType(arguments[0].type);
        if (!which_type.isDateTime() && !which_type.isDateTime64())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}. "
                "Should be DateTime or DateTime64", arguments[0].type->getName(), getName());

        String time_zone_name = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0, allow_nonconst_timezone_arguments);

        if (which_type.isDateTime())
            return std::make_shared<DataTypeDateTime>(time_zone_name);

        const auto * date_time64 = assert_cast<const DataTypeDateTime64 *>(arguments[0].type.get());
        return std::make_shared<DataTypeDateTime64>(date_time64->getScale(), time_zone_name);
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        bool is_constant_timezone = false;
        if (arguments[1].column)
            is_constant_timezone = isColumnConst(*arguments[1].column);

        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        return std::make_unique<FunctionBaseToTimeZone>(is_constant_timezone, data_types, result_type);
    }
private:
    const bool allow_nonconst_timezone_arguments;
};

}

REGISTER_FUNCTION(ToTimeZone)
{
    FunctionDocumentation::Description description = R"(
Converts a `DateTime` or `DateTime64` to the specified time zone.
The internal value (number of unix seconds) of the data doesn't change.
Only the value's time zone attribute and the value's string representation changes.
        )";
    FunctionDocumentation::Syntax syntax = "toTimezone(datetime, timezone)";
    FunctionDocumentation::Arguments arguments =
    {
        {"date", "The value to convert.", {"DateTime", "DateTime64"}},
        {"timezone", "The target time zone name.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the same timestamp as the input, but with the specified time zone", {"DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", R"(
SELECT toDateTime('2019-01-01 00:00:00', 'UTC') AS time_utc,
toTypeName(time_utc) AS type_utc,
toInt32(time_utc) AS int32utc,
toTimezone(time_utc, 'Asia/Yekaterinburg') AS time_yekat,
toTypeName(time_yekat) AS type_yekat,
toInt32(time_yekat) AS int32yekat,
toTimezone(time_utc, 'US/Samoa') AS time_samoa,
toTypeName(time_samoa) AS type_samoa,
toInt32(time_samoa) AS int32samoa
FORMAT Vertical;
        )",
        R"(
Row 1:
──────
time_utc:   2019-01-01 00:00:00
type_utc:   DateTime('UTC')
int32utc:   1546300800
time_yekat: 2019-01-01 05:00:00
type_yekat: DateTime('Asia/Yekaterinburg')
int32yekat: 1546300800
time_samoa: 2018-12-31 13:00:00
type_samoa: DateTime('US/Samoa')
int32samoa: 1546300800
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<ToTimeZoneOverloadResolver>(documentation);
    factory.registerAlias("toTimeZone", "toTimezone");
}

}
