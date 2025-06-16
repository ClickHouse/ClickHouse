#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Common/DateLUTImpl.h>
#include <Core/Field.h>

namespace DB
{

namespace
{


/** timezoneOf(x) - get the name of the timezone of DateTime data type.
  * Example: Pacific/Pitcairn.
  */
class FunctionTimezoneOf : public IFunction
{
public:
    static constexpr auto name = "timezoneOf";
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr) { return std::make_unique<FunctionTimezoneOf>(); }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & types) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"datetime", &isDateTimeOrDateTime64, nullptr, "DateTime or DateTime64"},
        };

        validateFunctionArguments(*this, types, mandatory_args);

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        DataTypePtr type_no_nullable = removeNullable(arguments[0].type);

        return DataTypeString().createColumnConst(input_rows_count,
            dynamic_cast<const TimezoneMixin &>(*type_no_nullable).getTimeZone().getTimeZone());
    }

    ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        DataTypePtr type_no_nullable = removeNullable(arguments[0].type);

        return DataTypeString().createColumnConst(1,
            dynamic_cast<const TimezoneMixin &>(*type_no_nullable).getTimeZone().getTimeZone());
    }

private:
    static bool isDateTimeOrDateTime64(const IDataType & type)
    {
        DataTypePtr type_no_nullable = removeNullable(type.getPtr());

        return WhichDataType(*type_no_nullable).isDateTimeOrDateTime64();
    }
};

}

REGISTER_FUNCTION(TimezoneOf)
{
    FunctionDocumentation::Description description_timezone_of = R"(
Returns the timezone name of a [`DateTime`](/sql-reference/data-types/datetime) or [`DateTime64`](/sql-reference/data-types/datetime64) value.
    )";
    FunctionDocumentation::Syntax syntax_timezone_of = "timeZoneOf(datetime)";
    FunctionDocumentation::Arguments arguments_timezone_of = {
        {"datetime", "A value of type [`DateTime`](/sql-reference/data-types/datetime) or [`DateTime64`](/sql-reference/data-types/datetime64)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_timezone_of = "Returns the timezone name for `datetime`. [`String`](/sql-reference/data-types/string).";
    FunctionDocumentation::Examples examples_timezone_of = {
        {"Usage example", R"(
SELECT timezoneOf(now());
        )",
        R"(
┌─timezoneOf(now())─┐
│ Europe/Amsterdam  │
└───────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_timezone_of = {21, 4};
    FunctionDocumentation::Category category_timezone_of = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_timezone_of = {description_timezone_of, syntax_timezone_of, arguments_timezone_of, returned_value_timezone_of, examples_timezone_of, introduced_in_timezone_of, category_timezone_of};

    factory.registerFunction<FunctionTimezoneOf>(documentation_timezone_of);
    factory.registerAlias("timeZoneOf", "timezoneOf");
}

}
