#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <IO/WriteHelpers.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionMonthName : public IFunction
{
public:
    static constexpr auto name = "monthName";

    static constexpr auto month_str = "month";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionMonthName>(context); }

    explicit FunctionMonthName(ContextPtr context_)
        : function_resolver(FunctionFactory::instance().get("dateName", std::move(context_)))
        {}

    String getName() const override { return name; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 1",
                getName(),
                arguments.size());

        WhichDataType argument_type(arguments[0].type);
        if (!argument_type.isDate() && !argument_type.isDateTime() && !argument_type.isDateTime64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type of argument of function {}, should be Date, DateTime or DateTime64",
                getName());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        size_t input_rows_count) const override
    {
        auto month_column = DataTypeString().createColumnConst(arguments[0].column->size(), month_str);
        ColumnsWithTypeAndName temporary_columns
        {
            ColumnWithTypeAndName(month_column, std::make_shared<DataTypeString>(), ""),
            arguments[0]
        };

        auto date_name_func = function_resolver->build(temporary_columns);
        return date_name_func->execute(temporary_columns, result_type, input_rows_count, /* dry_run = */ false);
    }

private:
    FunctionOverloadResolverPtr function_resolver;
};

REGISTER_FUNCTION(MonthName)
{
    FunctionDocumentation::Description description_monthName = R"(
Returns the name of the month as a string from a date or date with time value.
    )";
    FunctionDocumentation::Syntax syntax_monthName = R"(
monthName(datetime)
    )";
    FunctionDocumentation::Arguments arguments_monthName = {
        {"datetime", "Date or date with time. [`Date`](../data-types/date.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_monthName = "Returns the name of the month. [`String`](../data-types/string.md).";
    FunctionDocumentation::Examples examples_monthName = {
        {"Get month name from date", R"(
WITH toDateTime('2021-04-14 11:22:33') AS date_value
SELECT monthName(date_value)
        )",
        R"(
┌─monthName(date_value)─┐
│ April                 │
└───────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_monthName = {22, 1};
    FunctionDocumentation::Category category_monthName = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_monthName = {
        description_monthName,
        syntax_monthName,
        arguments_monthName,
        returned_value_monthName,
        examples_monthName,
        introduced_in_monthName,
        category_monthName
    };

    factory.registerFunction<FunctionMonthName>(documentation_monthName, FunctionFactory::Case::Insensitive);
}

}
