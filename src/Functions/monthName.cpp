#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <IO/WriteHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{

namespace
{

bool isDateOrDateTime(const IDataType & type)
{
    return isDate(type) || isDateTime(type) || isDateTime64(type);
}

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
        FunctionArgumentDescriptors mandatory_args{
            {"datetime", isDateOrDateTime, nullptr, "Date or DateTime"}
        };

        validateFunctionArguments(*this, arguments, mandatory_args);
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
    FunctionDocumentation::Description description = R"(
Returns the name of the month as a string from a date or date with time value.
    )";
    FunctionDocumentation::Syntax syntax = R"(
monthName(datetime)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "Date or date with time.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the name of the month.", {"String"}};
    FunctionDocumentation::Examples examples = {
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
    FunctionDocumentation::IntroducedIn introduced_in = {22, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMonthName>(documentation, FunctionFactory::Case::Insensitive);
}

}
