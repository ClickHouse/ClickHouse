#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToMonthNumSinceEpoch = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToMonthNumSinceEpochImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToMonthNumSinceEpoch)
{
    FunctionDocumentation::Description description = R"(Returns amount of months passed from year 1970)";
    FunctionDocumentation::Syntax syntax = "toMonthNumSinceEpoch(date)";
    FunctionDocumentation::Arguments arguments = {{"date", "Date, DateTime or DateTime64"}};
    FunctionDocumentation::ReturnedValue returned_value = "Positive integer";
    FunctionDocumentation::Examples examples = {{"Example", "SELECT toMonthNumSinceEpoch(toDate('2024-10-01'))", "657"}};
    FunctionDocumentation::Category category = {"DateTime"};

    factory.registerFunction<FunctionToMonthNumSinceEpoch>({description, syntax, arguments, returned_value, examples, category});
}

}
