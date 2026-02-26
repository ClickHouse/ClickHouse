#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToYearNumSinceEpoch = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToYearNumSinceEpochImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToYearNumSinceEpoch)
{
    FunctionDocumentation::Description description = R"(Returns amount of years passed from year 1970)";
    FunctionDocumentation::Syntax syntax = "toYearNumSinceEpoch(date)";
    FunctionDocumentation::Arguments arguments = {{"date", "Date, DateTime or DateTime64"}};
    FunctionDocumentation::ReturnedValue returned_value = "Positive integer";
    FunctionDocumentation::Examples examples = {{"Example", "SELECT toYearNumSinceEpoch(toDate('2024-10-01'))", "54"}};
    FunctionDocumentation::Category category = {"DateTime"};

    factory.registerFunction<FunctionToYearNumSinceEpoch>({description, syntax, arguments, returned_value, examples, category});
}

}
