#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionTimeSlot = FunctionDateOrDateTimeToDateTimeOrDateTime64<TimeSlotImpl>;

REGISTER_FUNCTION(TimeSlot)
{
    factory.registerFunction<FunctionTimeSlot>();
}

}


