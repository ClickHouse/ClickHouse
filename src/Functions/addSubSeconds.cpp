#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddNanoseconds = FunctionDateOrDateTimeAddInterval<AddNanosecondsImpl>;
void registerFunctionAddNanoseconds(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddNanoseconds>();
};

using FunctionAddMicroseconds = FunctionDateOrDateTimeAddInterval<AddMicrosecondsImpl>;
void registerFunctionAddMicroseconds(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddMicroseconds>();
};

using FunctionAddMilliseconds = FunctionDateOrDateTimeAddInterval<AddMillisecondsImpl>;
void registerFunctionAddMilliseconds(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddMilliseconds>();
};

}


