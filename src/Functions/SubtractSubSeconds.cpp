#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

//#define SUBSECOND(SUBSECOND_KIND) \
//    using FunctionAdd##SUBSECOND_KIND##seconds = FunctionDateOrDateTimeAddInterval<Add##SUBSECOND_KIND##secondsImpl>;\
//    void registerFunctionAdd##SUBSECOND_KIND##seconds(FunctionFactory & factory) \
//    { \
//        factory.registerFunction<FunctionAdd##SUBSECOND_KIND##seconds>(); \
//    };
//SUBSECOND(Nano)
//SUBSECOND(Micro)
//SUBSECOND(Milli)
//#undef SUBSECOND

using FunctionSubtractNanoseconds = FunctionDateOrDateTimeAddInterval<SubtractNanosecondsImpl>;
void registerFunctionSubtractNanoseconds(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubtractNanoseconds>();
};

using FunctionSubtractMicroseconds = FunctionDateOrDateTimeAddInterval<SubtractMicrosecondsImpl>;
void registerFunctionSubtractMicroseconds(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubtractMicroseconds>();
};

using FunctionSubtractMilliseconds = FunctionDateOrDateTimeAddInterval<SubtractMillisecondsImpl>;
void registerFunctionSubtractMilliseconds(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubtractMilliseconds>();
};

}


