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


