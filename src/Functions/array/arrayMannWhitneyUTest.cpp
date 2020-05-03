#include <Functions/FunctionFactory.h>
#include <Functions/array/arrayMannWhitneyUTest.h>


namespace DB
{
void registerFunctionsArrayMannWhitneyUTest(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayMannWhitneyUTestTwoSided>();
    factory.registerFunction<FunctionArrayMannWhitneyUTestGreater>();
    factory.registerFunction<FunctionArrayMannWhitneyUTestLess>();
}

}

