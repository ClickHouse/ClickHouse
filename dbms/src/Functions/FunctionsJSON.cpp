#include <Functions/FunctionsJSON.h>
#include <Functions/DummyJSONParser.h>
#include <Functions/SimdJSONParser.h>


namespace DB
{

void registerFunctionsJSON(FunctionFactory & factory)
{
#if USE_SIMDJSON
    if (__builtin_cpu_supports("avx2"))
    {
        registerFunctionsJSONTemplate<SimdJSONParser>(factory);
        return;
    }
#endif
    registerFunctionsJSONTemplate<DummyJSONParser>(factory);
}

}
