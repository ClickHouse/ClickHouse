#include <Functions/FunctionsJSON.h>
#include <Functions/DummyJSONParser.h>
#include <Functions/SimdJSONParser.h>
#include <Functions/RapidJSONParser.h>
#include <Common/CpuId.h>


namespace DB
{

void registerFunctionsJSON(FunctionFactory & factory)
{
#if USE_SIMDJSON
    if (Cpu::CpuFlagsCache::have_AVX2)
    {
        registerFunctionsJSONTemplate<SimdJSONParser>(factory);
        return;
    }
#endif

#if USE_RAPIDJSON
    registerFunctionsJSONTemplate<RapidJSONParser>(factory);
#else
    registerFunctionsJSONTemplate<DummyJSONParser>(factory);
#endif
}

}
