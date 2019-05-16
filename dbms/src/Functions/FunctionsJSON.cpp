#include <Functions/FunctionsJSON.h>
#include <Functions/DummyJSONParser.h>
#include <Functions/SimdJSONParser.h>
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
    registerFunctionsJSONTemplate<DummyJSONParser>(factory);
}

}
