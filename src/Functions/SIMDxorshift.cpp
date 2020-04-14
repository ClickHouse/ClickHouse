#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>
#include <Common/HashTable/Hash.h>
#include <Common/randomSeed.h>
#include <common/unaligned.h>

#include <Functions/FunctionFactory.h>

#include <Functions/SIMDxorshift.h>

extern "C" {
#include <simdxorshift128plus.h>
}

namespace DB
{

BEGIN_AVX_SPECIFIC_CODE

void RandImplXorshift::execute(char * output, size_t size)
{
    avx_xorshift128plus_key_t mykey;
    avx_xorshift128plus_init(324, 4444, &mykey);
    // TODO(set last 16 bytes)
    for (auto * end = output + size - 16; output < end; output += 32) {
        unalignedStore<__m256i>(output, avx_xorshift128plus(&mykey));
    }
}

struct NameRandXorshift { static constexpr auto name = "randxorshift"; };
using FunctionRandXorshift = FunctionRandomXorshift<UInt32, NameRandXorshift>;

void registerFunctionRandXorshift(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRandXorshift>();
}

END_TARGET_SPECIFIC_CODE

}
