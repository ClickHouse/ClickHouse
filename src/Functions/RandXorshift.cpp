/// Disable xorshift
#if 0
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>
#include <Common/HashTable/Hash.h>
#include <Common/randomSeed.h>
#include <common/unaligned.h>

#include <Functions/RandXorshift.h>

extern "C"
{
#include <xorshift128plus.h>
#include <simdxorshift128plus.h>
}

namespace DB
{

DECLARE_DEFAULT_CODE(

void RandXorshiftImpl::execute(char * output, size_t size)
{
    if (size == 0)
        return;

    char * end = output + size;

    xorshift128plus_key_s mykey;

    xorshift128plus_init(0xe9ef384566799595ULL ^ reinterpret_cast<intptr_t>(output),
                         0xa321e1523f4f88c7ULL ^ reinterpret_cast<intptr_t>(output),
                         &mykey);

    constexpr int bytes_per_write = 8;
    constexpr intptr_t mask = bytes_per_write - 1;

    // Process head to make output aligned.
    unalignedStore<UInt64>(output, xorshift128plus(&mykey));
    output = reinterpret_cast<char*>((reinterpret_cast<intptr_t>(output) | mask) + 1);

    while (end - output > 0)
    {
        *reinterpret_cast<UInt64*>(output) = xorshift128plus(&mykey);
        output += bytes_per_write;
    }
}

) // DECLARE_DEFAULT_CODE

DECLARE_AVX2_SPECIFIC_CODE(

void RandXorshiftImpl::execute(char * output, size_t size)
{
    if (size == 0)
        return;

    char * end = output + size;

    avx_xorshift128plus_key_t mykey;
    avx_xorshift128plus_init(0xe9ef384566799595ULL ^ reinterpret_cast<intptr_t>(output),
                             0xa321e1523f4f88c7ULL ^ reinterpret_cast<intptr_t>(output),
                             &mykey);

    constexpr int safe_overwrite = 15; /// How many bytes we can write behind the end.
    constexpr int bytes_per_write = 32;
    constexpr intptr_t mask = bytes_per_write - 1;

    if (size + safe_overwrite < bytes_per_write)
    {
        /// size <= 16.
        _mm_storeu_si128(reinterpret_cast<__m128i*>(output),
                         _mm256_extracti128_si256(avx_xorshift128plus(&mykey), 0));
        return;
    }

    /// Process head to make output aligned.
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(output), avx_xorshift128plus(&mykey));
    output = reinterpret_cast<char*>((reinterpret_cast<intptr_t>(output) | mask) + 1);

    while ((end - output) + safe_overwrite >= bytes_per_write)
    {
        _mm256_store_si256(reinterpret_cast<__m256i*>(output), avx_xorshift128plus(&mykey));
        output += bytes_per_write;
    }

    /// Process tail. (end - output) <= 16.
    if ((end - output) > 0)
    {
        _mm_store_si128(reinterpret_cast<__m128i*>(output),
                        _mm256_extracti128_si256(avx_xorshift128plus(&mykey), 0));
    }
}

) // DECLARE_AVX2_SPECIFIC_CODE

DECLARE_AVX2_SPECIFIC_CODE(

void RandXorshiftImpl2::execute(char * output, size_t size)
{
    if (size == 0)
        return;

    char * end = output + size;

    avx_xorshift128plus_key_t mykey;
    avx_xorshift128plus_init(0xe9ef384566799595ULL ^ reinterpret_cast<intptr_t>(output),
                             0xa321e1523f4f88c7ULL ^ reinterpret_cast<intptr_t>(output),
                             &mykey);

    avx_xorshift128plus_key_t mykey2;
    avx_xorshift128plus_init(0xdfe532a6b5a5eb2cULL ^ reinterpret_cast<intptr_t>(output),
                             0x21cdf6cd1e22bf9cULL ^ reinterpret_cast<intptr_t>(output),
                             &mykey2);

    constexpr int safe_overwrite = 15; /// How many bytes we can write behind the end.
    constexpr int bytes_per_write = 32;
    constexpr intptr_t mask = bytes_per_write - 1;

    if (size + safe_overwrite < bytes_per_write)
    {
        /// size <= 16.
        _mm_storeu_si128(reinterpret_cast<__m128i*>(output),
                         _mm256_extracti128_si256(avx_xorshift128plus(&mykey), 0));
        return;
    }

    /// Process head to make output aligned.
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(output), avx_xorshift128plus(&mykey));
    output = reinterpret_cast<char*>((reinterpret_cast<intptr_t>(output) | mask) + 1);

    while ((end - output) + safe_overwrite >= bytes_per_write * 2)
    {
        _mm256_store_si256(reinterpret_cast<__m256i*>(output), avx_xorshift128plus(&mykey));
        _mm256_store_si256(reinterpret_cast<__m256i*>(output + bytes_per_write), avx_xorshift128plus(&mykey2));
        output += bytes_per_write * 2;
    }

    if ((end - output) + safe_overwrite >= bytes_per_write)
    {
        _mm256_store_si256(reinterpret_cast<__m256i*>(output), avx_xorshift128plus(&mykey));
        output += bytes_per_write;
    }

    /// Process tail. (end - output) <= 16.
    if ((end - output) > 0)
    {
        _mm_store_si128(reinterpret_cast<__m128i*>(output),
                        _mm256_extracti128_si256(avx_xorshift128plus(&mykey), 0));
    }
}

) // DECLARE_AVX2_SPECIFIC_CODE

struct NameRandXorshift { static constexpr auto name = "randxorshift"; };
using FunctionRandXorshift = FunctionRandomXorshift<UInt32, NameRandXorshift>;
struct NameRandXorshift64 { static constexpr auto name = "randxorshift64"; };
using FunctionRandXorshift64 = FunctionRandomXorshift<UInt64, NameRandXorshift64>;

void registerFunctionRandXorshift(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRandXorshift>();
    factory.registerFunction<FunctionRandXorshift64>();
}

}
#endif
