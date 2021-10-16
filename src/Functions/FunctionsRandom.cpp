#include <Functions/FunctionsRandom.h>
#include <Functions/FunctionFactory.h>
#include <Functions/VectorExtension.h>
#include <Common/HashTable/Hash.h>
#include <Common/randomSeed.h>
#include <common/unaligned.h>
#if USE_MULTITARGET_CODE
#  include <x86intrin.h>
#endif


namespace DB
{

namespace
{
    /// NOTE Probably
    ///    http://www.pcg-random.org/
    /// or http://www.math.sci.hiroshima-u.ac.jp/~m-mat/MT/SFMT/
    /// or http://docs.yeppp.info/c/group__yep_random___w_e_l_l1024a.html
    /// could go better.

    struct LinearCongruentialGenerator
    {
        /// Constants from `man lrand48_r`.
        static constexpr UInt64 a = 0x5DEECE66D;
        static constexpr UInt64 c = 0xB;

        /// And this is from `head -c8 /dev/urandom | xxd -p`
        UInt64 current = 0x09826f4a081cee35ULL;

        void seed(UInt64 value)
        {
            current = value;
        }

        UInt32 next()
        {
            current = current * a + c;
            return current >> 16;
        }
    };

    UInt64 calcSeed(UInt64 rand_seed, UInt64 additional_seed)
    {
        return intHash64(rand_seed ^ intHash64(additional_seed));
    }

    void seed(LinearCongruentialGenerator & generator, UInt64 rand_seed, intptr_t additional_seed)
    {
        generator.seed(calcSeed(rand_seed, additional_seed));
    }

    /// The array of random numbers from 'head -c8 /dev/urandom | xxd -p'.
    /// Can be used for creating seeds for random generators.
    constexpr std::array<UInt64, 32> random_numbers = {
        0x0c8ff307dabc0c4cULL, 0xf4bce78bf3821c1bULL, 0x4eb628a1e189c21aULL, 0x85ae000d253e0dbcULL,
        0xc98073e6480f8a10ULL, 0xb17e9b70a084d570ULL, 0x1361c752b768da8cULL, 0x3d915f60c06d144dULL,
        0xd5bc9b7aced79587ULL, 0x66c28000ba8a66cfULL, 0x0fb58da7a48820f5ULL, 0x540ee1b57aa861a1ULL,
        0x212f11936ef2db04ULL, 0xa3939cd900edcc58ULL, 0xc676c84420170102ULL, 0xcbdc824e8b4bf3edULL,

        0x8296f9d93cc94e3bULL, 0x78a7e826d62085b2ULL, 0xaa30620211fc6c69ULL, 0xbd38de52f0a93677ULL,
        0x19983de8d79dcc4eULL, 0x8afe883ef2199e6fULL, 0xb7160f7ed022b60aULL, 0x2ce173d373ddafd4ULL,
        0x15762761bb55b9acULL, 0x3e448fc94fdd28e7ULL, 0xa5121232adfbe70aULL, 0xb1e0f6d286112804ULL,
        0x6062e96de9554806ULL, 0xcc679b329c28882aULL, 0x5c6d29f45cbc060eULL, 0x1af1325a86ffb162ULL,
    };
}

DECLARE_DEFAULT_CODE(

void RandImpl::execute(char * output, size_t size)
{
    LinearCongruentialGenerator generator0;
    LinearCongruentialGenerator generator1;
    LinearCongruentialGenerator generator2;
    LinearCongruentialGenerator generator3;

    UInt64 rand_seed = randomSeed();

    seed(generator0, rand_seed, random_numbers[0] + reinterpret_cast<intptr_t>(output));
    seed(generator1, rand_seed, random_numbers[1] + reinterpret_cast<intptr_t>(output));
    seed(generator2, rand_seed, random_numbers[2] + reinterpret_cast<intptr_t>(output));
    seed(generator3, rand_seed, random_numbers[3] + reinterpret_cast<intptr_t>(output));

    for (const char * end = output + size; output < end; output += 16)
    {
        unalignedStore<UInt32>(output, generator0.next());
        unalignedStore<UInt32>(output + 4, generator1.next());
        unalignedStore<UInt32>(output + 8, generator2.next());
        unalignedStore<UInt32>(output + 12, generator3.next());
    }
    /// It is guaranteed (by PaddedPODArray) that we can overwrite up to 15 bytes after end.
}

) // DECLARE_DEFAULT_CODE

DECLARE_AVX2_SPECIFIC_CODE(

using namespace VectorExtension;

/* Takes 2 vectors with LinearCongruentialGenerator states and combines them into vector with random values.
 * From every rand-state we use only bits 15...47 to generate random vector.
 */
inline UInt64x4 combineValues(UInt64x4 a, UInt64x4 b)
{
    auto xa = reinterpret_cast<__m256i>(a);
    auto xb = reinterpret_cast<__m256i>(b);
    /// Every state is 8-byte value and we need to use only 4 from the middle.
    /// Swap the low half and the high half of every state to move these bytes from the middle to sides.
    /// xa = xa[1, 0, 3, 2, 5, 4, 7, 6]
    xa = _mm256_shuffle_epi32(xa, 0xb1);
    /// Now every 8-byte value in xa is xx....xx and every value in xb is ..xxxx.. where x is random byte we want to use.
    /// Just blend them to get the result vector.
    /// result = xa[0],xb[1,2],xa[3,4],xb[5,6],xa[7,8],xb[9,10],xa[11,12],xb[13,14],xa[15]
    __m256i result = _mm256_blend_epi16(xa, xb, 0x66);
    return reinterpret_cast<UInt64x4>(result);
}

void RandImpl::execute(char * output, size_t size)
{
    if (size == 0)
        return;

    char * end = output + size;

    constexpr int vec_size = 4;
    constexpr int safe_overwrite = 15;
    constexpr int bytes_per_write = 4 * sizeof(UInt64x4);

    UInt64 rand_seed = randomSeed();

    UInt64 a = LinearCongruentialGenerator::a;
    // TODO(dakovalkov): try to remove this.
    /// Note: GCC likes to expand multiplication by a constant into shifts + additions.
    /// In this case a few multiplications become tens of shifts and additions. That leads to a huge slow down.
    /// To avoid it we pretend that 'a' is not a constant. Actually we hope that rand_seed is never 0.
    if (rand_seed == 0)
        a = LinearCongruentialGenerator::a + 2;

    constexpr UInt64 c = LinearCongruentialGenerator::c;

    UInt64x4 gens1{};
    UInt64x4 gens2{};
    UInt64x4 gens3{};
    UInt64x4 gens4{};

    for (int i = 0; i < vec_size; ++i)
    {
        gens1[i] = calcSeed(rand_seed, random_numbers[i] + reinterpret_cast<intptr_t>(output));
        gens2[i] = calcSeed(rand_seed, random_numbers[i + vec_size] + reinterpret_cast<intptr_t>(output));
        gens3[i] = calcSeed(rand_seed, random_numbers[i + 2 * vec_size] + reinterpret_cast<intptr_t>(output));
        gens4[i] = calcSeed(rand_seed, random_numbers[i + 3 * vec_size] + reinterpret_cast<intptr_t>(output));
    }

    while ((end - output) + safe_overwrite >= bytes_per_write)
    {
        gens1 = gens1 * a + c;
        gens2 = gens2 * a + c;
        unalignedStore<UInt64x4>(output, combineValues(gens1, gens2));
        gens3 = gens3 * a + c;
        gens4 = gens4 * a + c;
        unalignedStore<UInt64x4>(output + sizeof(UInt64x4), combineValues(gens3, gens4));
        gens1 = gens1 * a + c;
        gens2 = gens2 * a + c;
        unalignedStore<UInt64x4>(output + 2 * sizeof(UInt64x4), combineValues(gens1, gens2));
        gens3 = gens3 * a + c;
        gens4 = gens4 * a + c;
        unalignedStore<UInt64x4>(output + 3 * sizeof(UInt64x4), combineValues(gens3, gens4));
        output += bytes_per_write;
    }

    // Process tail
    while ((end - output) > 0)
    {
        gens1 = gens1 * a + c;
        gens2 = gens2 * a + c;
        UInt64x4 values = combineValues(gens1, gens2);
        for (int i = 0; i < vec_size && (end - output) > 0; ++i)
        {
            unalignedStore<UInt64>(output, values[i]);
            output += sizeof(UInt64);
        }
    }
}

) // DECLARE_AVX2_SPECIFIC_CODE

}
