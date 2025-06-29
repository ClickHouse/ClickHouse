#include <Functions/FunctionsRandom.h>
#include <Functions/FunctionFactory.h>
#include <Functions/VectorExtension.h>
#include <Common/HashTable/Hash.h>
#include <Common/randomSeed.h>
#include <base/unaligned.h>

#if USE_MULTITARGET_CODE
#    include <Common/TargetSpecific.h>

#    include <immintrin.h>
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
            return static_cast<UInt32>(current >> 16);
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

    void randImpl(char * output, size_t size)
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
}

#if USE_MULTITARGET_CODE

using namespace VectorExtension;

/* Takes 2 vectors with LinearCongruentialGenerator states and combines them into vector with random values.
 * From every rand-state we use only bits 15...47 to generate random vector.
 */
AVX2_FUNCTION_SPECIFIC_ATTRIBUTE ALWAYS_INLINE inline UInt64x4 combineValuesAVX2(UInt64x4 & a, UInt64x4 & b)
{
    auto xa = reinterpret_cast<__m256i>(a);
    auto xb = reinterpret_cast<__m256i>(b);

    /// 2 128-bit lanes
    /// Each lane consist of 4 32-bit words
    /// We only want to keep the 4 words of the middle so we move them to the sides
    /// Mask: 0xb1 => 0b10110001 => Order: 2, 3, 0, 1
    /// xa = a[2, 3, 0, 1, 6, 7, 4, 5]
    xa = _mm256_shuffle_epi32(xa, 0xb1);

    /// Now every 128-bit lane in xa is xx....xx and every value in xb is ..xxxx.. where x is random byte we want to use.
    /// Now each lane consists of 8 16-bit words
    /// Just blend them to get the result vector.
    /// Mask (least significant 8 bits): 0x66 => 0b01100110 => a_b_b_a_a_b_b_a (x2)
    /// result = xa[0],xb[1,2],xa[3,4],xb[5,6],xa[7] - xa[8],xb[9,10],xa[11,12],xb[13,14],xa[15]
    /// Final: a[2], b[1], b[2], a[1], a[6], b[5], b[6], a[5] - a[10], b[9], b[10], a[9], a[14], b[13], b[14], a[13]
    __m256i result = _mm256_blend_epi16(xa, xb, 0x66);
    return reinterpret_cast<UInt64x4>(result);
}

AVX2_FUNCTION_SPECIFIC_ATTRIBUTE void NO_INLINE RandImpl::executeAVX2(char * output, size_t size)
{
    if (size == 0)
        return;

    char * end = output + size;

    constexpr int vec_size = 4;
    constexpr int safe_overwrite = PADDING_FOR_SIMD - 1;
    constexpr int bytes_per_write = 4 * sizeof(UInt64x4);

    UInt64 rand_seed = randomSeed();

    UInt64 a = LinearCongruentialGenerator::a;
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
        unalignedStore<UInt64x4>(output, combineValuesAVX2(gens1, gens2));
        gens3 = gens3 * a + c;
        gens4 = gens4 * a + c;
        unalignedStore<UInt64x4>(output + sizeof(UInt64x4), combineValuesAVX2(gens3, gens4));
        gens1 = gens1 * a + c;
        gens2 = gens2 * a + c;
        unalignedStore<UInt64x4>(output + 2 * sizeof(UInt64x4), combineValuesAVX2(gens1, gens2));
        gens3 = gens3 * a + c;
        gens4 = gens4 * a + c;
        unalignedStore<UInt64x4>(output + 3 * sizeof(UInt64x4), combineValuesAVX2(gens3, gens4));
        output += bytes_per_write;
    }

    // Process tail
    while ((end - output) > 0)
    {
        gens1 = gens1 * a + c;
        gens2 = gens2 * a + c;
        UInt64x4 values = combineValuesAVX2(gens1, gens2);
        for (int i = 0; i < vec_size && (end - output) > 0; ++i)
        {
            unalignedStore<UInt64>(output, values[i]);
            output += sizeof(UInt64);
        }
    }
}


/* Takes 2 vectors with LinearCongruentialGenerator states and combines them into vector with random values.
 * From every rand-state we use only bits 15...47 to generate random vector.
 */
AVX512BW_FUNCTION_SPECIFIC_ATTRIBUTE ALWAYS_INLINE inline UInt64x8 combineValuesAVX512BW(UInt64x8 & a, UInt64x8 & b)
{
    auto xa = reinterpret_cast<__m512i>(a);
    auto xb = reinterpret_cast<__m512i>(b);

    /// 4 128-bit lanes
    /// Each lane consist of 4 32-bit words
    /// We only want to keep the 4 words of the middle so we move them to the sides
    /// Mask: 0xb1 => 0b10110001 => Order: 2, 3, 0, 1
    /// xa = a[2, 3, 0, 1, 6, 7, 4, 5, 10, 11, 8, 9, 14, 15, 12, 13]
    xa = _mm512_shuffle_epi32(xa, 0xb1); // 0b10110001 => 2_3_0_1 (128 bits x 4 times)

    /// Now every 128-bit lane in xa is xx....xx and every value in xb is ..xxxx.. where x is random byte we want to use.
    /// Now each lane consists of 32 16-bit words
    /// Just blend them to get the result vector.
    /// Mask (all 32 bits are used): 0x66666666 => 0b01100110011001100110011001100110
    __m512i result = _mm512_mask_blend_epi16(0x66666666, xa, xb);
    return reinterpret_cast<UInt64x8>(result);
}

AVX512BW_FUNCTION_SPECIFIC_ATTRIBUTE void NO_INLINE RandImpl::executeAVX512BW(char * output, size_t size)
{
    if (size == 0)
        return;

    char * end = output + size;

    constexpr int vec_size = 8;
    constexpr int safe_overwrite = PADDING_FOR_SIMD - 1;
    constexpr int bytes_per_write = 4 * sizeof(UInt64x8);

    UInt64 rand_seed = randomSeed();

    UInt64 a = LinearCongruentialGenerator::a;
    constexpr UInt64 c = LinearCongruentialGenerator::c;

    UInt64x8 gens1{};
    UInt64x8 gens2{};
    UInt64x8 gens3{};
    UInt64x8 gens4{};

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
        unalignedStore<UInt64x8>(output, combineValuesAVX512BW(gens1, gens2));
        gens3 = gens3 * a + c;
        gens4 = gens4 * a + c;
        unalignedStore<UInt64x8>(output + sizeof(UInt64x8), combineValuesAVX512BW(gens3, gens4));
        gens1 = gens1 * a + c;
        gens2 = gens2 * a + c;
        unalignedStore<UInt64x8>(output + 2 * sizeof(UInt64x8), combineValuesAVX512BW(gens1, gens2));
        gens3 = gens3 * a + c;
        gens4 = gens4 * a + c;
        unalignedStore<UInt64x8>(output + 3 * sizeof(UInt64x8), combineValuesAVX512BW(gens3, gens4));
        output += bytes_per_write;
    }

    // Process tail
    while ((end - output) > 0)
    {
        gens1 = gens1 * a + c;
        gens2 = gens2 * a + c;
        UInt64x8 values = combineValuesAVX512BW(gens1, gens2);
        for (int i = 0; i < vec_size && (end - output) > 0; ++i)
        {
            unalignedStore<UInt64>(output, values[i]);
            output += sizeof(UInt64);
        }
    }
}

#endif

void RandImpl::execute(char * output, size_t size)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::AVX512BW))
    {
        executeAVX512BW(output, size);
        return;
    }

    if (isArchSupported(TargetArch::AVX2))
    {
        executeAVX2(output, size);
        return;
    }
#endif

    randImpl(output, size);
}
}
