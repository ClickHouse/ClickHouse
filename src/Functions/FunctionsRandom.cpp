#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>
#include <Common/HashTable/Hash.h>
#include <Common/randomSeed.h>
#include <common/unaligned.h>
#include <x86intrin.h>

namespace DB
{

// TODO(dakovalkov): remove this workaround.
#if !defined(__clang__)
#  pragma GCC diagnostic ignored "-Wvector-operation-performance"
#endif

DECLARE_MULTITARGET_CODE(

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

}

void RandImpl::execute(char * output, size_t size)
{
    LinearCongruentialGenerator generator0;
    LinearCongruentialGenerator generator1;
    LinearCongruentialGenerator generator2;
    LinearCongruentialGenerator generator3;

    UInt64 rand_seed = randomSeed();

    seed(generator0, rand_seed, 0xfb4121280b2ab902ULL + reinterpret_cast<intptr_t>(output));
    seed(generator1, rand_seed, 0x0121cf76df39c673ULL + reinterpret_cast<intptr_t>(output));
    seed(generator2, rand_seed, 0x17ae86e3a19a602fULL + reinterpret_cast<intptr_t>(output));
    seed(generator3, rand_seed, 0x8b6e16da7e06d622ULL + reinterpret_cast<intptr_t>(output));

    for (const char * end = output + size; output < end; output += 16)
    {
        unalignedStore<UInt32>(output, generator0.next());
        unalignedStore<UInt32>(output + 4, generator1.next());
        unalignedStore<UInt32>(output + 8, generator2.next());
        unalignedStore<UInt32>(output + 12, generator3.next());
    }
    /// It is guaranteed (by PaddedPODArray) that we can overwrite up to 15 bytes after end.
}

void RandImpl2::execute(char * output, size_t size)
{
    if (size == 0)
        return;

    LinearCongruentialGenerator generator0;
    LinearCongruentialGenerator generator1;
    LinearCongruentialGenerator generator2;
    LinearCongruentialGenerator generator3;
    LinearCongruentialGenerator generator4;
    LinearCongruentialGenerator generator5;
    LinearCongruentialGenerator generator6;
    LinearCongruentialGenerator generator7;

    UInt64 rand_seed = randomSeed();

    seed(generator0, rand_seed, 0xfaaae481acb5874aULL + reinterpret_cast<intptr_t>(output));
    seed(generator1, rand_seed, 0x3181a34f32887db6ULL + reinterpret_cast<intptr_t>(output));
    seed(generator2, rand_seed, 0xb6970e4a91b66afdULL + reinterpret_cast<intptr_t>(output));
    seed(generator3, rand_seed, 0xc16062649e83dc13ULL + reinterpret_cast<intptr_t>(output));
    seed(generator4, rand_seed, 0xbb093972da5c8d92ULL + reinterpret_cast<intptr_t>(output));
    seed(generator5, rand_seed, 0xc37dcc410dcfed31ULL + reinterpret_cast<intptr_t>(output));
    seed(generator6, rand_seed, 0x45e1526b7a4367d5ULL + reinterpret_cast<intptr_t>(output));
    seed(generator7, rand_seed, 0x99c2759203868a7fULL + reinterpret_cast<intptr_t>(output));

    const char * end = output + size;

    constexpr int bytes_per_write = 32;
    constexpr int safe_overwrite = 15;

    for (; (end - output) + safe_overwrite >= bytes_per_write; output += safe_overwrite)
    {
        unalignedStore<UInt32>(output,      generator0.next());
        unalignedStore<UInt32>(output + 4,  generator1.next());
        unalignedStore<UInt32>(output + 8,  generator2.next());
        unalignedStore<UInt32>(output + 12, generator3.next());
        unalignedStore<UInt32>(output + 16, generator4.next());
        unalignedStore<UInt32>(output + 20, generator5.next());
        unalignedStore<UInt32>(output + 24, generator6.next());
        unalignedStore<UInt32>(output + 28, generator7.next());
    }

    seed(generator0, rand_seed, 0xfaaae481acb5874aULL + reinterpret_cast<intptr_t>(output));
    seed(generator1, rand_seed, 0x3181a34f32887db6ULL + reinterpret_cast<intptr_t>(output));
    seed(generator2, rand_seed, 0xb6970e4a91b66afdULL + reinterpret_cast<intptr_t>(output));
    seed(generator3, rand_seed, 0xc16062649e83dc13ULL + reinterpret_cast<intptr_t>(output));

    if (end - output > 0)
    {
        unalignedStore<UInt32>(output,      generator0.next());
        unalignedStore<UInt32>(output + 4,  generator1.next());
        unalignedStore<UInt32>(output + 8,  generator2.next());
        unalignedStore<UInt32>(output + 12, generator3.next());
    }
}

typedef UInt64 UInt64x16 __attribute__ ((vector_size (128)));
typedef UInt64 UInt64x8  __attribute__ ((vector_size (64)));
typedef UInt64 UInt64x4  __attribute__ ((vector_size (32)));

typedef UInt32 UInt32x16 __attribute__ ((vector_size (64)));
typedef UInt32 UInt32x8  __attribute__ ((vector_size (32)));
typedef UInt32 UInt32x4  __attribute__ ((vector_size (16)));

template <int Size>
struct DummyStruct;

template <>
struct DummyStruct<4>
{
    using UInt64Type = UInt64x4;
    using UInt32Type = UInt32x4;
};
template <>
struct DummyStruct<8>
{
    using UInt64Type = UInt64x8;
    using UInt32Type = UInt32x8;
};
template <>
struct DummyStruct<16>
{
    using UInt64Type = UInt64x16;
    using UInt32Type = UInt32x16;
};

template <int Size>
using VecUInt64 = typename DummyStruct<Size>::UInt64Type;
template <int Size>
using VecUInt32 = typename DummyStruct<Size>::UInt32Type;

void RandImpl3::execute(char * output, size_t size)
{
    if (size == 0)
        return;
    
    char * end = output + size;

    UInt64x4 generators = {
        0xfb4121280b2ab902ULL + reinterpret_cast<intptr_t>(output),
        0x0121cf76df39c673ULL + reinterpret_cast<intptr_t>(output),
        0x17ae86e3a19a602fULL + reinterpret_cast<intptr_t>(output),
        0x8b6e16da7e06d622ULL + reinterpret_cast<intptr_t>(output),
    };

    constexpr int bytes_per_write = sizeof(UInt32x4);
    constexpr int safe_overwrite = 15;

    while ((end - output) + safe_overwrite >= bytes_per_write)
    {
        generators *= LinearCongruentialGenerator::a;
        generators += LinearCongruentialGenerator::c;
        unalignedStore<UInt32x4>(output, __builtin_convertvector(generators >> 16, UInt32x4));
        output += bytes_per_write;
    }
}

void RandImpl4::execute(char * output, size_t size)
{
    if (size == 0)
        return;
    
    char * end = output + size;

    UInt64 rand_seed = randomSeed();

    UInt64x8 generators = {
        calcSeed(rand_seed, 0xfb4121280b2ab902ULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0x0121cf76df39c673ULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0x17ae86e3a19a602fULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0x8b6e16da7e06d622ULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0xfb4121f80b2ab902ULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0x0122cf767f39c633ULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0x14ae86e3a79a502fULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0x876316da7e06d622ULL + reinterpret_cast<intptr_t>(output)),
    };

    constexpr int bytes_per_write = sizeof(UInt32x8);
    constexpr int safe_overwrite = 15;

    while ((end - output) + safe_overwrite >= bytes_per_write)
    {
        generators *= LinearCongruentialGenerator::a;
        generators += LinearCongruentialGenerator::c;
        unalignedStore<UInt32x8>(output, __builtin_convertvector(generators >> 16, UInt32x8));
        output += bytes_per_write;
    }

    if ((end - output) > 0)
    {
        generators *= LinearCongruentialGenerator::a;
        generators += LinearCongruentialGenerator::c;
        UInt32x8 values = __builtin_convertvector(generators >> 16, UInt32x8);
        for (int i = 0; (end - output) > 0; ++i)
        {
            unalignedStore<UInt32>(output, values[i]);
            output += sizeof(UInt32);
        }
    }
}

void RandImpl5::execute(char * output, size_t size)
{
    if (size == 0)
        return;
    
    char * end = output + size;

    UInt64 rand_seed = randomSeed();

    UInt64x16 generators = {
        calcSeed(rand_seed, 0xfb4121280b2ab902ULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0x0121cf76df39c673ULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0x17ae86e3a19a602fULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0x8b6e16da7e06d622ULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0xfb4121f80b2ab902ULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0x0122cf767f39c633ULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0x14ae86e3a79a502fULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0x876316da7e06d622ULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0xfb4821280b2ab912ULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0x0126cf76df39c633ULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0x17a486e3a19a602fULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0x8b6216da7e08d622ULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0xfb4101f80b5ab902ULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0x01226f767f34c633ULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0x14ae86e3a75a502fULL + reinterpret_cast<intptr_t>(output)),
        calcSeed(rand_seed, 0x876e36da7e36d622ULL + reinterpret_cast<intptr_t>(output)),
    };

    constexpr int bytes_per_write = sizeof(UInt32x16);
    constexpr int safe_overwrite = 15;

    while ((end - output) + safe_overwrite >= bytes_per_write)
    {
        generators *= LinearCongruentialGenerator::a;
        generators += LinearCongruentialGenerator::c;
        unalignedStore<UInt32x16>(output, __builtin_convertvector(generators >> 16, UInt32x16));
        output += bytes_per_write;
    }

    if ((end - output) > 0)
    {
        generators *= LinearCongruentialGenerator::a;
        generators += LinearCongruentialGenerator::c;
        UInt32x16 values = __builtin_convertvector(generators >> 16, UInt32x16);
        for (int i = 0; (end - output) > 0; ++i)
        {
            unalignedStore<UInt32>(output, values[i]);
            output += sizeof(UInt32);
        }
    }
}

namespace {

constexpr std::array<UInt64, 16> random_numbers = {
    0x0c8ff307dabc0c4cULL,
    0xf4bce78bf3821c1bULL,
    0x4eb628a1e189c21aULL,
    0x85ae000d253e0dbcULL,

    0xc98073e6480f8a10ULL,
    0xb17e9b70a084d570ULL,
    0x1361c752b768da8cULL,
    0x3d915f60c06d144dULL,

    0xd5bc9b7aced79587ULL,
    0x66c28000ba8a66cfULL,
    0x0fb58da7a48820f5ULL,
    0x540ee1b57aa861a1ULL,

    0x212f11936ef2db04ULL,
    0xa3939cd900edcc58ULL,
    0xc676c84420170102ULL,
    0xcbdc824e8b4bf3edULL,
};

};

template <int VectorSize>
void RandVecImpl<VectorSize>::execute(char * output, size_t size)
{
    static_assert(VectorSize >= 4);
    static_assert(VectorSize <= random_numbers.size());

    if (size == 0)
        return;
    
    char * end = output + size;

    constexpr int safe_overwrite = 15;
    constexpr int bytes_per_write = sizeof(VecUInt32<VectorSize>);

    UInt64 rand_seed = randomSeed();

    VecUInt64<VectorSize> generators{};
    for (int i = 0; i < VectorSize; ++i)
        generators[i] = calcSeed(rand_seed, random_numbers[VectorSize] + reinterpret_cast<intptr_t>(output));

    while ((end - output) + safe_overwrite >= bytes_per_write)
    {
        generators *= LinearCongruentialGenerator::a;
        generators += LinearCongruentialGenerator::c;
        VecUInt32<VectorSize> values = __builtin_convertvector(generators >> 16, VecUInt32<VectorSize>);
        unalignedStore<VecUInt32<VectorSize>>(output, values);
        output += bytes_per_write;
    }

    if ((end - output) > 0)
    {
        generators *= LinearCongruentialGenerator::a;
        generators += LinearCongruentialGenerator::c;
        VecUInt32<VectorSize> values = __builtin_convertvector(generators >> 16, VecUInt32<VectorSize>);
        for (int i = 0; (end - output) > 0; ++i)
        {
            unalignedStore<UInt32>(output, values[i]);
            output += sizeof(UInt32);
        }
    }
}

template struct RandVecImpl<4>;
template struct RandVecImpl<8>;
template struct RandVecImpl<16>;

template <int VectorSize>
void RandVecImpl2<VectorSize>::execute(char * output, size_t size)
{
    static_assert(VectorSize >= 4);

    if (size == 0)
        return;
    
    char * end = output + size;

    constexpr int safe_overwrite = 15;
    constexpr int bytes_per_write = 2 * sizeof(VecUInt32<VectorSize>);

    UInt64 rand_seed = randomSeed();
    VecUInt64<VectorSize> gens1{}, gens2{};
    for (int i = 0; i < VectorSize; ++i)
    {
        gens1[i] = calcSeed(rand_seed, i * 1123465ull * reinterpret_cast<intptr_t>(output));
        gens2[i] = calcSeed(rand_seed, i * 6432453ull * reinterpret_cast<intptr_t>(output));
    }

    while ((end - output) + safe_overwrite >= bytes_per_write)
    {
        gens1 *= LinearCongruentialGenerator::a;
        gens1 += LinearCongruentialGenerator::c;
        VecUInt32<VectorSize> values1 = __builtin_convertvector(gens1 >> 16, VecUInt32<VectorSize>);
        unalignedStore<VecUInt32<VectorSize>>(output, values1);
        gens2 *= LinearCongruentialGenerator::a;
        gens2 += LinearCongruentialGenerator::c;
        VecUInt32<VectorSize> values2 = __builtin_convertvector(gens2 >> 16, VecUInt32<VectorSize>);
        unalignedStore<VecUInt32<VectorSize>>(output, values2);
        output += bytes_per_write;
    }
        
    while ((end - output) > 0)
    {
        gens1 *= LinearCongruentialGenerator::a;
        gens1 += LinearCongruentialGenerator::c;
        VecUInt32<VectorSize> values = __builtin_convertvector(gens1 >> 16, VecUInt32<VectorSize>);
        for (int i = 0; (end - output) > 0 && i < VectorSize; ++i)
        {
            unalignedStore<UInt32>(output, values[i]);
            output += sizeof(UInt32);
        }
    }
}

template struct RandVecImpl2<4>;
template struct RandVecImpl2<8>;
template struct RandVecImpl2<16>;

// template <int VectorSize>
// void RandVecImpl4<VectorSize>::execute(char * output, size_t size)
// {
//     static_assert(VectorSize >= 4);

//     if (size == 0)
//         return;
    
//     char * end = output + size;

//     constexpr int safe_overwrite = 15;
//     constexpr int bytes_per_write = 4 * sizeof(VecUInt32<VectorSize>);

//     VecUInt64<VectorSize> gens1{}, gens2{}, gens3{}, gens4{};
//     for (int i = 0; i < VectorSize; ++i)
//     {
//         gens1[i] = calcSeed(i * 1123465ull * reinterpret_cast<intptr_t>(output));
//         gens2[i] = calcSeed(i * 6432453ull * reinterpret_cast<intptr_t>(output));
//         gens3[i] = calcSeed(i * 1346434ull * reinterpret_cast<intptr_t>(output));
//         gens4[i] = calcSeed(i * 5344753ull * reinterpret_cast<intptr_t>(output));
//     }

//     while ((end - output) + safe_overwrite >= bytes_per_write)
//     {
//         gens1 *= LinearCongruentialGenerator::a;
//         gens1 += LinearCongruentialGenerator::c;
//         VecUInt32<VectorSize> values1 = __builtin_convertvector(gens1 >> 16, VecUInt32<VectorSize>);
//         unalignedStore<VecUInt32<VectorSize>>(output, values1);
//         gens2 *= LinearCongruentialGenerator::a;
//         gens2 += LinearCongruentialGenerator::c;
//         VecUInt32<VectorSize> values2 = __builtin_convertvector(gens2 >> 16, VecUInt32<VectorSize>);
//         unalignedStore<VecUInt32<VectorSize>>(output, values2);
//         gens3 *= LinearCongruentialGenerator::a;
//         gens3 += LinearCongruentialGenerator::c;
//         VecUInt32<VectorSize> values3 = __builtin_convertvector(gens3 >> 16, VecUInt32<VectorSize>);
//         unalignedStore<VecUInt32<VectorSize>>(output, values3);
//         gens4 *= LinearCongruentialGenerator::a;
//         gens4 += LinearCongruentialGenerator::c;
//         VecUInt32<VectorSize> values4 = __builtin_convertvector(gens4 >> 16, VecUInt32<VectorSize>);
//         unalignedStore<VecUInt32<VectorSize>>(output, values4);
//         output += bytes_per_write;
//     }
        
//     while ((end - output) > 0)
//     {
//         gens1 *= LinearCongruentialGenerator::a;
//         gens1 += LinearCongruentialGenerator::c;
//         VecUInt32<VectorSize> values = __builtin_convertvector(gens1 >> 16, VecUInt32<VectorSize>);
//         for (int i = 0; (end - output) > 0 && i < VectorSize; i += 4)
//         {
//             unalignedStore<UInt32>(output,      values[i]);
//             unalignedStore<UInt32>(output + 4,  values[i + 1]);
//             unalignedStore<UInt32>(output + 8,  values[i + 2]);
//             unalignedStore<UInt32>(output + 12, values[i + 3]);
//             output += 16;
//         }
//     }
// }

// template struct RandVecImpl2<4>; 
// template struct RandVecImpl2<8>; 
// template struct RandVecImpl2<16>; 

) //DECLARE_MULTITARGET_CODE

DECLARE_AVX2_SPECIFIC_CODE(

void RandImpl6::execute(char * output, size_t size)
{
    if (size == 0)
        return;
    
    char * end = output + size;

    UInt64x8 generators = {
        0x5f186ce5faee450bULL + reinterpret_cast<intptr_t>(output),
        0x9adb2ca3c72ac2eeULL + reinterpret_cast<intptr_t>(output),
        0x07acf8bfa2537705ULL + reinterpret_cast<intptr_t>(output),
        0x692b1b533834db92ULL + reinterpret_cast<intptr_t>(output),
        0x5148b84cdda30081ULL + reinterpret_cast<intptr_t>(output),
        0xe17b8a75a301ad47ULL + reinterpret_cast<intptr_t>(output),
        0x6d4a5d69ed2a5f56ULL + reinterpret_cast<intptr_t>(output),
        0x114e23266201b333ULL + reinterpret_cast<intptr_t>(output),
    };
    
    union {
        UInt64x8 vec;
        __m256i mm[2];
    } gens {generators};
    
    constexpr int bytes_per_write = sizeof(UInt32x8);
    constexpr int safe_overwrite = 15;

    const auto low_a  = _mm256_set1_epi64x(0xDEECE66D);
    // const auto high_a = _mm256_set1_epi64x(5);
    const auto c = _mm256_set1_epi64x(11);

    while ((end - output) + safe_overwrite >= bytes_per_write)
    {
        {
            auto gens_high = _mm256_srli_epi64(gens.mm[0], 32);
            auto low_low_res = _mm256_mul_epu32(gens.mm[0], low_a);
            auto high_low_res = _mm256_slli_epi64(_mm256_mul_epu32(gens_high, low_a), 32);
            auto low_high_res = _mm256_slli_epi64(gens.mm[0], 32) + _mm256_slli_epi64(gens.mm[0], 34);
            gens.mm[0] = _mm256_add_epi64(_mm256_add_epi64(low_low_res, high_low_res),
                                        _mm256_add_epi64(low_high_res, c));
        }
        {
            auto gens_high = _mm256_srli_epi64(gens.mm[1], 32);
            auto low_low_res = _mm256_mul_epu32(gens.mm[1], low_a);
            auto high_low_res = _mm256_slli_epi64(_mm256_mul_epu32(gens_high, low_a), 32);
            auto low_high_res = _mm256_slli_epi64(gens.mm[1], 32) + _mm256_slli_epi64(gens.mm[1], 34);
            gens.mm[1] = _mm256_add_epi64(_mm256_add_epi64(low_low_res, high_low_res),
                                        _mm256_add_epi64(low_high_res, c));
        }
        // generators *= LinearCongruentialGenerator::a;
        // generators += LinearCongruentialGenerator::c;
        unalignedStore<UInt32x8>(output, __builtin_convertvector(gens.vec >> 16, UInt32x8));
        output += bytes_per_write;
    }

    if ((end - output) > 0)
    {
        generators *= LinearCongruentialGenerator::a;
        generators += LinearCongruentialGenerator::c;
        UInt32x8 values = __builtin_convertvector(generators >> 16, UInt32x8);
        for (int i = 0; (end - output) > 0; ++i)
        {
            unalignedStore<UInt32>(output, values[i]);
            output += sizeof(UInt32);
        }
    }
}

) // DECLARE_AVX2_SPECIFIC_CODE

}
