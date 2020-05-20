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

}
