#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>
#include <Common/HashTable/Hash.h>
#include <Common/randomSeed.h>
#include <common/unaligned.h>

namespace DB
{

/*

// TODO(dakovalkov): remove this workaround.
#pragma GCC diagnostic ignored "-Wvector-operation-performance"

DECLARE_MULTITARGET_CODE(

*/

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

    void seed(LinearCongruentialGenerator & generator, intptr_t additional_seed)
    {
        generator.seed(intHash64(randomSeed() ^ intHash64(additional_seed)));
    }
}

void RandImpl::execute(char * output, size_t size)
{
    LinearCongruentialGenerator generator0;
    LinearCongruentialGenerator generator1;
    LinearCongruentialGenerator generator2;
    LinearCongruentialGenerator generator3;

    seed(generator0, 0xfb4121280b2ab902ULL + reinterpret_cast<intptr_t>(output));
    seed(generator1, 0x0121cf76df39c673ULL + reinterpret_cast<intptr_t>(output));
    seed(generator2, 0x17ae86e3a19a602fULL + reinterpret_cast<intptr_t>(output));
    seed(generator3, 0x8b6e16da7e06d622ULL + reinterpret_cast<intptr_t>(output));

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

    seed(generator0, 0xfaaae481acb5874aULL + reinterpret_cast<intptr_t>(output));
    seed(generator1, 0x3181a34f32887db6ULL + reinterpret_cast<intptr_t>(output));
    seed(generator2, 0xb6970e4a91b66afdULL + reinterpret_cast<intptr_t>(output));
    seed(generator3, 0xc16062649e83dc13ULL + reinterpret_cast<intptr_t>(output));
    seed(generator4, 0xbb093972da5c8d92ULL + reinterpret_cast<intptr_t>(output));
    seed(generator5, 0xc37dcc410dcfed31ULL + reinterpret_cast<intptr_t>(output));
    seed(generator6, 0x45e1526b7a4367d5ULL + reinterpret_cast<intptr_t>(output));
    seed(generator7, 0x99c2759203868a7fULL + reinterpret_cast<intptr_t>(output));

    const char * end = output + size;

    for (; (end - output + 15) <= 32; output += 32)
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

    if (end - output > 0)
    {
        unalignedStore<UInt32>(output,      generator0.next());
        unalignedStore<UInt32>(output + 4,  generator1.next());
        unalignedStore<UInt32>(output + 8,  generator2.next());
        unalignedStore<UInt32>(output + 12, generator3.next());
        output += 16;
    }
}

/*

typedef UInt64 UInt64x16 __attribute__ ((vector_size (128)));
typedef UInt64 UInt64x8  __attribute__ ((vector_size (64)));
typedef UInt64 UInt64x4  __attribute__ ((vector_size (32)));

typedef UInt32 UInt32x16 __attribute__ ((vector_size (64)));
typedef UInt32 UInt32x8  __attribute__ ((vector_size (32)));
typedef UInt32 UInt32x4  __attribute__ ((vector_size (16)));

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
        unalignedStore<UInt32x4>(output, __builtin_convertvector(generators, UInt32x4));
        output += bytes_per_write;
    }
}

void RandImpl4::execute(char * output, size_t size)
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

    constexpr int bytes_per_write = sizeof(UInt32x8);
    constexpr int safe_overwrite = 15;

    while ((end - output) + safe_overwrite >= bytes_per_write)
    {
        generators *= LinearCongruentialGenerator::a;
        generators += LinearCongruentialGenerator::c;
        unalignedStore<UInt32x8>(output, __builtin_convertvector(generators, UInt32x8));
        output += bytes_per_write;
    }

    if ((end - output) > 0)
    {
        generators *= LinearCongruentialGenerator::a;
        generators += LinearCongruentialGenerator::c;
        UInt32x8 values = __builtin_convertvector(generators, UInt32x8);
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

    UInt64x16 generators = {
        0xfb4121280b2ab902ULL + reinterpret_cast<intptr_t>(output),
        0x0121cf76df39c673ULL + reinterpret_cast<intptr_t>(output),
        0x17ae86e3a19a602fULL + reinterpret_cast<intptr_t>(output),
        0x8b6e16da7e06d622ULL + reinterpret_cast<intptr_t>(output),
        0xfb4121f80b2ab902ULL + reinterpret_cast<intptr_t>(output),
        0x0122cf767f39c633ULL + reinterpret_cast<intptr_t>(output),
        0x14ae86e3a79a502fULL + reinterpret_cast<intptr_t>(output),
        0x876316da7e06d622ULL + reinterpret_cast<intptr_t>(output),
        0xfb4821280b2ab912ULL + reinterpret_cast<intptr_t>(output),
        0x0126cf76df39c633ULL + reinterpret_cast<intptr_t>(output),
        0x17a486e3a19a602fULL + reinterpret_cast<intptr_t>(output),
        0x8b6216da7e08d622ULL + reinterpret_cast<intptr_t>(output),
        0xfb4101f80b5ab902ULL + reinterpret_cast<intptr_t>(output),
        0x01226f767f34c633ULL + reinterpret_cast<intptr_t>(output),
        0x14ae86e3a75a502fULL + reinterpret_cast<intptr_t>(output),
        0x876e36da7e36d622ULL + reinterpret_cast<intptr_t>(output),
    };

    constexpr int bytes_per_write = sizeof(UInt32x16);
    constexpr int safe_overwrite = 15;

    while ((end - output) + safe_overwrite >= bytes_per_write)
    {
        generators *= LinearCongruentialGenerator::a;
        generators += LinearCongruentialGenerator::c;
        unalignedStore<UInt32x16>(output, __builtin_convertvector(generators, UInt32x16));
        output += bytes_per_write;
    }

    if ((end - output) > 0)
    {
        generators *= LinearCongruentialGenerator::a;
        generators += LinearCongruentialGenerator::c;
        UInt32x16 values = __builtin_convertvector(generators, UInt32x16);
        for (int i = 0; (end - output) > 0; ++i)
        {
            unalignedStore<UInt32>(output, values[i]);
            output += sizeof(UInt32);
        }
    }
}

) //DECLARE_MULTITARGET_CODE

*/

}
