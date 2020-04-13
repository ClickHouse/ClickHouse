#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>
#include <Common/HashTable/Hash.h>
#include <Common/randomSeed.h>
#include <common/unaligned.h>


namespace DB
{

DECLARE_MULTITARGET_CODE(

namespace
{
    struct LinearCongruentialGenerator
    {
        static constexpr UInt64 a = 0x5DEECE66D;
        static constexpr UInt64 c = 0xB;

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

}

) //DECLARE_MULTITARGET_CODE

}
