#pragma once

/// Fairly good thread-safe random number generator, but probably slow-down thread creation a little.

#if defined(ARCADIA_BUILD)
#    include <util/random/common_ops.h>
#    include <util/random/random.h>

class SimpleRng64 : public TCommonRNG<uint64_t, SimpleRng64>
{
public:
    explicit SimpleRng64(uint64_t seed) { SetRandomSeed(seed); }
    inline uint64_t GenRand() const { return RandomNumber<uint64_t>(); }
};

extern thread_local SimpleRng64 thread_local_rng;
#else
#    include <pcg_random.hpp>
extern thread_local pcg64 thread_local_rng;
#endif
