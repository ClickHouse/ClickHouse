#include "FunctionsConsistentHashing.h"
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace
{

/// Code from https://arxiv.org/pdf/1406.2294.pdf
inline int32_t JumpConsistentHash(uint64_t key, int32_t num_buckets)
{
    int64_t b = -1, j = 0;
    while (j < num_buckets)
    {
        b = j;
        key = key * 2862933555777941757ULL + 1;
        j = static_cast<int64_t>((b + 1) * (static_cast<double>(1LL << 31) / static_cast<double>((key >> 33) + 1)));
    }
    return static_cast<int32_t>(b);
}

struct JumpConsistentHashImpl
{
    static constexpr auto name = "jumpConsistentHash";

    using HashType = UInt64;
    using ResultType = Int32;
    using BucketsType = ResultType;
    static constexpr auto max_buckets = static_cast<UInt64>(std::numeric_limits<BucketsType>::max());

    static inline ResultType apply(UInt64 hash, BucketsType n)
    {
        return JumpConsistentHash(hash, n);
    }
};

using FunctionJumpConsistentHash = FunctionConsistentHashImpl<JumpConsistentHashImpl>;

}

void registerFunctionJumpConsistentHash(FunctionFactory & factory)
{
    factory.registerFunction<FunctionJumpConsistentHash>();
}

}
