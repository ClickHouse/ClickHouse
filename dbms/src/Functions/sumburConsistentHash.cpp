#include "FunctionsConsistentHashing.h"
#include <Functions/FunctionFactory.h>

#include <sumbur.h>


namespace DB
{

struct SumburConsistentHashImpl
{
    static constexpr auto name = "sumburConsistentHash";

    using HashType = UInt32;
    using ResultType = UInt16;
    using BucketsType = ResultType;
    static constexpr auto max_buckets = static_cast<UInt64>(std::numeric_limits<BucketsType>::max());

    static inline ResultType apply(HashType hash, BucketsType n)
    {
        return static_cast<ResultType>(sumburConsistentHash(hash, n));
    }
};

using FunctionSumburConsistentHash = FunctionConsistentHashImpl<SumburConsistentHashImpl>;

void registerFunctionSumburConsistentHash(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSumburConsistentHash>();
}

}


