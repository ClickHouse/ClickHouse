#include <Functions/FunctionsConsistentHashing.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace
{

/// Code from https://arxiv.org/pdf/1406.2294.pdf
inline int32_t JumpConsistentHash(uint64_t key, int32_t num_buckets)
{
    int64_t b = -1;
    int64_t j = 0;
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

    static ResultType apply(UInt64 hash, BucketsType n)
    {
        return JumpConsistentHash(hash, n);
    }
};

using FunctionJumpConsistentHash = FunctionConsistentHashImpl<JumpConsistentHashImpl>;

}

REGISTER_FUNCTION(JumpConsistentHash)
{
    FunctionDocumentation::Description jumpConsistentHash_description = R"(
Calculates [JumpConsistentHash](JumpConsistentHash](https://arxiv.org/pdf/1406.2294.pdf) from a `UInt64` value.
)";
    FunctionDocumentation::Syntax jumpConsistentHash_syntax = "jumpConsistentHash(key, buckets)";
    FunctionDocumentation::Arguments jumpConsistentHash_arguments = {
        {"key", "The input key.", {"UInt64"}},
        {"buckets", "The number of buckets.", {"Int32"}}
    };
    FunctionDocumentation::ReturnedValue jumpConsistentHash_returned_value = {"Returns the computed hash value.", {"Int32"}};
    FunctionDocumentation::Examples jumpConsistentHash_examples = {
    {
        "Usage example",
        "SELECT jumpConsistentHash(256, 4)",
        R"(
┌─jumpConsistentHash(256, 4)─┐
│                          3 │
└────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn jumpConsistentHash_introduced_in = {1, 1};
    FunctionDocumentation::Category jumpConsistentHash_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation jumpConsistentHash_documentation = {jumpConsistentHash_description, jumpConsistentHash_syntax, jumpConsistentHash_arguments, jumpConsistentHash_returned_value, jumpConsistentHash_examples, jumpConsistentHash_introduced_in, jumpConsistentHash_category};
    factory.registerFunction<FunctionJumpConsistentHash>(jumpConsistentHash_documentation);
}

}
