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
    FunctionDocumentation::Description description = R"(
Calculates the [jump consistent hash](https://arxiv.org/pdf/1406.2294.pdf) for an integer.
)";
    FunctionDocumentation::Syntax syntax = "jumpConsistentHash(key, buckets)";
    FunctionDocumentation::Arguments arguments = {
        {"key", "The input key.", {"UInt64"}},
        {"buckets", "The number of buckets.", {"Int32"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the computed hash value.", {"Int32"}};
    FunctionDocumentation::Examples examples = {
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
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionJumpConsistentHash>(documentation);
}

}
