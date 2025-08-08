#include <Functions/FunctionsConsistentHashing.h>
#include <Functions/FunctionFactory.h>

#include <consistent_hashing.h>

namespace DB
{

/// An O(1) time and space consistent hash algorithm by Konstantin Oblakov
struct KostikConsistentHashImpl
{
    static constexpr auto name = "kostikConsistentHash";

    using HashType = UInt64;
    /// Actually it supports UInt64, but it is efficient only if n <= 32768
    using ResultType = UInt16;
    using BucketsType = ResultType;
    static constexpr auto max_buckets = 32768;

    static ResultType apply(UInt64 hash, BucketsType n)
    {
        return ConsistentHashing(hash, n);
    }
};

using FunctionKostikConsistentHash = FunctionConsistentHashImpl<KostikConsistentHashImpl>;

REGISTER_FUNCTION(KostikConsistentHash)
{
    FunctionDocumentation::Description kostikConsistentHash_description = R"(
An O(1) time and space consistent hash algorithm by Konstantin 'kostik' Oblakov. Previously `yandexConsistentHash`.

:::note
It is efficient only if n <= 32768.
:::
)";
    FunctionDocumentation::Syntax kostikConsistentHash_syntax = "kostikConsistentHash(input, n)";
    FunctionDocumentation::Arguments kostikConsistentHash_arguments = {
        {"input", "A UInt64-type key.", {"UInt64"}},
        {"n", "Number of buckets.", {"UInt16"}}
    };
    FunctionDocumentation::ReturnedValue kostikConsistentHash_returned_value = {"Returns the computed hash value.", {"UInt16"}};
    FunctionDocumentation::Examples kostikConsistentHash_examples = {
    {
        "Usage example",
        "SELECT kostikConsistentHash(16045690984833335023, 2);",
        R"(
┌─kostikConsistentHash(16045690984833335023, 2)─┐
│                                             1 │
└───────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn kostikConsistentHash_introduced_in = {22, 6};
    FunctionDocumentation::Category kostikConsistentHash_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation kostikConsistentHash_documentation = {kostikConsistentHash_description, kostikConsistentHash_syntax, kostikConsistentHash_arguments, kostikConsistentHash_returned_value, kostikConsistentHash_examples, kostikConsistentHash_introduced_in, kostikConsistentHash_category};
    factory.registerFunction<FunctionKostikConsistentHash>(kostikConsistentHash_documentation);
    factory.registerAlias("yandexConsistentHash", "kostikConsistentHash");
}

}
