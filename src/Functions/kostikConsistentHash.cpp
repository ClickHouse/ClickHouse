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
    FunctionDocumentation::Description description = R"(
An O(1) time and space consistent hash algorithm by Konstantin 'Kostik' Oblakov.
Only efficient with `n <= 32768`.
)";
    FunctionDocumentation::Syntax syntax = "kostikConsistentHash(input, n)";
    FunctionDocumentation::Arguments arguments = {
        {"input", "An integer key.", {"UInt64"}},
        {"n", "The number of buckets.", {"UInt16"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the computed hash value.", {"UInt16"}};
    FunctionDocumentation::Examples examples = {
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
    FunctionDocumentation::IntroducedIn introduced_in = {22, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionKostikConsistentHash>(documentation);
    factory.registerAlias("yandexConsistentHash", "kostikConsistentHash");
}

}
