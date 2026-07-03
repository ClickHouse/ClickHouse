#include <Functions/FunctionsHashing.h>

#include <Functions/FunctionFactory.h>

/// FunctionsHashing instantiations are separated into files FunctionsHashing*.cpp
/// to better parallelize the build procedure and avoid MSan build failure
/// due to excessive resource consumption.

namespace DB
{

REGISTER_FUNCTION(HashingMurmur)
{
    FunctionDocumentation::Description murmurHash2_32_description = R"(
Computes the [MurmurHash2](https://github.com/aappleby/smhasher) hash of the input value.

:::note
The calculated hash values may be equal for the same input values of different argument types.
This affects for example integer types of different size, named and unnamed `Tuple` with the same data, `Map` and the corresponding `Array(Tuple(key, value))` type with the same data.
:::
)";
    FunctionDocumentation::Syntax murmurHash2_32_syntax = "murmurHash2_32(arg1[, arg2, ...])";
    FunctionDocumentation::Arguments murmurHash2_32_arguments = {
        {"arg1[, arg2, ...]", "A variable number of input arguments for which to compute the hash.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue murmurHash2_32_returned_value = {"Returns the computed hash value of the input arguments.", {"UInt32"}};
    FunctionDocumentation::Examples murmurHash2_32_examples = {
    {
        "Usage example",
        "SELECT murmurHash2_32(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash2, toTypeName(MurmurHash2) AS type;",
        R"(
┌─MurmurHash2─┬─type───┐
│  3681770635 │ UInt32 │
└─────────────┴────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn murmurHash2_32_introduced_in = {18, 5};
    FunctionDocumentation::Category murmurHash2_32_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation murmurHash2_32_documentation = {murmurHash2_32_description, murmurHash2_32_syntax, murmurHash2_32_arguments, murmurHash2_32_returned_value, murmurHash2_32_examples, murmurHash2_32_introduced_in, murmurHash2_32_category};
    factory.registerFunction<FunctionMurmurHash2_32>(murmurHash2_32_documentation);

    FunctionDocumentation::Description murmurHash2_64_description = R"(
Computes the [MurmurHash2](https://github.com/aappleby/smhasher) hash of the input value.

:::note
The calculated hash values may be equal for the same input values of different argument types.
This affects for example integer types of different size, named and unnamed `Tuple` with the same data, `Map` and the corresponding `Array(Tuple(key, value))` type with the same data.
:::
)";
    FunctionDocumentation::Syntax murmurHash2_64_syntax = "murmurHash2_64(arg1[, arg2, ...])";
    FunctionDocumentation::Arguments murmurHash2_64_arguments = {
        {"arg1[, arg2, ...]", "A variable number of input arguments for which to compute the hash.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue murmurHash2_64_returned_value = {"Returns the computed hash of the input arguments.", {"UInt64"}};
    FunctionDocumentation::Examples murmurHash2_64_examples = {
    {
        "Usage example",
        "SELECT murmurHash2_64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash2, toTypeName(MurmurHash2) AS type;",
        R"(
┌──────────MurmurHash2─┬─type───┐
│ 11832096901709403633 │ UInt64 │
└──────────────────────┴────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn murmurHash2_64_introduced_in = {18, 10};
    FunctionDocumentation::Category murmurHash2_64_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation murmurHash2_64_documentation = {murmurHash2_64_description, murmurHash2_64_syntax, murmurHash2_64_arguments, murmurHash2_64_returned_value, murmurHash2_64_examples, murmurHash2_64_introduced_in, murmurHash2_64_category};
    factory.registerFunction<FunctionMurmurHash2_64>(murmurHash2_64_documentation);

    FunctionDocumentation::Description murmurHash3_32_description = R"(
Produces a [MurmurHash3](https://github.com/aappleby/smhasher) hash value.

:::note
The calculated hash values may be equal for the same input values of different argument types.
This affects for example integer types of different size, named and unnamed `Tuple` with the same data, `Map` and the corresponding `Array(Tuple(key, value))` type with the same data.
:::
)";
    FunctionDocumentation::Syntax murmurHash3_32_syntax = "murmurHash3_32(arg1[, arg2, ...])";
    FunctionDocumentation::Arguments murmurHash3_32_arguments = {
        {"arg1[, arg2, ...]", "A variable number of input arguments for which to compute the hash.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue murmurHash3_32_returned_value = {"Returns the computed hash value of the input arguments.", {"UInt32"}};
    FunctionDocumentation::Examples murmurHash3_32_examples = {
    {
        "Usage example",
        "SELECT murmurHash3_32(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash3, toTypeName(MurmurHash3) AS type;",
        R"(
┌─MurmurHash3─┬─type───┐
│     2152717 │ UInt32 │
└─────────────┴────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn murmurHash3_32_introduced_in = {18, 10};
    FunctionDocumentation::Category murmurHash3_32_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation murmurHash3_32_documentation = {murmurHash3_32_description, murmurHash3_32_syntax, murmurHash3_32_arguments, murmurHash3_32_returned_value, murmurHash3_32_examples, murmurHash3_32_introduced_in, murmurHash3_32_category};
    factory.registerFunction<FunctionMurmurHash3_32>(murmurHash3_32_documentation);

    FunctionDocumentation::Description murmurHash3_64_description = R"(
Computes the [MurmurHash3](https://github.com/aappleby/smhasher) hash of the input value.

:::note
The calculated hash values may be equal for the same input values of different argument types.
This affects for example integer types of different size, named and unnamed `Tuple` with the same data, `Map` and the corresponding `Array(Tuple(key, value))` type with the same data.
:::
)";
    FunctionDocumentation::Syntax murmurHash3_64_syntax = "murmurHash3_64(arg1[, arg2, ...])";
    FunctionDocumentation::Arguments murmurHash3_64_arguments = {
        {"arg1[, arg2, ...]", "A variable number of input arguments for which to compute the hash.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue murmurHash3_64_returned_value = {"Returns the computed hash value of the input arguments.", {"UInt64"}};
    FunctionDocumentation::Examples murmurHash3_64_examples = {
    {
        "Usage example",
        "SELECT murmurHash3_64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash3, toTypeName(MurmurHash3) AS type;",
        R"(
┌──────────MurmurHash3─┬─type───┐
│ 11832096901709403633 │ UInt64 │
└──────────────────────┴────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn murmurHash3_64_introduced_in = {18, 10};
    FunctionDocumentation::Category murmurHash3_64_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation murmurHash3_64_documentation = {murmurHash3_64_description, murmurHash3_64_syntax, murmurHash3_64_arguments, murmurHash3_64_returned_value, murmurHash3_64_examples, murmurHash3_64_introduced_in, murmurHash3_64_category};
    factory.registerFunction<FunctionMurmurHash3_64>(murmurHash3_64_documentation);

    FunctionDocumentation::Description murmurHash3_128_description = R"(
Computes the 128-bit [MurmurHash3](https://github.com/aappleby/smhasher) hash of the input value.
)";
    FunctionDocumentation::Syntax murmurHash3_128_syntax = "murmurHash3_128(arg1[, arg2, ...])";
    FunctionDocumentation::Arguments murmurHash3_128_arguments = {
        {"arg1[, arg2, ...]", "A variable number of input arguments for which to compute the hash.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue murmurHash3_128_returned_value = {"Returns the computed 128-bit `MurmurHash3` hash value of the input arguments.", {"FixedString(16)"}};
    FunctionDocumentation::Examples murmurHash3_128_examples = {
    {
        "Usage example",
        "SELECT hex(murmurHash3_128('foo', 'foo', 'foo'));",
        R"(
┌─hex(murmurHash3_128('foo', 'foo', 'foo'))─┐
│ F8F7AD9B6CD4CF117A71E277E2EC2931          │
└───────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn murmurHash3_128_introduced_in = {18, 10};
    FunctionDocumentation::Category murmurHash3_128_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation murmurHash3_128_documentation = {murmurHash3_128_description, murmurHash3_128_syntax, murmurHash3_128_arguments, murmurHash3_128_returned_value, murmurHash3_128_examples, murmurHash3_128_introduced_in, murmurHash3_128_category};
    factory.registerFunction<FunctionMurmurHash3_128>(murmurHash3_128_documentation);

    FunctionDocumentation::Description gccMurmurHash_description = R"(
Computes the 64-bit [MurmurHash2](https://github.com/aappleby/smhasher) hash of the input value using the same seed as used by [GCC](https://github.com/gcc-mirror/gcc/blob/41d6b10e96a1de98e90a7c0378437c3255814b16/libstdc%2B%2B-v3/include/bits/functional_hash.h#L191).

It is portable between Clang and GCC builds.
)";
    FunctionDocumentation::Syntax gccMurmurHash_syntax = "gccMurmurHash(arg1[, arg2, ...])";
    FunctionDocumentation::Arguments gccMurmurHash_arguments = {
        {"arg1[, arg2, ...]", "A variable number of arguments for which to compute the hash.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue gccMurmurHash_returned_value = {"Returns the calculated hash value of the input arguments.", {"UInt64"}};
    FunctionDocumentation::Examples gccMurmurHash_examples = {
    {
        "Usage example",
        R"(
SELECT
    gccMurmurHash(1, 2, 3) AS res1,
    gccMurmurHash(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2)))) AS res2
        )",
        R"(
┌─────────────────res1─┬────────────────res2─┐
│ 12384823029245979431 │ 1188926775431157506 │
└──────────────────────┴─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn gccMurmurHash_introduced_in = {20, 1};
    FunctionDocumentation::Category gccMurmurHash_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation gccMurmurHash_documentation = {gccMurmurHash_description, gccMurmurHash_syntax, gccMurmurHash_arguments, gccMurmurHash_returned_value, gccMurmurHash_examples, gccMurmurHash_introduced_in, gccMurmurHash_category};
    factory.registerFunction<FunctionGccMurmurHash>(gccMurmurHash_documentation);

    FunctionDocumentation::Description kafkaMurmurHash_description = R"(
Calculates the 32-bit [MurmurHash2](https://github.com/aappleby/smhasher) hash of the input value using the same seed as used by [Kafka](https://github.com/apache/kafka/blob/461c5cfe056db0951d9b74f5adc45973670404d7/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L482) and without the highest bit to be compatible with [Default Partitioner](https://github.com/apache/kafka/blob/139f7709bd3f5926901a21e55043388728ccca78/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java#L328).
)";
    FunctionDocumentation::Syntax kafkaMurmurHash_syntax = "kafkaMurmurHash(arg1[, arg2, ...])";
    FunctionDocumentation::Arguments kafkaMurmurHash_arguments = {
        {"arg1[, arg2, ...]", "A variable number of parameters for which to compute the hash.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue kafkaMurmurHash_returned_value = {"Returns the calculated hash value of the input arguments.", {"UInt32"}};
    FunctionDocumentation::Examples kafkaMurmurHash_examples = {
    {
        "Usage example",
        R"(
SELECT
    kafkaMurmurHash('foobar') AS res1,
    kafkaMurmurHash(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS res2
        )",
        R"(
┌───────res1─┬─────res2─┐
│ 1357151166 │ 85479775 │
└────────────┴──────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn kafkaMurmurHash_introduced_in = {23, 4};
    FunctionDocumentation::Category kafkaMurmurHash_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation kafkaMurmurHash_documentation = {kafkaMurmurHash_description, kafkaMurmurHash_syntax, kafkaMurmurHash_arguments, kafkaMurmurHash_returned_value, kafkaMurmurHash_examples, kafkaMurmurHash_introduced_in, kafkaMurmurHash_category};
    factory.registerFunction<FunctionKafkaMurmurHash>(kafkaMurmurHash_documentation);
}
}
