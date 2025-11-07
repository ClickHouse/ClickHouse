#include <Functions/FunctionsHashing.h>

#include <Functions/FunctionFactory.h>

/// FunctionsHashing instantiations are separated into files FunctionsHashing*.cpp
/// to better parallelize the build procedure and avoid MSan build failure
/// due to excessive resource consumption.

namespace DB
{

REGISTER_FUNCTION(Hashing)
{
    FunctionDocumentation::Description sipHash64_description = R"(
Produces a 64-bit [SipHash](https://en.wikipedia.org/wiki/SipHash) hash value.

This is a cryptographic hash function. It works at least three times faster than the [`MD5`](#MD5) hash function.

The function [interprets](/sql-reference/functions/type-conversion-functions#reinterpretasstring) all the input parameters as strings and calculates the hash value for each of them.
It then combines the hashes using the following algorithm:

1. The first and the second hash value are concatenated to an array which is hashed.
2. The previously calculated hash value and the hash of the third input parameter are hashed in a similar way.
3. This calculation is repeated for all remaining hash values of the original input.

:::note
the calculated hash values may be equal for the same input values of different argument types.
This affects for example integer types of different size, named and unnamed `Tuple` with the same data, `Map` and the corresponding `Array(Tuple(key, value))` type with the same data.
:::
)";
    FunctionDocumentation::Syntax sipHash64_syntax = "sipHash64(arg1[, arg2, ...])";
    FunctionDocumentation::Arguments sipHash64_arguments = {
        {"arg1[, arg2, ...]", "A variable number of input arguments.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue sipHash64_returned_value = {"Returns a computed hash value of the input arguments.", {"UInt64"}};
    FunctionDocumentation::Examples sipHash64_examples = {
        {
            "Usage example",
            "SELECT sipHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS SipHash, toTypeName(SipHash) AS type;",
            R"(
┌──────────────SipHash─┬─type───┐
│ 11400366955626497465 │ UInt64 │
└──────────────────────┴────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn sipHash64_introduced_in = {1, 1};
    FunctionDocumentation::Category sipHash64_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation sipHash64_documentation = {sipHash64_description, sipHash64_syntax, sipHash64_arguments, sipHash64_returned_value, sipHash64_examples, sipHash64_introduced_in, sipHash64_category};
    factory.registerFunction<FunctionSipHash64>(sipHash64_documentation);

    FunctionDocumentation::Description sipHash64Keyed_description = R"(
Like [`sipHash64`](#sipHash64) but additionally takes an explicit key argument instead of using a fixed key.
)";
    FunctionDocumentation::Syntax sipHash64Keyed_syntax = "sipHash64Keyed((k0, k1), arg1[,arg2, ...])";
    FunctionDocumentation::Arguments sipHash64Keyed_arguments = {
        {"(k0, k1)", "A tuple of two values representing the key.", {"Tuple(UInt64, UInt64)"}},
        {"arg1[,arg2, ...]", "A variable number of input arguments.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue sipHash64Keyed_returned_value = {"Returns the computed hash of the input values.", {"UInt64"}};
    FunctionDocumentation::Examples sipHash64Keyed_examples = {
        {
            "Usage example",
            "SELECT sipHash64Keyed((506097522914230528, 1084818905618843912), array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS SipHash, toTypeName(SipHash) AS type;",
            R"(
┌─────────────SipHash─┬─type───┐
│ 8017656310194184311 │ UInt64 │
└─────────────────────┴────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn sipHash64Keyed_introduced_in = {23, 2};
    FunctionDocumentation::Category sipHash64Keyed_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation sipHash64Keyed_documentation = {sipHash64Keyed_description, sipHash64Keyed_syntax, sipHash64Keyed_arguments, sipHash64Keyed_returned_value, sipHash64Keyed_examples, sipHash64Keyed_introduced_in, sipHash64Keyed_category};
    factory.registerFunction<FunctionSipHash64Keyed>(sipHash64Keyed_documentation);

    FunctionDocumentation::Description sipHash128_description = R"(
Like [`sipHash64`](#sipHash64) but produces a 128-bit hash value, i.e. the final xor-folding state is done up to 128 bits.

:::tip use sipHash128Reference for new projects
This 128-bit variant differs from the reference implementation and is weaker.
This version exists because, when it was written, there was no official 128-bit extension for SipHash.
New projects are advised to use [`sipHash128Reference`](#sipHash128Reference).
:::
)";
    FunctionDocumentation::Syntax sipHash128_syntax = "sipHash128(arg1[, arg2, ...])";
    FunctionDocumentation::Arguments sipHash128_arguments = {
        {"arg1[, arg2, ...]", "A variable number of input arguments for which to compute the hash.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue sipHash128_returned_value = {"Returns a 128-bit `SipHash` hash value.", {"FixedString(16)"}};
    FunctionDocumentation::Examples sipHash128_examples = {
    {
        "Usage example",
        "SELECT hex(sipHash128('foo', '\\x01', 3));",
        R"(
┌─hex(sipHash128('foo', '', 3))────┐
│ 9DE516A64A414D4B1B609415E4523F24 │
└──────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn sipHash128_introduced_in = {1, 1};
    FunctionDocumentation::Category sipHash128_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation sipHash128_documentation = {sipHash128_description, sipHash128_syntax, sipHash128_arguments, sipHash128_returned_value, sipHash128_examples, sipHash128_introduced_in, sipHash128_category};
    factory.registerFunction<FunctionSipHash128>(sipHash128_documentation);

    FunctionDocumentation::Description sipHash128Keyed_description = R"(
Same as [`sipHash128`](#sipHash128) but additionally takes an explicit key argument instead of using a fixed key.

:::tip use sipHash128ReferenceKeyed for new projects
This 128-bit variant differs from the reference implementation and it's weaker.
This version exists because, when it was written, there was no official 128-bit extension for SipHash.
New projects should probably use [`sipHash128ReferenceKeyed`](#sipHash128ReferenceKeyed).
:::
)";
    FunctionDocumentation::Syntax sipHash128Keyed_syntax = "sipHash128Keyed((k0, k1), [arg1, arg2, ...])";
    FunctionDocumentation::Arguments sipHash128Keyed_arguments = {
        {"(k0, k1)", "A tuple of two UInt64 values representing the key.", {"Tuple(UInt64, UInt64)"}},
        {"arg1[, arg2, ...]", "A variable number of input arguments for which to compute the hash.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue sipHash128Keyed_returned_value = {"A 128-bit `SipHash` hash value of type [FixedString(16)](../data-types/fixedstring.md).", {"FixedString(16)"}};
    FunctionDocumentation::Examples sipHash128Keyed_examples = {
        {
            "Usage example",
            "SELECT hex(sipHash128Keyed((506097522914230528, 1084818905618843912),'foo', '\\x01', 3));",
            R"(
┌─hex(sipHash128Keyed((506097522914230528, 1084818905618843912), 'foo', '', 3))─┐
│ B8467F65C8B4CFD9A5F8BD733917D9BF                                              │
└───────────────────────────────────────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn sipHash128Keyed_introduced_in = {23, 2};
    FunctionDocumentation::Category sipHash128Keyed_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation sipHash128Keyed_documentation = {sipHash128Keyed_description, sipHash128Keyed_syntax, sipHash128Keyed_arguments, sipHash128Keyed_returned_value, sipHash128Keyed_examples, sipHash128Keyed_introduced_in, sipHash128Keyed_category};
    factory.registerFunction<FunctionSipHash128Keyed>(sipHash128Keyed_documentation);

    FunctionDocumentation::Description sipHash128Ref_description = R"(
Like [`sipHash128`](/sql-reference/functions/hash-functions#sipHash128) but implements the 128-bit algorithm from the original authors of SipHash.
    )";
    FunctionDocumentation::Syntax sipHash128Ref_syntax = "sipHash128Reference(arg1[, arg2, ...])";
    FunctionDocumentation::Arguments sipHash128Ref_arguments = {
        {"arg1[, arg2, ...]", "A variable number of input arguments for which to compute the hash.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue sipHash128Ref_returned_value = {"Returns the computed 128-bit `SipHash` hash value of the input arguments.", {"FixedString(16)"}};
    FunctionDocumentation::Examples sipHash128Ref_examples = {
    {
        "Usage example",
        "SELECT hex(sipHash128Reference('foo', '\x01', 3));",
        R"(
┌─hex(sipHash128Reference('foo', '', 3))─┐
│ 4D1BE1A22D7F5933C0873E1698426260       │
└────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn sipHash128Ref_introduced_in = {23, 2};
    FunctionDocumentation::Category sipHash128Ref_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation sipHash128Ref_documentation = {sipHash128Ref_description, sipHash128Ref_syntax, sipHash128Ref_arguments, sipHash128Ref_returned_value, sipHash128Ref_examples, sipHash128Ref_introduced_in, sipHash128Ref_category};
    factory.registerFunction<FunctionSipHash128Reference>(sipHash128Ref_documentation);

    FunctionDocumentation::Description sipHash128RefKeyed_description = R"(
Same as [`sipHash128Reference`](#sipHash128Reference) but additionally takes an explicit key argument instead of using a fixed key.
    )";
    FunctionDocumentation::Syntax sipHash128RefKeyed_syntax = "sipHash128ReferenceKeyed((k0, k1), arg1[, arg2, ...])";
    FunctionDocumentation::Arguments sipHash128RefKeyed_arguments = {
        {"(k0, k1)", "Tuple of two values representing the key", {"Tuple(UInt64, UInt64)"}},
        {"arg1[, arg2, ...]", "A variable number of input arguments for which to compute the hash.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue sipHash128RefKeyed_returned_value = {"Returns the computed 128-bit `SipHash` hash value of the input arguments.", {"FixedString(16)"}};
    FunctionDocumentation::Examples sipHash128RefKeyed_examples = {
    {
        "Usage example",
        "SELECT hex(sipHash128Reference('foo', '\x01', 3));",
         R"(
┌─hex(sipHash128Reference('foo', '', 3))─┐
│ 4D1BE1A22D7F5933C0873E1698426260       │
└────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn sipHash128RefKeyed_introduced_in = {23, 2};
    FunctionDocumentation::Category sipHash128RefKeyed_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation sipHash128RefKeyed_documentation = {sipHash128RefKeyed_description, sipHash128RefKeyed_syntax, sipHash128RefKeyed_arguments, sipHash128RefKeyed_returned_value, sipHash128RefKeyed_examples, sipHash128RefKeyed_introduced_in, sipHash128RefKeyed_category};
    factory.registerFunction<FunctionSipHash128ReferenceKeyed>(sipHash128RefKeyed_documentation);

    FunctionDocumentation::Description cityHash64_description = R"(
Produces a 64-bit [CityHash](https://github.com/google/cityhash) hash value.

This is a fast non-cryptographic hash function.
It uses the CityHash algorithm for string parameters and implementation-specific fast non-cryptographic hash function for parameters with other data types.
The function uses the CityHash combinator to get the final results.

:::info
Google changed the algorithm of CityHash after it was added to ClickHouse.
In other words, ClickHouse's cityHash64 and Google's upstream CityHash now produce different results.
ClickHouse cityHash64 corresponds to CityHash v1.0.2.
:::

:::note
The calculated hash values may be equal for the same input values of different argument types.
This affects for example integer types of different size, named and unnamed `Tuple` with the same data, `Map` and the corresponding `Array(Tuple(key, value))` type with the same data.
:::
)";
    FunctionDocumentation::Syntax cityHash64_syntax = "cityHash64(arg1[, arg2, ...])";
    FunctionDocumentation::Arguments cityHash64_arguments = {
        {"arg1[, arg2, ...]", "A variable number of input arguments for which to compute the hash.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue cityHash64_returned_value = {"Returns the computed hash of the input arguments.", {"UInt64"}};
    FunctionDocumentation::Examples cityHash64_examples = {
        {
            "Call example",
            "SELECT cityHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS CityHash, toTypeName(CityHash) AS type;",
            R"(
┌─────────────CityHash─┬─type───┐
│ 12072650598913549138 │ UInt64 │
└──────────────────────┴────────┘
            )"
        },
        {
            "Computing the checksum of the entire table with accuracy up to the row order",
            R"(
CREATE TABLE users (
    id UInt32,
    name String,
    age UInt8,
    city String
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO users VALUES
(1, 'Alice', 25, 'New York'),
(2, 'Bob', 30, 'London'),
(3, 'Charlie', 35, 'Tokyo');

SELECT groupBitXor(cityHash64(*)) FROM users;
            )",
            R"(
┌─groupBitXor(⋯age, city))─┐
│     11639977218258521182 │
└──────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn cityHash64_introduced_in = {1, 1};
    FunctionDocumentation::Category cityHash64_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation cityHash64_documentation = {cityHash64_description, cityHash64_syntax, cityHash64_arguments, cityHash64_returned_value, cityHash64_examples, cityHash64_introduced_in, cityHash64_category};
    factory.registerFunction<FunctionCityHash64>(cityHash64_documentation);

    FunctionDocumentation::Description farmFingerprint64_description = R"(
Produces a 64-bit [FarmHash](https://github.com/google/farmhash) value using the `Fingerprint64` method.

:::tip
`farmFingerprint64` is preferred for a stable and portable value over [`farmHash64`](#farmHash64).
:::

:::note
The calculated hash values may be equal for the same input values of different argument types.
This affects for example integer types of different size, named and unnamed `Tuple` with the same data, `Map` and the corresponding `Array(Tuple(key, value))` type with the same data.
:::
)";
    FunctionDocumentation::Syntax farmFingerprint64_syntax = "farmFingerprint64(arg1[, arg2, ...])";
    FunctionDocumentation::Arguments farmFingerprint64_arguments = {
        {"arg1[, arg2, ...]", "A variable number of input arguments for which to compute the hash.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue farmFingerprint64_returned_value = {"Returns the computed hash value of the input arguments.", {"UInt64"}};
    FunctionDocumentation::Examples farmFingerprint64_examples = {
        {
            "Usage example",
            "SELECT farmFingerprint64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS FarmFingerprint, toTypeName(FarmFingerprint) AS type;",
            R"(
┌─────FarmFingerprint─┬─type───┐
│ 5752020380710916328 │ UInt64 │
└─────────────────────┴────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn farmFingerprint64_introduced_in = {20, 12};
    FunctionDocumentation::Category farmFingerprint64_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation farmFingerprint64_documentation = {farmFingerprint64_description, farmFingerprint64_syntax, farmFingerprint64_arguments, farmFingerprint64_returned_value, farmFingerprint64_examples, farmFingerprint64_introduced_in, farmFingerprint64_category};
    factory.registerFunction<FunctionFarmFingerprint64>(farmFingerprint64_documentation);

    FunctionDocumentation::Description farmHash64_description = R"(
Produces a 64-bit [FarmHash](https://github.com/google/farmhash) using the `Hash64` method.

:::tip
[`farmFingerprint64`](#farmFingerprint64) is preferred for a stable and portable value.
:::

:::note
The calculated hash values may be equal for the same input values of different argument types.
This affects for example integer types of different size, named and unnamed `Tuple` with the same data, `Map` and the corresponding `Array(Tuple(key, value))` type with the same data.
:::
)";
    FunctionDocumentation::Syntax farmHash64_syntax = "farmHash64(arg1[, arg2, ...])";
    FunctionDocumentation::Arguments farmHash64_arguments = {
        {"arg1[, arg2, ...]", "A variable number of input arguments for which to compute the hash.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue farmHash64_returned_value = {"Returns the computed hash value of the input arguments.", {"UInt64"}};
    FunctionDocumentation::Examples farmHash64_examples = {
        {
            "Usage example",
            "SELECT farmHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS FarmHash, toTypeName(FarmHash) AS type;",
            R"(
┌─────────────FarmHash─┬─type───┐
│ 18125596431186471178 │ UInt64 │
└──────────────────────┴────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn farmHash64_introduced_in = {1, 1};
    FunctionDocumentation::Category farmHash64_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation farmHash64_documentation = {farmHash64_description, farmHash64_syntax, farmHash64_arguments, farmHash64_returned_value, farmHash64_examples, farmHash64_introduced_in, farmHash64_category};
    factory.registerFunction<FunctionFarmHash64>(farmHash64_documentation);
    FunctionDocumentation::Description metroHash64_description = R"(
Produces a 64-bit [MetroHash](http://www.jandrewrogers.com/2015/05/27/metrohash/) hash value.

:::note
The calculated hash values may be equal for the same input values of different argument types.
This affects for example integer types of different size, named and unnamed `Tuple` with the same data, `Map` and the corresponding `Array(Tuple(key, value))` type with the same data.
:::
)";
    FunctionDocumentation::Syntax metroHash64_syntax = "metroHash64(arg1[, arg2, ...])";
    FunctionDocumentation::Arguments metroHash64_arguments = {
        {"arg1[, arg2, ...]", "A variable number of input arguments for which to compute the hash.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue metroHash64_returned_value = {"Returns the computed hash of the input arguments.", {"UInt64"}};
    FunctionDocumentation::Examples metroHash64_examples = {
    {
        "Usage example",
        R"(
SELECT metroHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MetroHash, toTypeName(MetroHash) AS type;
        )",
        R"(
┌────────────MetroHash─┬─type───┐
│ 14235658766382344533 │ UInt64 │
└──────────────────────┴────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn metroHash64_introduced_in = {1, 1};
    FunctionDocumentation::Category metroHash64_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation metroHash64_documentation = {metroHash64_description, metroHash64_syntax, metroHash64_arguments, metroHash64_returned_value, metroHash64_examples, metroHash64_introduced_in, metroHash64_category};
    factory.registerFunction<FunctionMetroHash64>(metroHash64_documentation);
    FunctionDocumentation::Description URLHash_description = R"(
A fast, decent-quality non-cryptographic hash function for a string obtained from a URL using some type of normalization.

This hash function has two modes:

| Mode             | Description                                                                                                                                                                                 |
|------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|`URLHash(url)`    | Calculates a hash from a string without one of the trailing symbols `/`,`?` or `#` at the end, if present.                                                                                  |
|`URLHash(url, N)` | Calculates a hash from a string up to the N level in the URL hierarchy, without one of the trailing symbols `/`,`?` or `#` at the end, if present. Levels are the same as in `URLHierarchy`.|
)";
    FunctionDocumentation::Syntax URLHash_syntax = "URLHash(url[, N])";
    FunctionDocumentation::Arguments URLHash_arguments = {
        {"url", "URL string to hash.", {"String"}},
        {"N", "Optional. Level in the URL hierarchy.", {"(U)Int*"}}
    };
    FunctionDocumentation::ReturnedValue URLHash_returned_value = {"Returns the computed hash value of `url`.", {"UInt64"}};
    FunctionDocumentation::Examples URLHash_examples = {
    {
        "Usage example",
        "SELECT URLHash('https://www.clickhouse.com')",
        R"(
┌─URLHash('htt⋯house.com')─┐
│     13614512636072854701 │
└──────────────────────────┘
        )"
    },
    {
        "Hash of url with specified level",
        R"(
SELECT URLHash('https://www.clickhouse.com/docs', 0);
SELECT URLHash('https://www.clickhouse.com/docs', 1);
        )",
        R"(
-- hash of https://www.clickhouse.com
┌─URLHash('htt⋯m/docs', 0)─┐
│     13614512636072854701 │
└──────────────────────────┘
-- hash of https://www.clickhouse.com/docs
┌─URLHash('htt⋯m/docs', 1)─┐
│     13167253331440520598 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn URLHash_introduced_in = {1, 1};
    FunctionDocumentation::Category URLHash_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation URLHash_documentation = {URLHash_description, URLHash_syntax, URLHash_arguments, URLHash_returned_value, URLHash_examples, URLHash_introduced_in, URLHash_category};
    factory.registerFunction<FunctionURLHash>(URLHash_documentation);
    FunctionDocumentation::Description javaHash_description = R"(
Calculates JavaHash from:
- [string](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452),
- [Byte](https://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/Byte.java#l405),
- [Short](https://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/Short.java#l410),
- [Integer](https://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/Integer.java#l959),
- [Long](https://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/Long.java#l1060).

:::caution
This hash function is unperformant.
Use it only when this algorithm is already in use in another system and you need to calculate the same result.
:::

:::note
Java only supports calculating the hash of signed integers,
so if you want to calculate a hash of unsigned integers you must cast them to the proper signed ClickHouse types.
:::
)";
    FunctionDocumentation::Syntax javaHash_syntax = "javaHash(arg)";
    FunctionDocumentation::Arguments javaHash_arguments = {
        {"arg", "Input value to hash.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue javaHash_returned_value = {"Returns the computed hash of `arg`", {"Int32"}};
    FunctionDocumentation::Examples javaHash_examples = {
    {
        "Usage example 1",
        R"(
SELECT javaHash(toInt32(123));
        )",
        R"(
┌─javaHash(toInt32(123))─┐
│               123      │
└────────────────────────┘
        )"
     },
     {
        "Usage example 2",
        R"(
SELECT javaHash('Hello, world!');
        )",
        R"(
┌─javaHash('Hello, world!')─┐
│               -1880044555 │
└───────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn javaHash_introduced_in = {20, 1};
    FunctionDocumentation::Category javaHash_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation javaHash_documentation = {javaHash_description, javaHash_syntax, javaHash_arguments, javaHash_returned_value, javaHash_examples, javaHash_introduced_in, javaHash_category};
    factory.registerFunction<FunctionJavaHash>(javaHash_documentation);
    FunctionDocumentation::Description javaHashUTF16LE_description = R"(
Calculates [JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) from a string, assuming it contains bytes representing a string in UTF-16LE encoding.
)";
    FunctionDocumentation::Syntax javaHashUTF16LE_syntax = "javaHashUTF16LE(arg)";
    FunctionDocumentation::Arguments javaHashUTF16LE_arguments = {
        {"arg", "A string in UTF-16LE encoding.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue javaHashUTF16LE_returned_value = {"Returns the computed hash of the UTF-16LE encoded string.", {"Int32"}};
    FunctionDocumentation::Examples javaHashUTF16LE_examples = {
    {
        "Usage example",
        "SELECT javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'));",
        R"(
┌─javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'))─┐
│                                                      3556498 │
└──────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn javaHashUTF16LE_introduced_in = {20, 1};
    FunctionDocumentation::Category javaHashUTF16LE_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation javaHashUTF16LE_documentation = {javaHashUTF16LE_description, javaHashUTF16LE_syntax, javaHashUTF16LE_arguments, javaHashUTF16LE_returned_value, javaHashUTF16LE_examples, javaHashUTF16LE_introduced_in, javaHashUTF16LE_category};
    factory.registerFunction<FunctionJavaHashUTF16LE>(javaHashUTF16LE_documentation);
    FunctionDocumentation::Description hiveHash_description = R"(
Calculates a "HiveHash" from a string.
This is just [`JavaHash`](#javaHash) with zeroed out sign bits.
This function is used in [Apache Hive](https://en.wikipedia.org/wiki/Apache_Hive) for versions before 3.0.

:::caution
This hash function is unperformant.
Use it only when this algorithm is already used in another system and you need to calculate the same result.
:::
)";
    FunctionDocumentation::Syntax hiveHash_syntax = "hiveHash(arg)";
    FunctionDocumentation::Arguments hiveHash_arguments = {
        {"arg", "Input string to hash.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue hiveHash_returned_value = {R"(Returns the computed "hive hash" of the input string.)", {"Int32"}};
    FunctionDocumentation::Examples hiveHash_examples = {
    {
        "Usage example",
        "SELECT hiveHash('Hello, world!');",
        R"(
┌─hiveHash('Hello, world!')─┐
│                 267439093 │
└───────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn hiveHash_introduced_in = {20, 1};
    FunctionDocumentation::Category hiveHash_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation hiveHash_documentation = {hiveHash_description, hiveHash_syntax, hiveHash_arguments, hiveHash_returned_value, hiveHash_examples, hiveHash_introduced_in, hiveHash_category};
    factory.registerFunction<FunctionHiveHash>(hiveHash_documentation);

    FunctionDocumentation::Description xxHash32_description = R"(
Calculates a [xxHash](http://cyan4973.github.io/xxHash/) from a string.

For the 64-bit version see [`xxHash64`](#xxHash64)
)";
    FunctionDocumentation::Syntax xxHash32_syntax = "xxHash32(arg)";
    FunctionDocumentation::Arguments xxHash32_arguments = {
        {"arg", "Input string to hash.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue xxHash32_returned_value = {"Returns the computed 32-bit hash of the input string.", {"UInt32"}};
    FunctionDocumentation::Examples xxHash32_examples = {
    {
        "Usage example",
        "SELECT xxHash32('Hello, world!');",
        R"(
┌─xxHash32('Hello, world!')─┐
│                 834093149 │
└───────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn xxHash32_introduced_in = {20, 1};
    FunctionDocumentation::Category xxHash32_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation xxHash32_documentation = {xxHash32_description, xxHash32_syntax, xxHash32_arguments, xxHash32_returned_value, xxHash32_examples, xxHash32_introduced_in, xxHash32_category};
    factory.registerFunction<FunctionXxHash32>(xxHash32_documentation);

    FunctionDocumentation::Description xxHash64_description = R"(
Calculates a [xxHash](http://cyan4973.github.io/xxHash/) from a string.

For the 32-bit version see [`xxHash32`](#xxHash32)
)";
    FunctionDocumentation::Syntax xxHash64_syntax = "xxHash64(arg)";
    FunctionDocumentation::Arguments xxHash64_arguments = {
        {"arg", "Input string to hash.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue xxHash64_returned_value = {"Returns the computed 64-bit hash of the input string.", {"UInt64"}};
    FunctionDocumentation::Examples xxHash64_examples = {
    {
        "Usage example",
        "SELECT xxHash64('Hello, world!');",
        R"(
┌─xxHash64('Hello, world!')─┐
│      17691043854468224118 │
└───────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn xxHash64_introduced_in = {20, 1};
    FunctionDocumentation::Category xxHash64_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation xxHash64_documentation = {xxHash64_description, xxHash64_syntax, xxHash64_arguments, xxHash64_returned_value, xxHash64_examples, xxHash64_introduced_in, xxHash64_category};
    factory.registerFunction<FunctionXxHash64>(xxHash64_documentation);

    FunctionDocumentation::Description xxh3_description = "Computes a [XXH3](https://github.com/Cyan4973/xxHash) 64-bit hash value.";
    FunctionDocumentation::Syntax xxh3_syntax = "xxh3(expr)";
    FunctionDocumentation::Arguments xxh3_argument = {{"expr", "A list of expressions of any data type.", {"Any"}}};
    FunctionDocumentation::ReturnedValue xxh3_returned_value = {"Returns the computed 64-bit `xxh3` hash value", {"UInt64"}};
    FunctionDocumentation::Examples xxh3_example = {{"Usage example", "SELECT xxh3('ClickHouse')", "18009318874338624809"}};
    FunctionDocumentation::IntroducedIn xxh3_introduced_in = {22, 12};
    FunctionDocumentation::Category xxh3_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation xxh3_documentation = {xxh3_description, xxh3_syntax, xxh3_argument, xxh3_returned_value, xxh3_example, xxh3_introduced_in, xxh3_category};
    factory.registerFunction<FunctionXXH3>(xxh3_documentation);

    FunctionDocumentation::Description wyHash64_description = "Computes a 64-bit [wyHash64](https://github.com/wangyi-fudan/wyhash) hash value.";
    FunctionDocumentation::Syntax wyHash64_syntax = "wyHash64(arg)";
    FunctionDocumentation::Arguments wyHash64_argument = {{"arg", "String argument for which to compute the hash.", {"String"}}};
    FunctionDocumentation::ReturnedValue wyHash64_returned_value = {"Returns the computed 64-bit hash value", {"UInt64"}};
    FunctionDocumentation::Examples wyHash64_example = {{"Usage example", "SELECT wyHash64('ClickHouse') AS Hash;", "12336419557878201794"}};
    FunctionDocumentation::IntroducedIn wyHash64_introduced_in = {22, 7};
    FunctionDocumentation::Category wyHash64_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation wyHash64_documentation = {wyHash64_description, wyHash64_syntax, wyHash64_argument, wyHash64_returned_value, wyHash64_example, wyHash64_introduced_in, wyHash64_category};
    factory.registerFunction<FunctionWyHash64>(wyHash64_documentation);

#if USE_SSL
    FunctionDocumentation::Description halfMD5_description = R"(
[Interprets](/sql-reference/functions/type-conversion-functions#reinterpretasstring) all the input
parameters as strings and calculates the MD5 hash value for each of them. Then combines hashes, takes the first 8 bytes of the hash of the
resulting string, and interprets them as [UInt64](/sql-reference/data-types/int-uint) in big-endian byte order. The function is
relatively slow (5 million short strings per second per processor core).

Consider using the [`sipHash64`](#sipHash64) function instead.

The function takes a variable number of input parameters.
Arguments can be any of the supported data types.
For some data types calculated value of hash function may be the same for the same values even if types of arguments differ (integers of different size, named and unnamed Tuple with the same data, Map and the corresponding Array(Tuple(key, value)) type with the same data).
    )";
    FunctionDocumentation::Syntax halfMD5_syntax = "halfMD5(arg1[, arg2, ..., argN])";
    FunctionDocumentation::Arguments halfMD5_arguments = {{"arg1[, arg2, ..., argN]", "Variable number of arguments for which to compute the hash.", {"Any"}}};
    FunctionDocumentation::ReturnedValue halfMD5_returned_value = {"Returns the computed half MD5 hash of the given input params returned as a `UInt64` in big-endian byte order.", {"UInt64"}};
    FunctionDocumentation::Examples halfMD5_examples = {
    {
        "Usage example",
        R"(
SELECT HEX(halfMD5('abc', 'cde', 'fgh'));
        )",
        R"(
┌─hex(halfMD5('abc', 'cde', 'fgh'))─┐
│ 2C9506B7374CFAF4                  │
└───────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn halfMD5_introduced_in = {1, 1};
    FunctionDocumentation::Category halfMD5_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation halfMD5_documentation = {halfMD5_description, halfMD5_syntax, halfMD5_arguments, halfMD5_returned_value, halfMD5_examples, halfMD5_introduced_in, halfMD5_category};
    factory.registerFunction<FunctionHalfMD5>(halfMD5_documentation);
#endif
}
}
