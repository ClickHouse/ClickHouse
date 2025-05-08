#include <Functions/sparseGrams.h>

namespace DB
{

REGISTER_FUNCTION(SparseGrams)
{
    FunctionDocumentation::Description description_sparse = R"(
Finds all substrings of a given string that have a length of at least `n`,
where the hashes of the (n-1)-grams at the borders of the substring
are strictly greater than those of any (n-1)-gram inside the substring.
Uses `CRC32` as a hash function.
)";
    FunctionDocumentation::Syntax syntax_sparse = "sparseGrams(s[, min_ngram_length, max_ngram_length])";
    FunctionDocumentation::Arguments arguments_sparse = {
        {"s", "An input string.", {"String"}},
        {"min_ngram_length", "Optional. The minimum length of extracted ngram. The default and minimal value is 3.", {"UInt*"}},
        {"max_ngram_length", "Optional. The maximum length of extracted ngram. The default value is 100. Should be not less than `min_ngram_length`.", {"UInt*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_sparse = {"Returns an array of selected substrings.", {"Array(String)"}};
    FunctionDocumentation::Examples examples_sparse = {
    {
        "Usage example",
        "SELECT sparseGrams('alice', 3)",
        R"(
┌─sparseGrams('alice', 3)────────────┐
│ ['ali','lic','lice','ice']         │
└────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation_sparse = {description_sparse, syntax_sparse, arguments_sparse, returned_value_sparse, examples_sparse, introduced_in, category};

    FunctionDocumentation::Description description_sparse_utf8 = R"(
Finds all substrings of a given UTF-8 string that have a length of at least `n`, where the hashes of the (n-1)-grams at the borders of the substring are strictly greater than those of any (n-1)-gram inside the substring.
Expects a UTF-8 string, throws an exception in case of an invalid UTF-8 sequence.
Uses `CRC32` as a hash function.
)";
    FunctionDocumentation::Syntax syntax_sparse_utf8 = "sparseGramsUTF8(s[, min_ngram_length, max_ngram_length])";
    FunctionDocumentation::ReturnedValue returned_value_sparse_utf8 = {"Returns an array of selected UTF-8 substrings.", {"Array(String)"}};
    FunctionDocumentation::Examples examples_sparse_utf8 = {
    {
        "Usage example",
        "SELECT sparseGramsUTF8('алиса', 3)",
        R"(
┌─sparseGramsUTF8('алиса', 3)─┐
│ ['али','лис','иса']         │
└─────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation documentation_sparse_utf8 = {description_sparse_utf8, syntax_sparse_utf8, arguments_sparse, returned_value_sparse_utf8, examples_sparse_utf8, introduced_in, category};

    FunctionDocumentation::Description description_hashes = R"(
Finds hashes of all substrings of a given string that have a length of at least `n`,
where the hashes of the (n-1)-grams at the borders of the substring
are strictly greater than those of any (n-1)-gram inside the substring.
Uses `CRC32` as a hash function.
)";
    FunctionDocumentation::Syntax syntax_hashes = "sparseGramsHashes(s[, min_ngram_length, max_ngram_length])";
    FunctionDocumentation::ReturnedValue returned_value_hashes = {"Returns an array of selected substrings CRC32 hashes.", {"Array(UInt32)"}};
    FunctionDocumentation::Examples examples_hashes = {
    {
        "Usage example",
        "SELECT sparseGramsHashes('alice', 3)",
        R"(
┌─sparseGramsHashes('alice', 3)──────────────────────┐
│ [1481062250,2450405249,4012725991,1918774096]      │
└────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation documentation_hashes = {description_hashes, syntax_hashes, arguments_sparse, returned_value_hashes, examples_hashes, introduced_in, category};

    FunctionDocumentation::Description description_hashes_utf8 = R"(
Finds hashes of all substrings of a given UTF-8 string that have a length of at least `n`, where the hashes of the (n-1)-grams at the borders of the substring are strictly greater than those of any (n-1)-gram inside the substring.
Expects UTF-8 string, throws an exception in case of invalid UTF-8 sequence.
Uses `CRC32` as a hash function.
)";
    FunctionDocumentation::Syntax syntax_hashes_utf8 = "sparseGramsHashesUTF8(s[, min_ngram_length, max_ngram_length])";
    FunctionDocumentation::ReturnedValue returned_value_hashes_utf8 = {"Returns an array of selected UTF-8 substrings CRC32 hashes.", {"Array(UInt32)"}};
    FunctionDocumentation::Examples examples_hashes_utf8 = {
    {
        "Usage example",
        "SELECT sparseGramsHashesUTF8('алиса', 3)",
        R"(
┌─sparseGramsHashesUTF8('алиса', 3)─┐
│ [4178533925,3855635300,561830861] │
└───────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation documentation_hashes_utf8 = {description_hashes_utf8, syntax_hashes_utf8, arguments_sparse, returned_value_hashes_utf8, examples_hashes_utf8, introduced_in, category};

    factory.registerFunction<FunctionSparseGrams>(documentation_sparse);
    factory.registerFunction<FunctionSparseGramsUTF8>(documentation_sparse_utf8);

    factory.registerFunction<SparseGramsHashes<false>>(documentation_hashes);
    factory.registerFunction<SparseGramsHashes<true>>(documentation_hashes_utf8);
}

}
