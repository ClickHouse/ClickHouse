#include <Functions/FunctionFactory.h>
#include <Functions/FunctionTokens.h>
#include <Functions/sparseGramsImpl.h>

namespace DB
{

/** Functions that finds all substrings win minimal length n
  * such their border (n-1)-grams' hashes are more than hashes of every (n-1)-grams' in substring.
  * As a hash function use zlib crc32, which is crc32-ieee with 0xffffffff as initial value
  *
  * sparseGrams(s)
  */
using FunctionSparseGrams = FunctionTokens<SparseGramsImpl<false>>;
using FunctionSparseGramsUTF8 = FunctionTokens<SparseGramsImpl<true>>;

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
        {"max_ngram_length", "Optional. The maximum length of extracted ngram. The default value is 100. Should be not less than `min_ngram_length`.", {"UInt*"}},
        {"min_cutoff_length", "Optional. If specified, only n-grams with length greater or equal than `min_cutoff_length` are returned. The default value is the same as `min_ngram_length`. Should be not less than `min_ngram_length` and not greater than `max_ngram_length`.", {"UInt*"}}
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
    FunctionDocumentation documentation_sparse = {description_sparse, syntax_sparse, arguments_sparse, {}, returned_value_sparse, examples_sparse, introduced_in, category};

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
    FunctionDocumentation documentation_sparse_utf8 = {description_sparse_utf8, syntax_sparse_utf8, arguments_sparse, {}, returned_value_sparse_utf8, examples_sparse_utf8, introduced_in, category};

    factory.registerFunction<FunctionSparseGrams>(documentation_sparse);
    factory.registerFunction<FunctionSparseGramsUTF8>(documentation_sparse_utf8);
}

}
