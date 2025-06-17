#include <Functions/sparseGrams.h>

namespace DB
{

REGISTER_FUNCTION(SparseGrams)
{
    const FunctionDocumentation description = {
        .description=R"(Finds all substrings of a given string that have a length of at least `n`,
where the hashes of the (n-1)-grams at the borders of the substring
are strictly greater than those of any (n-1)-gram inside the substring.)",
        .arguments={
            {"s", "An input string"},
            {"min_ngram_length", "The minimum length of extracted ngram. The default and minimal value is 3"},
            {"max_ngram_length", "The maximum length of extracted ngram. The default value is 100. Should be not less than 'min_ngram_length'"},
        },
        .returned_value{"An array of selected substrings"},
        .category = FunctionDocumentation::Category::String
    };
    const FunctionDocumentation hashes_description{
        .description = R"(Finds hashes of all substrings of a given string that have a length of at least `n`,
where the hashes of the (n-1)-grams at the borders of the substring
are strictly greater than those of any (n-1)-gram inside the substring.)",
        .arguments = description.arguments,
        .returned_value = "An array of selected substrings hashes",
        .category = description.category};

    factory.registerFunction<FunctionSparseGrams>(description);
    factory.registerFunction<FunctionSparseGramsUTF8>(description);

    factory.registerFunction<SparseGramsHashes<false>>(hashes_description);
    factory.registerFunction<SparseGramsHashes<true>>(hashes_description);
}

}
