#include <Functions/hasAnyAllPhrases.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/PhraseSearch.h>
#include <Interpreters/Context.h>
#include <Interpreters/ITokenizer.h>
#include <Interpreters/TokenizerFactory.h>
#include <Common/FunctionDocumentation.h>
#include <Common/UnorderedSetWithMemoryTracking.h>

#include <absl/container/flat_hash_set.h>

#include <algorithm>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

constexpr size_t arg_input = 0;
constexpr size_t arg_phrases = 1;
constexpr size_t arg_tokenizer = 2;

/// The input may be Array(Nothing) (the type of an empty array literal `[]`), which carries no phrases.
bool isArrayOfString(const IDataType & type)
{
    const auto * array_type = checkAndGetDataType<DataTypeArray>(&type);
    if (!array_type)
        return false;

    const DataTypePtr & nested_type = array_type->getNestedType();
    return isString(nested_type) || isNothing(nested_type);
}

/// Tokenizes every phrase from the constant array argument, builds the per-phrase KMP failure tables and
/// collapses degenerate cases into the `result_is_always_zero` flag (see HasAnyAllPhrasesState).
template <class Traits>
HasAnyAllPhrasesState
initializePhrases(const ColumnsWithTypeAndName & arguments, const ITokenizer & tokenizer, std::string_view function_name)
{
    HasAnyAllPhrasesState state;

    auto column_phrases = arguments[arg_phrases].column;
    Field phrases_field = (*column_phrases)[0];

    if (phrases_field.isNull() || phrases_field.getType() != Field::Types::Array)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Function '{}' requires a const Array(String) phrases argument, got: {}",
            function_name,
            column_phrases->getFamilyName());

    const Array & phrases = phrases_field.safeGet<Array>();

    /// Deduplicate identical phrases (by their tokenized form). Running two copies of the same
    /// KMP matcher would only waste per-row work, never change the result. `absl::Hash` hashes the
    /// token vector directly, so the lookup is O(1) without building an intermediate key.
    absl::flat_hash_set<VectorWithMemoryTracking<String>> seen_token_sequences;

    for (const auto & phrase_element : phrases)
    {
        if (phrase_element.isNull() || phrase_element.getType() != Field::Types::String)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function '{}' requires a const Array(String) phrases argument, but an element has type {}",
                function_name,
                phrase_element.getTypeName());

        const String & phrase_str = phrase_element.safeGet<String>();

        /// Tokenize the phrase, preserving order (no deduplication) - exactly as `hasPhrase` does.
        VectorWithMemoryTracking<String> tokens;
        tokenizer.stringToTokens(phrase_str.data(), phrase_str.size(), tokens);

        if (tokens.empty())
        {
            /// A phrase that tokenizes to nothing can never match (same as `hasPhrase('input', '')` -> 0).
            if constexpr (Traits::mode == HasAnyAllPhrasesMode::All)
            {
                /// `... AND hasPhrase('input', '')` is unconditionally 0.
                state.patterns.clear();
                state.result_is_always_zero = true;
                return state;
            }
            else
            {
                /// `... OR hasPhrase('input', '')` contributes nothing: drop the phrase.
                continue;
            }
        }

        if (!seen_token_sequences.insert(tokens).second)
            continue;

        PhrasePattern pattern;
        pattern.failure = buildPhraseFailureFunction(tokens);
        pattern.tokens = std::move(tokens);
        state.patterns.push_back(std::move(pattern));
    }

    /// No phrases left to match: result is unconditionally 0. This happens for an empty array
    /// `hasAnyPhrases([])` / `hasAllPhrases([])`, or for `hasAnyPhrases` when every phrase tokenized to
    /// nothing. It is consistent with `hasPhrase('input', '')` -> 0 (which also returns no match on an
    /// empty token sequence) and with `hasAllTokens`/`hasAnyTokens` on empty needles. Keeping the
    /// `patterns` empty here is also what prevents the KMP matcher from dereferencing `tokens[0]` of an
    /// empty phrase (the same guard `hasPhrase` has).
    if (state.patterns.empty())
        state.result_is_always_zero = true;

    return state;
}

/// Runs all phrase matchers over the input column's token stream in a single tokenization pass.
/// For `Any` it stops on the first matching phrase; for `All` it stops once every phrase has matched.
template <class Traits, typename StringColumn>
requires std::same_as<StringColumn, ColumnString> || std::same_as<StringColumn, ColumnFixedString>
void executeMatchPhrases(
    const StringColumn & col_input,
    PaddedPODArray<UInt8> & col_result,
    size_t input_rows_count,
    const ITokenizer & tokenizer,
    const PhrasePatterns & patterns)
{
    col_result.resize(input_rows_count);

    const size_t num_phrases = patterns.size();

    /// Per-phrase mutable matcher state, reused across rows to avoid reallocation.
    /// `matched`/`remaining` track which phrases are still pending; they are only read in `All` mode
    /// (in `Any` mode the first full match returns immediately), hence `[[maybe_unused]]`.
    std::vector<size_t> positions(num_phrases, 0);
    [[maybe_unused]] std::vector<UInt8> matched;
    if constexpr (Traits::mode == HasAnyAllPhrasesMode::All)
        matched.assign(num_phrases, 0);

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        std::string_view input = col_input.getDataAt(i);
        col_result[i] = 0;

        std::ranges::fill(positions, 0);
        [[maybe_unused]] size_t remaining = num_phrases;
        if constexpr (Traits::mode == HasAnyAllPhrasesMode::All)
            std::ranges::fill(matched, 0);

        auto callback = [&](const char * token_start, size_t token_len) -> bool
        {
            std::string_view current_token(token_start, token_len);

            for (size_t p = 0; p < num_phrases; ++p)
            {
                if constexpr (Traits::mode == HasAnyAllPhrasesMode::All)
                {
                    if (matched[p])
                        continue;
                }

                const auto & phrase_tokens = patterns[p].tokens;
                const auto & failure = patterns[p].failure;
                size_t & match_position = positions[p];

                /// Standard KMP step, identical to `hasPhrase`'s MatchPhraseMatcher.
                while (match_position > 0 && current_token != phrase_tokens[match_position])
                    match_position = failure[match_position - 1];

                if (current_token == phrase_tokens[match_position])
                {
                    ++match_position;
                    if (match_position == phrase_tokens.size())
                    {
                        if constexpr (Traits::mode == HasAnyAllPhrasesMode::Any)
                        {
                            col_result[i] = 1;
                            return true; /// Any phrase matched: stop scanning this row.
                        }
                        else
                        {
                            matched[p] = 1;
                            if (--remaining == 0)
                            {
                                col_result[i] = 1;
                                return true; /// Every phrase matched: stop scanning this row.
                            }
                        }
                    }
                }
            }

            return false;
        };

        forEachToken(tokenizer, input.data(), input.size(), callback);
    }
}

template <class Traits>
void executeStringColumn(
    ColumnPtr col_input,
    PaddedPODArray<UInt8> & col_result,
    size_t input_rows_count,
    const ITokenizer & tokenizer,
    const PhrasePatterns & patterns)
{
    if (const auto * col_input_string = checkAndGetColumn<ColumnString>(col_input.get()))
        executeMatchPhrases<Traits>(*col_input_string, col_result, input_rows_count, tokenizer, patterns);
    else if (const auto * col_input_fixedstring = checkAndGetColumn<ColumnFixedString>(col_input.get()))
        executeMatchPhrases<Traits>(*col_input_fixedstring, col_result, input_rows_count, tokenizer, patterns);
}

}

template <class Traits>
FunctionHasAnyAllPhrasesOverloadResolver<Traits>::FunctionHasAnyAllPhrasesOverloadResolver(ContextPtr)
{
}

template <class Traits>
DataTypePtr FunctionHasAnyAllPhrasesOverloadResolver<Traits>::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    FunctionArgumentDescriptors mandatory_args{
        {"input", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"},
        {"phrases", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArrayOfString), isColumnConst, "const Array(String)"}};

    FunctionArgumentDescriptors optional_args{
        {"tokenizer", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"}};

    validateFunctionArguments(name, arguments, mandatory_args, optional_args);

    return std::make_shared<DataTypeNumber<UInt8>>();
}

template <class Traits>
FunctionBasePtr
FunctionHasAnyAllPhrasesOverloadResolver<Traits>::buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const
{
    if (arguments.size() < 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function '{}' requires at least 2 arguments, got {}", name, arguments.size());

    if (!isArrayOfString(*arguments[arg_phrases].type))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "A value of illegal type was provided as 2nd argument 'phrases' to function '{}'. Expected: const Array(String), got: {}",
            name,
            arguments[arg_phrases].type->getName());

    if (!arguments[arg_phrases].column || !isColumnConst(*arguments[arg_phrases].column))
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "A value of illegal type was provided as 2nd argument 'phrases' to function '{}'. Expected: const Array(String), got: {}",
            name,
            arguments[arg_phrases].type->getName());

    DataTypes argument_types{std::from_range_t{}, arguments | std::views::transform([](auto & elem) { return elem.type; })};

    const auto tokenizer_name = arguments.size() < 3 || !arguments[arg_tokenizer].column ? SplitByNonAlphaTokenizer::getExternalName()
                                                                                         : arguments[arg_tokenizer].column->getDataAt(0);
    auto tokenizer = TokenizerFactory::instance().get(tokenizer_name);
    static const UnorderedSetWithMemoryTracking<ITokenizer::Type> supported_types = {
        ITokenizer::Type::SplitByNonAlpha,
        ITokenizer::Type::SplitByString,
        ITokenizer::Type::AsciiCJK,
        ITokenizer::Type::Ngrams,
    };
    if (!supported_types.contains(tokenizer->getType()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function '{}' does not support the '{}' tokenizer.", name, tokenizer_name);

    auto state = initializePhrases<Traits>(arguments, *tokenizer, getName());
    return std::make_shared<FunctionBaseHasAnyAllPhrases<Traits>>(
        std::move(tokenizer), std::move(state), std::move(argument_types), return_type);
}

template <class Traits>
ExecutableFunctionPtr FunctionBaseHasAnyAllPhrases<Traits>::prepare(const ColumnsWithTypeAndName &) const
{
    return std::make_unique<ExecutableFunctionHasAnyAllPhrases<Traits>>(tokenizer, state);
}

template <class Traits>
ColumnPtr ExecutableFunctionHasAnyAllPhrases<Traits>::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
{
    if (input_rows_count == 0)
        return ColumnVector<UInt8>::create();

    auto col_result = ColumnVector<UInt8>::create();
    if (state.result_is_always_zero)
    {
        col_result->getData().assign(input_rows_count, UInt8(0));
        return col_result;
    }

    executeStringColumn<Traits>(arguments[arg_input].column, col_result->getData(), input_rows_count, *tokenizer, state.patterns);
    return col_result;
}

template class ExecutableFunctionHasAnyAllPhrases<HasAnyPhrasesTraits>;
template class ExecutableFunctionHasAnyAllPhrases<HasAllPhrasesTraits>;
template class FunctionBaseHasAnyAllPhrases<HasAnyPhrasesTraits>;
template class FunctionBaseHasAnyAllPhrases<HasAllPhrasesTraits>;
template class FunctionHasAnyAllPhrasesOverloadResolver<HasAnyPhrasesTraits>;
template class FunctionHasAnyAllPhrasesOverloadResolver<HasAllPhrasesTraits>;

REGISTER_FUNCTION(HasAnyPhrases)
{
    FunctionDocumentation::Description description = R"(
Checks if the `input` contains any of the `phrases`, where each phrase is matched as in
[`hasPhrase`](#hasPhrase) (all of the phrase's tokens appear in consecutive order).

It is equivalent to `hasPhrase(input, phrases[1]) OR hasPhrase(input, phrases[2]) OR ...`, but
tokenizes the `input` only once for all phrases instead of once per phrase. The query analyzer
can rewrite a chain of `OR`-ed `hasPhrase` calls on the same column into `hasAnyPhrases`, but this is
disabled by default (setting `optimize_rewrite_has_phrase_or_chain`) because rewriting `hasPhrase` is
not always beneficial when a text index can answer `hasPhrase` directly.

:::note
Column `input` should have a [text index](../../engines/table-engines/mergetree-family/textindexes) defined for optimal performance.
If no text index is defined, the function performs a brute-force column scan which is orders of magnitude slower than an index lookup.
:::

Each phrase is tokenized with the same tokenizer as the `input`. If the column has no text index defined,
the `splitByNonAlpha` tokenizer is used instead - unless a tokenizer is provided as the optional third argument.
The tokenizer argument must be one of `splitByNonAlpha`, `splitByString`, `ngrams`, or `asciiCJK`.
    )";
    FunctionDocumentation::Syntax syntax = "hasAnyPhrases(input, phrases[, tokenizer])";
    FunctionDocumentation::Arguments arguments = {
        {"input", "The input column.", {"String", "FixedString"}},
        {"phrases", "Phrases to search for.", {"const Array(String)"}},
        {"tokenizer", "The tokenizer to use. Optional, defaults to `splitByNonAlpha`.", {"const String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns `1` if any of the phrases is found as a consecutive token sequence, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples
        = {{"Any of several phrases",
            "SELECT hasAnyPhrases('the quick brown fox jumps', ['lazy dog', 'quick brown'])",
            R"(
┌─hasAnyPhrases('the quick brown fox jumps', ['lazy dog', 'quick brown'])─┐
│                                                                      1 │
└────────────────────────────────────────────────────────────────────────┘
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasAnyAllPhrasesOverloadResolver<HasAnyPhrasesTraits>>(documentation);
    factory.registerAlias("hasAnyPhrase", FunctionHasAnyAllPhrasesOverloadResolver<HasAnyPhrasesTraits>::name);
}

REGISTER_FUNCTION(HasAllPhrases)
{
    FunctionDocumentation::Description description = R"(
Checks if the `input` contains all of the `phrases`, where each phrase is matched as in
[`hasPhrase`](#hasPhrase) (all of the phrase's tokens appear in consecutive order).

It is equivalent to `hasPhrase(input, phrases[1]) AND hasPhrase(input, phrases[2]) AND ...`, but
tokenizes the `input` only once for all phrases instead of once per phrase. The query analyzer can
rewrite a chain of `AND`-ed `hasPhrase` calls on the same column into `hasAllPhrases`, but this is
disabled by default (setting `optimize_rewrite_has_phrase_and_chain`): unlike `OR`, an `AND` chain
short-circuits on the first absent phrase, so coalescing it can be slower for selective `AND` filters
on the brute-force (no text index) path.

:::note
Column `input` should have a [text index](../../engines/table-engines/mergetree-family/textindexes) defined for optimal performance.
If no text index is defined, the function performs a brute-force column scan which is orders of magnitude slower than an index lookup.
:::

Each phrase is tokenized with the same tokenizer as the `input`. If the column has no text index defined,
the `splitByNonAlpha` tokenizer is used instead - unless a tokenizer is provided as the optional third argument.
The tokenizer argument must be one of `splitByNonAlpha`, `splitByString`, `ngrams`, or `asciiCJK`.
    )";
    FunctionDocumentation::Syntax syntax = "hasAllPhrases(input, phrases[, tokenizer])";
    FunctionDocumentation::Arguments arguments = {
        {"input", "The input column.", {"String", "FixedString"}},
        {"phrases", "Phrases to search for.", {"const Array(String)"}},
        {"tokenizer", "The tokenizer to use. Optional, defaults to `splitByNonAlpha`.", {"const String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns `1` if every phrase is found as a consecutive token sequence, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples
        = {{"All of several phrases",
            "SELECT hasAllPhrases('the quick brown fox jumps', ['quick brown', 'fox jumps'])",
            R"(
┌─hasAllPhrases('the quick brown fox jumps', ['quick brown', 'fox jumps'])─┐
│                                                                       1 │
└──────────────────────────────────────────────────────────────────────────┘
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasAnyAllPhrasesOverloadResolver<HasAllPhrasesTraits>>(documentation);
    factory.registerAlias("hasAllPhrase", FunctionHasAnyAllPhrasesOverloadResolver<HasAllPhrasesTraits>::name);
}

}
