#include "config.h"

#include <Functions/hasPhrase.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/TokenSearchArgumentTypes.h>
#include <Interpreters/Context.h>
#include <Interpreters/ITokenizer.h>
#include <Interpreters/TokenizerFactory.h>
#include <Common/FunctionDocumentation.h>
#include <Common/UnorderedSetWithMemoryTracking.h>

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
constexpr size_t arg_phrase = 1;
constexpr size_t arg_tokenizer = 2;

VectorWithMemoryTracking<String> initializePhraseTokens(const ColumnsWithTypeAndName & arguments, const ITokenizer & tokenizer, std::string_view function_name)
{
    auto column_phrase = arguments[arg_phrase].column;

    Field phrase_field = (*column_phrase)[0];

    /// An Array phrase is a token sequence used as-is.
    if (phrase_field.getType() == Field::Types::Array)
    {
        VectorWithMemoryTracking<String> tokens;
        for (const Field & element : phrase_field.safeGet<Array>())
        {
            if (element.getType() != Field::Types::String)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function '{}' requires the elements of an Array phrase argument to be String, got: {}",
                    function_name,
                    element.getTypeName());

            if (!element.safeGet<String>().empty())
                tokens.push_back(element.safeGet<String>());
        }
        return tokens;
    }

    if (phrase_field.isNull() || phrase_field.getType() != Field::Types::String)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Function '{}' requires a String phrase argument, got: {}",
            function_name,
            column_phrase->getFamilyName());

    auto phrase_str = phrase_field.safeGet<String>();

    /// Tokenize the phrase, preserving order (no deduplication).
    VectorWithMemoryTracking<String> tokens;
    tokenizer.stringToTokens(phrase_str.data(), phrase_str.size(), tokens);
    return tokens;
}

/// KMP style failure array.
/// For example, phrase "a a b" in input "a a a b" correctly matches at positions 1-3.
VectorWithMemoryTracking<size_t> buildFailureFunction(const VectorWithMemoryTracking<String> & phrase_tokens)
{
    const size_t size = phrase_tokens.size();
    VectorWithMemoryTracking<size_t> failure(size, 0);

    size_t k = 0;
    for (size_t i = 1; i < size; ++i)
    {
        while (k > 0 && phrase_tokens[k] != phrase_tokens[i])
            k = failure[k - 1];

        if (phrase_tokens[k] == phrase_tokens[i])
            ++k;

        failure[i] = k;
    }

    return failure;
}

/// Matcher that checks if all phrase tokens appear consecutively in the input's token stream.
struct MatchPhraseMatcher
{
    MatchPhraseMatcher(const VectorWithMemoryTracking<String> & phrase_tokens_, const VectorWithMemoryTracking<size_t> & failure_)
        : phrase_tokens(phrase_tokens_)
        , failure(failure_)
        , match_position(0)
    {
    }

    template <typename OnMatchCallback>
    auto operator()(OnMatchCallback && onMatchCallback)
    {
        return [&](const char * token_start, size_t token_len)
        {
            std::string_view current_token(token_start, token_len);

            /// Follow failure links until we find a match or exhaust the chain.
            while (match_position > 0 && current_token != phrase_tokens[match_position])
                match_position = failure[match_position - 1];

            if (current_token == phrase_tokens[match_position])
            {
                ++match_position;
                if (match_position == phrase_tokens.size())
                {
                    onMatchCallback();
                    return true;
                }
            }

            return false;
        };
    }

    void reset() { match_position = 0; }

private:
    const VectorWithMemoryTracking<String> & phrase_tokens;
    const VectorWithMemoryTracking<size_t> & failure;
    size_t match_position;
};

template <typename StringColumn>
requires std::same_as<StringColumn, ColumnString> || std::same_as<StringColumn, ColumnFixedString>
void executeMatchPhrase(
    const StringColumn & col_input,
    PaddedPODArray<UInt8> & col_result,
    size_t input_rows_count,
    const ITokenizer * tokenizer,
    const VectorWithMemoryTracking<String> & phrase_tokens,
    const VectorWithMemoryTracking<size_t> & failure_table)
{
    MatchPhraseMatcher matcher(phrase_tokens, failure_table);

    col_result.resize(input_rows_count);

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        std::string_view input = col_input.getDataAt(i);
        col_result[i] = 0;
        matcher.reset();

        forEachToken(*tokenizer, input.data(), input.size(), matcher([&] { col_result[i] = 1; }));
    }
}

/// The elements of an array input are the tokens themselves and are never tokenized.
template <typename StringColumn>
requires std::same_as<StringColumn, ColumnString> || std::same_as<StringColumn, ColumnFixedString>
void executeMatchPhraseOnArray(
    const ColumnArray & col_input,
    const StringColumn & col_elements,
    const ColumnNullable * elements_nullable,
    PaddedPODArray<UInt8> & col_result,
    size_t input_rows_count,
    const VectorWithMemoryTracking<String> & phrase_tokens,
    const VectorWithMemoryTracking<size_t> & failure_table)
{
    MatchPhraseMatcher matcher(phrase_tokens, failure_table);
    const auto & offsets = col_input.getOffsets();

    col_result.resize(input_rows_count);

    ColumnArray::Offset current_offset = 0;
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        const ColumnArray::Offset array_size = offsets[i] - current_offset;
        col_result[i] = 0;
        matcher.reset();

        for (ColumnArray::Offset j = 0; j < array_size; ++j)
        {
            const size_t element_index = current_offset + j;
            if (elements_nullable && elements_nullable->isNullAt(element_index))
                continue;

            /// An empty element is not a token; the index has no position for one.
            std::string_view element = col_elements.getDataAt(element_index);
            if (element.empty())
                continue;

            if (matcher([&] { col_result[i] = 1; })(element.data(), element.size()))
                break;
        }

        current_offset = offsets[i];
    }
}
}

FunctionHasPhraseOverloadResolver::FunctionHasPhraseOverloadResolver(ContextPtr)
{
}

DataTypePtr FunctionHasPhraseOverloadResolver::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    FunctionArgumentDescriptors mandatory_args{
        {"input",
         static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedStringOrArrayOfStringOrFixedString),
         nullptr,
         "String, FixedString, Array(String) or Array(FixedString)"},
        {"phrase",
         static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrArrayOfStringType),
         isColumnConst,
         "const String or const Array(String)"}};

    FunctionArgumentDescriptors optional_args{
        {"tokenizer", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"}};

    validateFunctionArguments(name, arguments, mandatory_args, optional_args);

    return std::make_shared<DataTypeNumber<UInt8>>();
}

FunctionBasePtr
FunctionHasPhraseOverloadResolver::buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const
{
    if (arguments.size() < 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function '{}' requires at least 2 arguments, got {}", name, arguments.size());

    if (!isStringOrArrayOfStringType(*arguments[arg_phrase].type))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "A value of illegal type was provided as 2nd argument 'phrase' to function '{}'. "
            "Expected: const String or const Array(String), got: {}",
            name,
            arguments[arg_phrase].type->getName());

    if (!arguments[arg_phrase].column || !isColumnConst(*arguments[arg_phrase].column))
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "A value of illegal type was provided as 2nd argument 'phrase' to function '{}'. "
            "Expected: const String or const Array(String), got: {}",
            name,
            arguments[arg_phrase].type->getName());

    DataTypes argument_types{std::from_range_t{}, arguments | std::views::transform([](auto & elem) { return elem.type; })};

    const auto tokenizer_name = arguments.size() < 3 || !arguments[arg_tokenizer].column ? SplitByNonAlphaTokenizer::getExternalName()
                                                                                         : arguments[arg_tokenizer].column->getDataAt(0);
    auto tokenizer = TokenizerFactory::instance().get(tokenizer_name);
    static const UnorderedSetWithMemoryTracking<ITokenizer::Type> supported_types = {
        ITokenizer::Type::SplitByNonAlpha,
        ITokenizer::Type::SplitByString,
        ITokenizer::Type::SplitByRegexp,
        ITokenizer::Type::AsciiCJK,
#if USE_ICU
        ITokenizer::Type::Icu,
#endif
        ITokenizer::Type::Ngrams,
    };

    if (!supported_types.contains(tokenizer->getType()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function '{}' does not support the '{}' tokenizer.", name, tokenizer_name);

    auto phrase_tokens = initializePhraseTokens(arguments, *tokenizer, getName());
    return std::make_shared<FunctionBaseHasPhrase>(std::move(tokenizer), std::move(phrase_tokens), std::move(argument_types), return_type);
}

ExecutableFunctionPtr FunctionBaseHasPhrase::prepare(const ColumnsWithTypeAndName &) const
{
    auto failure_table = buildFailureFunction(phrase_tokens);
    return std::make_unique<ExecutableFunctionHasPhrase>(tokenizer, phrase_tokens, std::move(failure_table));
}

ColumnPtr
ExecutableFunctionHasPhrase::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
{
    if (input_rows_count == 0)
        return ColumnVector<UInt8>::create();

    auto col_result = ColumnVector<UInt8>::create();
    if (phrase_tokens.empty())
    {
        col_result->getData().assign(input_rows_count, UInt8(0));
        return col_result;
    }

    ColumnPtr col_input = arguments[arg_input].column;
    if (const auto * col_input_string = checkAndGetColumn<ColumnString>(col_input.get()))
        executeMatchPhrase(*col_input_string, col_result->getData(), input_rows_count, tokenizer.get(), phrase_tokens, failure_table);
    else if (const auto * col_input_fixedstring = checkAndGetColumn<ColumnFixedString>(col_input.get()))
        executeMatchPhrase(*col_input_fixedstring, col_result->getData(), input_rows_count, tokenizer.get(), phrase_tokens, failure_table);
    else if (const auto * col_input_array = checkAndGetColumn<ColumnArray>(col_input.get()))
    {
        const IColumn * elements = &col_input_array->getData();
        const auto * elements_nullable = checkAndGetColumn<ColumnNullable>(elements);
        if (elements_nullable)
            elements = &elements_nullable->getNestedColumn();

        if (const auto * col_elements_string = checkAndGetColumn<ColumnString>(elements))
            executeMatchPhraseOnArray(
                *col_input_array,
                *col_elements_string,
                elements_nullable,
                col_result->getData(),
                input_rows_count,
                phrase_tokens,
                failure_table);
        else if (const auto * col_elements_fixedstring = checkAndGetColumn<ColumnFixedString>(elements))
            executeMatchPhraseOnArray(
                *col_input_array,
                *col_elements_fixedstring,
                elements_nullable,
                col_result->getData(),
                input_rows_count,
                phrase_tokens,
                failure_table);
    }

    return col_result;
}

REGISTER_FUNCTION(HasPhrase)
{
    FunctionDocumentation::Description description = R"(
Checks if the `input` contains all tokens from the `phrase` in consecutive order.

<Note>
Column `input` should have a [text index](/reference/engines/table-engines/mergetree-family/textindexes) defined for optimal performance.
If no text index is defined, the function performs a brute-force column scan which is orders of magnitude slower than an index lookup.
</Note>

Prior to searching, the function tokenizes both the `input` and the `phrase` arguments using the tokenizer specified for the text index.
If the column has no text index defined, the `splitByNonAlpha` tokenizer is used instead — unless a tokenizer is provided as the optional third argument.
The tokenizer argument must be one of `splitByNonAlpha`, `splitByString`, `splitByRegexp`, `ngrams`, `asciiCJK`, or `icu`.
Note that `splitByRegexp` is not supported for `hasPhrase` when the text index also defines a postprocessor.

If `input` is an [Array(String)](/reference/data-types/array), its elements are the tokens themselves and are not tokenized,
so `hasPhrase(['quick', 'brown'], 'quick brown')` matches. A `String` `phrase` is still tokenized; if `phrase` is an
[Array(String)](/reference/data-types/array), its elements are the tokens to search for, in order and including duplicates.
Empty elements are ignored on both sides, because no tokenizer produces an empty token.

<Note>
When a text index defines a [preprocessor](/reference/engines/table-engines/mergetree-family/textindexes#creating-a-text-index) (for example `lowerUTF8`), `hasPhrase` applies it to both `input` and `phrase` before tokenization.
The preprocessor is only applied on the text index path, so results may differ between queries that use the text index and queries that do not (e.g. `SETTINGS use_skip_indexes = 0`).
This inconsistency is tolerated to improve the usability of full-text search.
</Note>

Unlike [`hasToken`](#hasToken), [`hasAnyTokens`](#hasAnyTokens) and [`hasAllTokens`](#hasAllTokens), `hasPhrase` requires the tokens to appear in the same order
and without any intervening tokens. For example, `hasPhrase('the quick brown fox', 'quick fox')` returns 0
because "brown" appears between "quick" and "fox".
    )";
    FunctionDocumentation::Syntax syntax = "hasPhrase(input, phrase[, tokenizer])";
    FunctionDocumentation::Arguments arguments = {
        {"input",
         "The input column.",
         {"String",
          "FixedString",
          "Nullable(String)",
          "Nullable(FixedString)",
          "Array(String)",
          "Array(FixedString)",
          "Array(Nullable(String))",
          "Array(Nullable(FixedString))"}},
        {"phrase", "Phrase to search for.", {"const String", "const Array(String)"}},
        {"tokenizer", "The tokenizer to use. Optional, defaults to `splitByNonAlpha`.", {"const String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns `1` if the phrase is found as a consecutive token sequence, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples
        = {{"Phrase match",
            "SELECT hasPhrase('the quick brown fox jumps', 'quick brown')",
            R"(
┌─hasPhrase('the quick brown fox jumps', 'quick brown')─┐
│                                                     1 │
└───────────────────────────────────────────────────────┘
        )"},
           {"Non-consecutive tokens",
            "SELECT hasPhrase('the quick brown fox jumps', 'quick fox')",
            R"(
┌─hasPhrase('the quick brown fox jumps', 'quick fox')─┐
│                                                   0 │
└─────────────────────────────────────────────────────┘
        )"},
           {"Token sequence given as arrays",
            "SELECT hasPhrase(['a', 'b', 'c'], ['a', 'b'])",
            R"(
┌─hasPhrase(['a', 'b', 'c'], ['a', 'b'])─┐
│                                      1 │
└────────────────────────────────────────┘
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasPhraseOverloadResolver>(documentation);
    factory.registerAlias("matchPhrase", FunctionHasPhraseOverloadResolver::name);
}

}
