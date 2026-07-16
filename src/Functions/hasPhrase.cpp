#include <Functions/hasPhrase.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ITokenizer.h>
#include <Interpreters/TokenizerFactory.h>
#include <Common/FunctionDocumentation.h>

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

std::vector<String> initializePhraseTokens(const ColumnsWithTypeAndName & arguments, const ITokenizer & tokenizer, std::string_view function_name)
{
    auto column_phrase = arguments[arg_phrase].column;

    Field phrase_field = (*column_phrase)[0];
    if (phrase_field.isNull() || phrase_field.getType() != Field::Types::String)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Function '{}' requires a String phrase argument, got: {}",
            function_name,
            column_phrase->getFamilyName());

    auto phrase_str = phrase_field.safeGet<String>();

    /// Tokenize the phrase, preserving order (no deduplication).
    std::vector<String> tokens;
    tokenizer.stringToTokens(phrase_str.data(), phrase_str.size(), tokens);
    return tokens;
}

/// KMP style failure array.
/// For example, phrase "a a b" in input "a a a b" correctly matches at positions 1-3.
std::vector<size_t> buildFailureFunction(const std::vector<String> & phrase_tokens)
{
    const size_t size = phrase_tokens.size();
    std::vector<size_t> failure(size, 0);

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
    MatchPhraseMatcher(const std::vector<String> & phrase_tokens_, const std::vector<size_t> & failure_)
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
    const std::vector<String> & phrase_tokens;
    const std::vector<size_t> & failure;
    size_t match_position;
};

template <typename StringColumn>
requires std::same_as<StringColumn, ColumnString> || std::same_as<StringColumn, ColumnFixedString>
void executeMatchPhrase(
    const StringColumn & col_input,
    PaddedPODArray<UInt8> & col_result,
    size_t input_rows_count,
    const ITokenizer * tokenizer,
    const std::vector<String> & phrase_tokens,
    const std::vector<size_t> & failure_table)
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
}

FunctionHasPhraseOverloadResolver::FunctionHasPhraseOverloadResolver(ContextPtr)
{
}

DataTypePtr FunctionHasPhraseOverloadResolver::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    FunctionArgumentDescriptors mandatory_args{
        {"input", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"},
        {"phrase", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"}};

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

    if (!isString(arguments[arg_phrase].type))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "A value of illegal type was provided as 2nd argument 'phrase' to function '{}'. Expected: const String, got: {}",
            name,
            arguments[arg_phrase].type->getName());

    if (!arguments[arg_phrase].column || !isColumnConst(*arguments[arg_phrase].column))
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "A value of illegal type was provided as 2nd argument 'phrase' to function '{}'. Expected: const String, got: {}",
            name,
            arguments[arg_phrase].type->getName());

    DataTypes argument_types{std::from_range_t{}, arguments | std::views::transform([](auto & elem) { return elem.type; })};

    const auto tokenizer_name = arguments.size() < 3 || !arguments[arg_tokenizer].column ? SplitByNonAlphaTokenizer::getExternalName()
                                                                                         : arguments[arg_tokenizer].column->getDataAt(0);
    auto tokenizer = TokenizerFactory::instance().get(tokenizer_name);
    static const std::unordered_set<ITokenizer::Type> supported_types = {
        ITokenizer::Type::SplitByNonAlpha,
        ITokenizer::Type::SplitByString,
        ITokenizer::Type::AsciiCJK,
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

    return col_result;
}

REGISTER_FUNCTION(HasPhrase)
{
    FunctionDocumentation::Description description = R"(
Checks if the haystack contains all tokens from the phrase in consecutive order.

Prior to searching, the function tokenizes both the `input` and the `phrase` arguments using the tokenizer specified as the optional third argument.
The tokenizer argument must be one of `splitByNonAlpha`, `splitByString`, `ngrams`, or `asciiCJK`.
If no tokenizer is specified, by default the `splitByNonAlpha` tokenizer would be used.

Unlike [`hasToken`](#hasToken), [`hasAnyTokens`](#hasAnyTokens) and [`hasAllTokens`](#hasAllTokens), `hasPhrase` requires the tokens to appear in the same order
and without any intervening tokens. For example, `hasPhrase('the quick brown fox', 'quick fox')` returns 0
because "brown" appears between "quick" and "fox".
    )";
    FunctionDocumentation::Syntax syntax = "hasPhrase(input, phrase[, tokenizer])";
    FunctionDocumentation::Arguments arguments = {
        {"input", "The input column.", {"String", "FixedString"}},
        {"phrase", "Phrase to search for.", {"const String"}},
        {"tokenizer", "The tokenizer to use. Optional, defaults to `splitByNonAlpha`.", {"const String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns `1` if the phrase is found as a consecutive token sequence, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples
        = {{"Phrase match",
            "SELECT hasPhrase('the quick brown fox jumps', 'quick brown')",
            R"(
┌─hasPhrase('the quick brown fox jumps', 'quick brown')─┐
│                                                      1 │
└────────────────────────────────────────────────────────┘
        )"},
           {"Non-consecutive tokens",
            "SELECT hasPhrase('the quick brown fox jumps', 'quick fox')",
            R"(
┌─hasPhrase('the quick brown fox jumps', 'quick fox')─┐
│                                                    0 │
└──────────────────────────────────────────────────────┘
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasPhraseOverloadResolver>(documentation);
    factory.registerAlias("matchPhrase", FunctionHasPhraseOverloadResolver::name);
}

}
