#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <Columns/ColumnString.h>
#include <Common/FunctionDocumentation.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Interpreters/ITokenizer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Tokenize a phrase string into an ordered list of tokens.
std::vector<String> tokenizePhrase(const String & phrase, const ITokenizer & tokenizer)
{
    std::vector<String> tokens;
    tokenizer.stringToTokens(phrase.data(), phrase.size(), tokens);
    return tokens;
}

/// Check if `phrase_tokens` appear adjacently in order within the haystack.
/// Scans the haystack token stream, tracking consecutive phrase token matches.
bool hasPhraseInString(std::string_view haystack, const std::vector<String> & phrase_tokens, const ITokenizer & tokenizer)
{
    if (phrase_tokens.empty())
        return true;

    if (phrase_tokens.size() == 1)
    {
        /// Single token: just check existence.
        bool found = false;
        forEachToken(tokenizer, haystack.data(), haystack.size(),
            [&](const char * token_start, size_t token_len)
            {
                if (std::string_view(token_start, token_len) == phrase_tokens[0])
                {
                    found = true;
                    return true;
                }
                return false;
            });
        return found;
    }

    /// Multi-token phrase: scan and track consecutive matches.
    size_t match_idx = 0;
    bool found = false;

    forEachToken(tokenizer, haystack.data(), haystack.size(),
        [&](const char * token_start, size_t token_len)
        {
            std::string_view token(token_start, token_len);

            if (token == phrase_tokens[match_idx])
            {
                ++match_idx;
                if (match_idx == phrase_tokens.size())
                {
                    found = true;
                    return true;
                }
            }
            else
            {
                /// Reset, but check if current token matches the first phrase token.
                match_idx = (token == phrase_tokens[0]) ? 1 : 0;
            }
            return false;
        });

    return found;
}

} /// anonymous namespace

/// hasPhrase(haystack, phrase) -> UInt8
/// Returns 1 if the phrase (all tokens in order, adjacent) is found in the haystack.
/// Always uses splitByNonAlpha tokenizer, consistent with hasToken.
class FunctionHasPhrase : public IFunction
{
public:
    static constexpr auto name = "hasPhrase";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionHasPhrase>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args
        {
            {"haystack", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"},
            {"phrase", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"}
        };

        validateFunctionArguments(name, arguments, mandatory_args);

        DataTypePtr return_type = std::make_shared<DataTypeNumber<UInt8>>();
        if (arguments[0].type->isNullable())
            return_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNumber<UInt8>>());
        return return_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return ColumnVector<UInt8>::create();

        SplitByNonAlphaTokenizer tokenizer;

        /// Extract phrase tokens.
        auto phrase_column = arguments[1].column;
        String phrase_str = (*phrase_column)[0].safeGet<String>();
        auto phrase_tokens = tokenizePhrase(phrase_str, tokenizer);

        /// Empty phrase matches everything.
        if (phrase_tokens.empty())
        {
            auto col_result = ColumnVector<UInt8>::create();
            col_result->getData().assign(input_rows_count, UInt8(1));
            return col_result;
        }

        auto col_result = ColumnVector<UInt8>::create();
        auto & result_data = col_result->getData();
        result_data.resize(input_rows_count);

        ColumnPtr col_input = arguments[0].column;
        const auto * col_string = checkAndGetColumn<ColumnString>(col_input.get());
        if (!col_string)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function {} requires String column as first argument", getName());

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view input = col_string->getDataAt(i);
            result_data[i] = hasPhraseInString(input, phrase_tokens, tokenizer) ? 1 : 0;
        }

        return col_result;
    }
};

REGISTER_FUNCTION(HasPhrase)
{
    FunctionDocumentation::Description description = R"(
Checks if a phrase (sequence of adjacent tokens) is present in the haystack string.

The phrase is tokenized and the function checks if all tokens appear consecutively
and in order in the haystack.

:::note
Column `haystack` should have a [text index](../../engines/table-engines/mergetree-family/textindexes)
for optimal performance. If no text index is defined, the function performs a brute-force scan.
:::
    )";
    FunctionDocumentation::Syntax syntax = "hasPhrase(haystack, phrase)";
    FunctionDocumentation::Arguments doc_arguments = {
        {"haystack", "String to be searched.", {"String"}},
        {"phrase", "Phrase to search for. Will be tokenized using splitByNonAlpha.", {"const String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns 1 if the phrase is found, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Phrase search",
        "SELECT hasPhrase('the quick brown fox', 'quick brown')",
        R"(
┌─hasPhrase('the quick brown fox', 'quick brown')─┐
│                                                1 │
└──────────────────────────────────────────────────┘
        )"
    },
    {
        "Order matters",
        "SELECT hasPhrase('the quick brown fox', 'brown quick')",
        R"(
┌─hasPhrase('the quick brown fox', 'brown quick')─┐
│                                                0 │
└──────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, doc_arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasPhrase>(documentation);
}

}
