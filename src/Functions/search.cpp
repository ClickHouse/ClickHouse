#include <Functions/search.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/ITokenExtractor.h>
#include <Common/FunctionDocumentation.h>

#include <algorithm>
#include <roaring.hh>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

template <class SearchTraits>
DataTypePtr FunctionSearchImpl<SearchTraits>::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    FunctionArgumentDescriptors mandatory_args{
        {"input", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"},
        {"needles", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"}};

    validateFunctionArguments(*this, arguments, mandatory_args);

    return std::make_shared<DataTypeNumber<UInt8>>();
}

namespace
{
constexpr size_t arg_input = 0;
constexpr size_t arg_needles = 1;

template <typename StringColumnType>
void executeSearchAny(
    std::unique_ptr<ITokenExtractor> token_extractor,
    StringColumnType & col_input,
    size_t rows_count_input,
    const std::vector<String> & needles,
    PaddedPODArray<UInt8> & col_result)
{
    for (size_t i = 0; i < rows_count_input; ++i)
    {
        const auto value = col_input.getDataAt(i);
        col_result[i] = false;
        for (const auto & token : token_extractor->getTokens(value.data, value.size))
        {
            if (std::ranges::any_of(needles, [&token](const auto & needle) { return needle == token; }))
            {
                col_result[i] = true;
                break;
            }
        }
    }
}

template <typename StringColumnType>
void executeSearchAll(
    std::unique_ptr<ITokenExtractor> token_extractor,
    StringColumnType & col_input,
    size_t rows_count_input,
    const std::vector<String> & needles,
    PaddedPODArray<UInt8> & col_result)
{
    static roaring::Roaring mask_zero;

    roaring::Roaring mask;
    for (size_t i = 0; i < rows_count_input; ++i)
    {
        const auto value = col_input.getDataAt(i);
        col_result[i] = false;

        mask &= mask_zero;
        for (const auto & token : token_extractor->getTokens(value.data, value.size))
        {
            for (size_t pos = 0; pos < needles.size(); ++pos)
                if (token == needles[pos])
                    mask.add(pos);

            if (mask.cardinality() == needles.size())
            {
                col_result[i] = true;
                break;
            }
        }
    }
}

template <class SearchTraits, typename StringColumnType>
void execute(
    std::unique_ptr<ITokenExtractor> token_extractor,
    StringColumnType & col_input,
    size_t rows_count_input,
    const std::vector<String> & needles,
    PaddedPODArray<UInt8> & col_result)
{
    col_result.resize(rows_count_input);

    if constexpr (std::is_same_v<SearchTraits, traits::SearchAnyTraits>)
        executeSearchAny(std::move(token_extractor), col_input, rows_count_input, needles, col_result);
    else if constexpr (std::is_same_v<SearchTraits, traits::SearchAllTraits>)
        executeSearchAll(std::move(token_extractor), col_input, rows_count_input, needles, col_result);
}
}

template <class SearchTraits>
ColumnPtr FunctionSearchImpl<SearchTraits>::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
{
    if (input_rows_count == 0)
        return ColumnVector<UInt8>::create();

    if (!parameters.has_value())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Function '{}' should be used with the index column, but got column '{}'",
            getName(),
            arguments[arg_input].name);

    auto col_input = arguments[arg_input].column;
    auto col_needles = arguments[arg_needles].column;
    auto col_result = ColumnVector<UInt8>::create();

    std::unique_ptr<ITokenExtractor> token_extractor;
    if (parameters->tokenizer == DefaultTokenExtractor::getExternalName())
        token_extractor = std::make_unique<DefaultTokenExtractor>();
    else if (parameters->tokenizer == NoOpTokenExtractor::getExternalName())
        token_extractor = std::make_unique<NoOpTokenExtractor>();
    else if (parameters->tokenizer == SplitTokenExtractor::getExternalName())
    {
        const auto& separators = parameters->separators.value_or(std::vector<String>{" "});
        token_extractor = std::make_unique<SplitTokenExtractor>(separators);
    }
    else if (parameters->tokenizer == NgramTokenExtractor::getExternalName())
    {
        auto ngrams = parameters->ngram_size.value_or(DEFAULT_NGRAM_SIZE);
        if (ngrams < 2 || ngrams > 8)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Ngrams argument of function '{}' should be between 2 and 8, got: {}", name, ngrams);
        token_extractor = std::make_unique<NgramTokenExtractor>(ngrams);
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function '{}' supports only tokenizers 'default', 'ngram', 'split', and 'no_op'", name);

    const auto & col_needles_tokens = col_needles->getDataAt(0);
    std::vector<String> needles = DefaultTokenExtractor().getTokens(col_needles_tokens.data, col_needles_tokens.size);

    if (const auto * column_string = checkAndGetColumn<ColumnString>(col_input.get()))
        execute<SearchTraits>(std::move(token_extractor), *column_string, input_rows_count, needles, col_result->getData());
    else if (const auto * column_fixed_string = checkAndGetColumn<ColumnFixedString>(col_input.get()))
        execute<SearchTraits>(std::move(token_extractor), *column_fixed_string, input_rows_count, needles, col_result->getData());

    return col_result;
}

FunctionDocumentation::IntroducedIn introduced_in = {25, 6};
FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;

REGISTER_FUNCTION(SearchAny)
{
    factory.registerFunction<FunctionSearchImpl<traits::SearchAnyTraits>>(FunctionDocumentation{
        .description = "Searches the needle tokens in the generated tokens from the text by a given tokenizer. Returns true if any needle "
                       "tokens exists in the text, otherwise false.",
        .introduced_in = introduced_in,
        .category = category});
}

REGISTER_FUNCTION(SearchAll)
{
    factory.registerFunction<FunctionSearchImpl<traits::SearchAllTraits>>(FunctionDocumentation{
        .description = "Searches the needle tokens in the generated tokens from the text by a given tokenizer. Returns true if all needle "
                       "tokens exists in the text, otherwise false.",
        .introduced_in = introduced_in,
        .category = category});
}
}
