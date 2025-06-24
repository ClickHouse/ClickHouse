#include <Functions/searchAnyAll.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ITokenExtractor.h>
#include <Common/FunctionDocumentation.h>

#include <algorithm>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_full_text_index;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

template <class SearchTraits>
FunctionPtr FunctionSearchImpl<SearchTraits>::create(ContextPtr context)
{
    return std::make_shared<FunctionSearchImpl>(context);
}

template <class SearchTraits>
FunctionSearchImpl<SearchTraits>::FunctionSearchImpl(ContextPtr context)
    : allow_experimental_full_text_index(context->getSettingsRef()[Setting::allow_experimental_full_text_index])
{
}

template <class SearchTraits>
void FunctionSearchImpl<SearchTraits>::setGinFilterParameters(const GinFilterParameters & params)
{
    /// Index parameters can be set multiple times.
    /// This happens exactly in a case that same searchAny/searchAll query is used again.
    /// This is fine because the parameters would be same.
    if (parameters.has_value() && params != parameters.value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Function '{}': Different index parameters are set.", getName());
    parameters = params;
}

template <class SearchTraits>
DataTypePtr FunctionSearchImpl<SearchTraits>::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    if (!allow_experimental_full_text_index)
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Enable the setting 'allow_experimental_full_text_index' to use function {}", getName());

    FunctionArgumentDescriptors mandatory_args{
        {"input", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"},
        {"needles", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), isColumnConst, "const Array"}};

    validateFunctionArguments(*this, arguments, mandatory_args);

    return std::make_shared<DataTypeNumber<UInt8>>();
}

namespace
{
constexpr size_t arg_input = 0;
constexpr size_t arg_needles = 1;
constexpr size_t supported_number_of_needles = 64;

template <typename StringColumnType>
void executeSearchAny(
    std::unique_ptr<ITokenExtractor> token_extractor,
    StringColumnType & col_input,
    size_t input_rows_count,
    const std::vector<String> & needles,
    PaddedPODArray<UInt8> & col_result)
{
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        const auto value = col_input.getDataAt(i);
        col_result[i] = false;

        const auto & tokens = token_extractor->getTokens(value.data, value.size);
        for (const auto & token : tokens)
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
    size_t input_rows_count,
    const std::vector<String> & needles,
    PaddedPODArray<UInt8> & col_result)
{
    const size_t ns = needles.size();
    /// It is equivalent to ((2 ^ ns) - 1), but avoids overflow in case of ns = 64.
    const UInt64 expected_mask = ((1ULL << (ns - 1)) + ((1ULL << (ns - 1)) - 1));

    UInt64 mask;
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        const auto value = col_input.getDataAt(i);
        col_result[i] = false;

        mask = 0;
        const auto & tokens = token_extractor->getTokens(value.data, value.size);
        for (const auto & token : tokens)
        {
            for (size_t pos = 0; pos < needles.size(); ++pos)
                if (token == needles[pos])
                    mask |= (1 << pos);

            if (mask == expected_mask)
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
    size_t input_rows_count,
    const std::vector<String> & needles,
    PaddedPODArray<UInt8> & col_result)
{
    col_result.resize(input_rows_count);

    switch (SearchTraits::search_mode)
    {
        case GinSearchMode::Any:
            executeSearchAny(std::move(token_extractor), col_input, input_rows_count, needles, col_result);
            break;
        case GinSearchMode::All:
            executeSearchAll(std::move(token_extractor), col_input, input_rows_count, needles, col_result);
            break;
    }
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
    else if (parameters->tokenizer == NgramTokenExtractor::getExternalName())
    {
        auto ngrams = parameters->ngram_size.value_or(DEFAULT_NGRAM_SIZE);
        if (ngrams < 2 || ngrams > 8)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Ngrams argument of function '{}' should be between 2 and 8, got: {}", name, ngrams);
        token_extractor = std::make_unique<NgramTokenExtractor>(ngrams);
    }
    else if (parameters->tokenizer == SplitTokenExtractor::getExternalName())
    {
        const auto & separators = parameters->separators.value_or(std::vector<String>{" "});
        token_extractor = std::make_unique<SplitTokenExtractor>(separators);
    }
    else if (parameters->tokenizer == NoOpTokenExtractor::getExternalName())
        token_extractor = std::make_unique<NoOpTokenExtractor>();
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function '{}' supports only tokenizers 'default', 'ngram', 'split', and 'no_op'", name);

    std::vector<String> needles;
    if (const ColumnConst * col_needles_const = checkAndGetColumnConst<ColumnArray>(col_needles.get()))
    {
        for (const auto & needle_field : col_needles_const->getValue<Array>())
        {
            const auto & needle = needle_field.safeGet<String>();
            const auto & tokens = token_extractor->getTokens(needle.data(), needle.size());
            for (const auto & token : tokens)
                needles.emplace_back(token);
        }
    }
    else
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Needles argument of function '{}' should be Array(String), got: {}",
            name,
            col_needles->getFamilyName());

    if (needles.size() > supported_number_of_needles)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function '{}' supports a max of {} needles", name, supported_number_of_needles);

    if (const auto * column_string = checkAndGetColumn<ColumnString>(col_input.get()))
        execute<SearchTraits>(std::move(token_extractor), *column_string, input_rows_count, needles, col_result->getData());
    else if (const auto * column_fixed_string = checkAndGetColumn<ColumnFixedString>(col_input.get()))
        execute<SearchTraits>(std::move(token_extractor), *column_fixed_string, input_rows_count, needles, col_result->getData());

    return col_result;
}

template class FunctionSearchImpl<traits::SearchAnyTraits>;
template class FunctionSearchImpl<traits::SearchAllTraits>;

FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
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
