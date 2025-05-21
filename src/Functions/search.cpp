#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/GinFilter.h>
#include <Interpreters/ITokenExtractor.h>

#include <roaring.hh>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

struct SearchAnyProps
{
    static constexpr String name = "searchAny";
    static constexpr GinSearchMode search_mode = GinSearchMode::ANY;
};

struct SearchAllProps
{
    static constexpr String name = "searchAll";
    static constexpr GinSearchMode search_mode = GinSearchMode::ALL;
};

namespace
{
template <typename Type>
std::optional<Type> getArgument(const ColumnsWithTypeAndName & arguments, size_t index)
{
    if (index < arguments.size())
    {
        if constexpr (std::is_same_v<Type, UInt64>)
            return arguments[index].column->getUInt(0);
        else if constexpr (std::is_same_v<Type, String>)
            return arguments[index].column->getDataAt(0).toString();
        else if constexpr (std::is_same_v<Type, std::string_view>)
            return arguments[index].column->getDataAt(0).toView();
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Search function argument at index '{}' expected to be any of UInt64, String, or std::string_view",
                index);
        }
    }
    return std::nullopt;
}
}

template <typename SearchProps>
class FunctionSearchImpl : public IFunction
{
    static constexpr size_t arg_input = 0;
    static constexpr size_t arg_needle = 1;
    static constexpr size_t arg_tokenizer = 2;
    static constexpr size_t arg_ngrams = 3;

public:
    static constexpr auto name = SearchProps::name;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionSearchImpl>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"input", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"},
            {"needle", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "String"}};

        FunctionArgumentDescriptors optional_args;

        if (arguments.size() > 2)
        {
            optional_args.emplace_back("tokenizer", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "String");

            validateFunctionArguments(
                *this, {arguments[arg_input], arguments[arg_needle], arguments[arg_tokenizer]}, mandatory_args, optional_args);

            if (arguments.size() == 4)
            {
                const auto tokenizer = arguments[arg_tokenizer].column->getDataAt(0).toString();

                if (tokenizer == NgramTokenExtractor::getExternalName())
                    optional_args.emplace_back("ngrams", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt8), isColumnConst, "UInt8");
            }
        }

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeNumber<UInt8>>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_input = arguments[arg_input].column;
        auto col_needle = arguments[arg_needle].column;
        auto col_result = ColumnVector<UInt8>::create();

        if (input_rows_count == 0)
            return col_result;

        const auto tokenizer_arg = getArgument<std::string_view>(arguments, arg_tokenizer).value_or(SplitTokenExtractor::getExternalName());

        std::unique_ptr<ITokenExtractor> token_extractor;
        if (tokenizer_arg == SplitTokenExtractor::getExternalName())
            token_extractor = std::make_unique<SplitTokenExtractor>();
        else if (tokenizer_arg == NoOpTokenExtractor::getExternalName())
            token_extractor = std::make_unique<NoOpTokenExtractor>();
        else if (tokenizer_arg == NgramTokenExtractor::getExternalName())
        {
            auto ngrams = getArgument<UInt64>(arguments, arg_ngrams).value_or(3);
            if (ngrams < 2 || ngrams > 8)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Ngrams argument of function {} should be between 2 and 8, got: {}", name, ngrams);
            token_extractor = std::make_unique<NgramTokenExtractor>(ngrams);
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function '{}' supports only tokenizers 'default', 'ngram', and 'noop'", name);
        }

        const auto & col_needle_tokens = col_needle->getDataAt(0);
        std::vector<String> needle_tokens = SplitTokenExtractor().getTokens(col_needle_tokens.data, col_needle_tokens.size);

        if (const auto * column_string = checkAndGetColumn<ColumnString>(col_input.get()))
            executeImpl(std::move(token_extractor), *column_string, input_rows_count, needle_tokens, col_result->getData());
        else if (const auto * column_fixed_string = checkAndGetColumn<ColumnFixedString>(col_input.get()))
            executeImpl(std::move(token_extractor), *column_fixed_string, input_rows_count, needle_tokens, col_result->getData());

        return col_result;
    }

private:
    template <typename StringColumnType>
    void executeImpl(
        std::unique_ptr<ITokenExtractor> token_extractor,
        StringColumnType & col_input,
        size_t rows_count_input,
        const std::vector<String>& needle_tokens,
        PaddedPODArray<UInt8> & col_result) const
    {
        col_result.resize(rows_count_input);

        for (size_t i = 0; i < rows_count_input; ++i)
        {
            const auto value{col_input.getDataAt(i)};

            col_result[i] = false;

            [[maybe_unused]] roaring::Roaring mask;
            for (const auto& token : token_extractor->getTokens(value.data, value.size))
            {
                for (size_t pos = 0; pos < needle_tokens.size(); ++pos) {
                    if (token == needle_tokens[pos]) {
                        if constexpr (SearchProps::search_mode == GinSearchMode::ALL)
                        {
                            mask.add(pos);
                        }
                        else
                        {
                            col_result[i] = true;
                            break;
                        }
                    }
                }
            }
            if constexpr (SearchProps::search_mode == GinSearchMode::ALL)
                col_result[i] = mask.cardinality() == needle_tokens.size();
        }
    }
};

REGISTER_FUNCTION(SearchAny)
{
    factory.registerFunction<FunctionSearchImpl<SearchAnyProps>>(FunctionDocumentation{
        .description = "Searches the needle tokens in the generated tokens from the text by a given tokenizer. Returns true if any needle "
                       "tokens exists in the text, otherwise false.",
        .category = FunctionDocumentation::Category::StringSearch});
}

REGISTER_FUNCTION(SearchAll)
{
    factory.registerFunction<FunctionSearchImpl<SearchAllProps>>(FunctionDocumentation{
        .description = "Searches the needle tokens in the generated tokens from the text by a given tokenizer. Returns true if all needle "
                       "tokens exists in the text, otherwise false.",
        .category = FunctionDocumentation::Category::StringSearch});
}
}
