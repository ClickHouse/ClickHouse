#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ITokenExtractor.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/predicate.hpp>

namespace DB
{

namespace details
{
static constexpr std::string_view TOKENIZER_DEFAULT = "default";
static constexpr std::string_view TOKENIZER_NGRAM = "ngram";
static constexpr std::string_view TOKENIZER_NOOP = "noop";

#if USE_CPPJIEBA
static constexpr std::string_view TOKENIZER_CHINESE = "chinese";
static constexpr std::string_view TOKENIZER_CHINESE_MODE_FINE_GRAINED = "fine-grained";
static constexpr std::string_view TOKENIZER_CHINESE_MODE_COARSE_GRAINED = "coarse-grained";
#endif
}


namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

class FunctionTokenize : public IFunction
{
public:
    static constexpr auto name = "tokenize";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTokenize>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 2 || arguments.size() > 3)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} argument count does not match.", getName());

        const auto tokenizer = arguments[0].column->getDataAt(0).toString();

        if (boost::iequals(tokenizer, details::TOKENIZER_NGRAM))
        {
            FunctionArgumentDescriptors args{
                {"tokenizer",
                 static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString),
                 nullptr,
                 "String or FixedString"},
                {"ngram_size", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt8), nullptr, "Number"},
                {"input",
                 static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString),
                 nullptr,
                 "String or FixedString"}};
            validateFunctionArguments(*this, arguments, args);
        }
#if USE_CPPJIEBA
        else if (boost::iequals(tokenizer, details::TOKENIZER_CHINESE) && arguments.size() == 3)
        {
            FunctionArgumentDescriptors args{
                {"tokenizer",
                 static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString),
                 nullptr,
                 "String or FixedString"},
                {"mode", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"},
                {"input",
                 static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString),
                 nullptr,
                 "String or FixedString"}};
            validateFunctionArguments(*this, arguments, args);
        }
#endif
        else
        {
            FunctionArgumentDescriptors args{
                {"tokenizer",
                 static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString),
                 nullptr,
                 "String or FixedString"},
                {"value",
                 static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString),
                 nullptr,
                 "String or FixedString"}};
            validateFunctionArguments(*this, arguments, args);
        }

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto column_offsets = ColumnArray::ColumnOffsets::create();
        auto result_column = ColumnString::create();

        auto token_extractor = [&arguments]
        {
            std::vector<std::pair<std::string_view, std::function<std::unique_ptr<ITokenExtractor>(void)>>> supported_tokenizers{
                {details::TOKENIZER_DEFAULT, [] { return std::make_unique<SplitTokenExtractor>(); }},
                {details::TOKENIZER_NOOP, [] { return std::make_unique<NoOpTokenExtractor>(); }},
                {details::TOKENIZER_NGRAM,
                 [&]
                 {
                     const auto ngram_size = arguments[1].column->getUInt(0);
                     return std::make_unique<NgramTokenExtractor>(ngram_size);
                 }}};

#if USE_CPPJIEBA
            supported_tokenizers.emplace_back(
                std::string{details::TOKENIZER_CHINESE},
                [&]
                {
                    ChineseGranularMode granular_mode = ChineseGranularMode::Fine;
                    if (arguments.size() == 3)
                    {
                        auto input_mode = arguments[1].column->getDataAt(0).toString();
                        if (boost::iequals(input_mode, details::TOKENIZER_CHINESE_MODE_FINE_GRAINED))
                            granular_mode = ChineseGranularMode::Fine;
                        else if (boost::iequals(input_mode, details::TOKENIZER_CHINESE_MODE_COARSE_GRAINED))
                            granular_mode = ChineseGranularMode::Coarse;
                        else
                            throw Exception(
                                ErrorCodes::BAD_ARGUMENTS,
                                "Chinese tokenizer supports only 'fine' or 'coarse' grained modes, but got {}",
                                input_mode);
                    }
                    return std::make_unique<ChineseTokenExtractor>(granular_mode);
                });
#endif

            const auto tokenizer = arguments[0].column->getDataAt(0).toString();
            for (const auto & supported_tokenizer : supported_tokenizers)
                if (boost::iequals(supported_tokenizer.first, tokenizer))
                    return supported_tokenizer.second();

            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Only {} tokenizers are supported, but got '{}'",
                [&supported_tokenizers]
                {
                    std::stringstream out; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
                    for (size_t i = 0; i < supported_tokenizers.size() - 1; ++i)
                        out << supported_tokenizers[i].first << ", ";
                    out << supported_tokenizers.back().first;
                    return out.str();
                }(),
                tokenizer);
        }();

        auto input_column = arguments.size() == 2 ? arguments[1].column : arguments[2].column;

        if (const auto * column_string = checkAndGetColumn<ColumnString>(input_column.get()))
            executeImpl(std::move(token_extractor), *column_string, *column_offsets, input_rows_count, *result_column);
        else if (const auto * column_fixed_string = checkAndGetColumn<ColumnFixedString>(input_column.get()))
            executeImpl(std::move(token_extractor), *column_fixed_string, *column_offsets, input_rows_count, *result_column);

        return ColumnArray::create(std::move(result_column), std::move(column_offsets));
    }

private:
    template <typename StringColumnType>
    void executeImpl(
        std::unique_ptr<ITokenExtractor> token_extractor,
        const StringColumnType & column_input,
        ColumnArray::ColumnOffsets & column_offsets_input,
        size_t rows_count_input,
        ColumnString & column_result) const
    {
        auto & offsets_data = column_offsets_input.getData();

        offsets_data.resize(rows_count_input);

        std::vector<String> tokens;
        for (size_t i = 0; i < rows_count_input; ++i)
        {
            const StringRef input = column_input.getDataAt(i);
            tokens = token_extractor->getTokens(input.data, input.size);

            for (const auto & token : tokens)
            {
                column_result.insertData(token.data(), token.size());
            }

            offsets_data[i] = tokens.size();
            tokens.clear();
        }
    }
};

REGISTER_FUNCTION(Tokenizer)
{
    factory.registerFunction<FunctionTokenize>(FunctionDocumentation{
        .description = R"(
Splits the text into tokens by the given tokenizer. Supports 'default', 'none', 'ngram' and 'chinese' tokenizers.
    )",
        .category = FunctionDocumentation::Category::StringSplitting}

    );
}
}
