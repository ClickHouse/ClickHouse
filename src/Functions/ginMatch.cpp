#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ITokenExtractor.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <boost/algorithm/string/predicate.hpp>
#include "Interpreters/GinFilter.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

struct GinMatchAnyProps
{
    static constexpr auto name = "ginMatchAny";
    static constexpr auto match = GinMatch::ANY;
};

struct GinMatchAllProps
{
    static constexpr auto name = "ginMatchAll";
    static constexpr auto match = GinMatch::ALL;
};

template <typename GinMatchProps>
class FunctionGinMatchImpl : public IFunction
{
public:
    static constexpr auto name = GinMatchProps::name;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionGinMatchImpl>();
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }
    bool isVariadic() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }

    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 3 || arguments.size() > 4)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} argument count does not match.", getName());

        FunctionArgumentDescriptors args{
            {"tokenizer",
             static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString),
             nullptr,
             "String or FixedString"}};

        const auto tokenizer = arguments[0].column->getDataAt(0).toString();
        if (boost::iequals(tokenizer, details::TOKENIZER_NGRAM))
        {
            // ngram size arg
            args.emplace_back("ngram_size", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt8), nullptr, "Number");
        }
#if USE_CPPJIEBA
        else if (boost::iequals(tokenizer, details::TOKENIZER_CHINESE) && arguments.size() == 4)
        {
            // chinese mode arg
            args.emplace_back(
                "mode",
                static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString),
                nullptr,
                "String or FixedString");
        }
#endif

        args.emplace_back(
            "input", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString");
        args.emplace_back(
            "needle", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString");

        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeNumber<UInt8>>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        std::unique_ptr<ITokenExtractor> token_extractor = [&arguments]
        {
            std::vector<std::pair<std::string_view, std::function<std::unique_ptr<ITokenExtractor>(void)>>> supported_tokenizers{
                {details::TOKENIZER_DEFAULT, [] { return std::make_unique<SplitTokenExtractor>(); }},
                {details::TOKENIZER_NOOP, [] { return std::make_unique<NoOpTokenExtractor>(); }},
                {details::TOKENIZER_NGRAM,
                 [&]
                 {
                     const auto ngram_size = arguments[1].column->getUInt(0);
                     if (ngram_size <= 0)
                         throw Exception(ErrorCodes::BAD_ARGUMENTS, "Ngram size should be at least 1, but got {}", ngram_size);
                     return std::make_unique<NgramTokenExtractor>(ngram_size);
                 }}};

#if USE_CPPJIEBA
            supported_tokenizers.emplace_back(
                std::string{details::TOKENIZER_CHINESE},
                [&]
                {
                    ChineseGranularMode granular_mode = ChineseGranularMode::Fine;
                    if (arguments.size() == 4)
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

            const auto tokenizer = arguments.size() == 1 ? String{details::TOKENIZER_DEFAULT} : arguments[0].column->getDataAt(0).toString();
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

        auto column_input = arguments[arguments.size() - 2].column;
        auto column_token = arguments[arguments.size() - 1].column;

        auto result_column = ColumnVector<UInt8>::create();

        SplitTokenExtractor needle_token_extractor{};
        std::unordered_set<std::string> needle_tokens{};
        const auto & needle_token_col{column_token->getDataAt(0)};
        for (auto needle_token : needle_token_extractor.getTokens(needle_token_col.data, needle_token_col.size))
            needle_tokens.emplace(std::move(needle_token));

        if (const auto * column_string = checkAndGetColumn<ColumnString>(column_input.get()))
            executeImpl(std::move(token_extractor), *column_string, input_rows_count, needle_tokens, result_column->getData());
        else if (const auto * column_fixed_string = checkAndGetColumn<ColumnFixedString>(column_input.get()))
            executeImpl(std::move(token_extractor), *column_fixed_string, input_rows_count, needle_tokens, result_column->getData());

        return result_column;
    }

private:
    template <typename StringColumnType>
    void executeImpl(
        std::unique_ptr<ITokenExtractor> token_extractor,
        StringColumnType & input_column,
        size_t input_rows_count,
        const std::unordered_set<std::string>& needle_tokens,
        PaddedPODArray<UInt8> & result) const
    {
        result.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto value{input_column.getDataAt(i)};

            result[i] = false;

            [[maybe_unused]] UInt64 mask{};
            for (const auto& token : token_extractor->getTokens(value.data, value.size))
            {
                if (auto it{needle_tokens.find(token)}; it != needle_tokens.end()) {
                    if constexpr (GinMatchProps::match == GinMatch::ALL)
                    {
                        auto dist{std::distance(needle_tokens.begin(), it)};
                        mask |= 1 << dist;
                    }
                    else
                    {
                        result[i] = true;
                        break;
                    }
                }
            }
            if constexpr (GinMatchProps::match == GinMatch::ALL)
            {
                result[i] = mask == ((1 << needle_tokens.size()) - 1);
            }
        }
    }
};

REGISTER_FUNCTION(GinMatchAny)
{
    factory.registerFunction<FunctionGinMatchImpl<GinMatchAnyProps>>();
}

REGISTER_FUNCTION(GinMatchAll)
{
    factory.registerFunction<FunctionGinMatchImpl<GinMatchAllProps>>();
}

}


