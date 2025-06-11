#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ITokenExtractor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class FunctionTokens : public IFunction
{
    static constexpr size_t arg_value = 0;
    static constexpr size_t arg_tokenizer = 1;
    static constexpr size_t arg_ngrams = 2;

public:
    static constexpr auto name = "tokens";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTokens>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"value", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}};

        FunctionArgumentDescriptors optional_args;

        if (arguments.size() > 1)
        {
            optional_args.emplace_back("tokenizer", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "String");

            validateFunctionArguments(*this, {arguments[arg_value], arguments[arg_tokenizer]}, mandatory_args, optional_args);

            if (arguments.size() == 3)
            {
                const auto tokenizer = arguments[arg_tokenizer].column->getDataAt(0).toString();

                if (tokenizer == NgramTokenExtractor::getExternalName())
                    optional_args.emplace_back("ngrams", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt8), isColumnConst, "UInt8");
            }
        }

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_input = arguments[arg_value].column;
        auto col_result = ColumnString::create();
        auto col_offsets = ColumnArray::ColumnOffsets::create();

        if (input_rows_count == 0)
            return ColumnArray::create(std::move(col_result), std::move(col_offsets));

        std::unique_ptr<ITokenExtractor> token_extractor;

        const auto tokenizer_arg = arguments.size() < 2 ? SplitTokenExtractor::getExternalName()
                                                        : arguments[arg_tokenizer].column->getDataAt(0).toView();

        if (tokenizer_arg == SplitTokenExtractor::getExternalName())
            token_extractor = std::make_unique<SplitTokenExtractor>();
        else if (tokenizer_arg == NoOpTokenExtractor::getExternalName())
            token_extractor = std::make_unique<NoOpTokenExtractor>();
        else if (tokenizer_arg == NgramTokenExtractor::getExternalName())
        {
            auto ngrams = arguments.size() < 3 ? 3
                                               : arguments[arg_ngrams].column->getUInt(0);
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

        if (const auto * column_string = checkAndGetColumn<ColumnString>(col_input.get()))
            executeImpl(std::move(token_extractor), *column_string, *col_offsets, input_rows_count, *col_result);
        else if (const auto * column_fixed_string = checkAndGetColumn<ColumnFixedString>(col_input.get()))
            executeImpl(std::move(token_extractor), *column_fixed_string, *col_offsets, input_rows_count, *col_result);

        return ColumnArray::create(std::move(col_result), std::move(col_offsets));
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
        size_t tokens_count = 0;
        for (size_t i = 0; i < rows_count_input; ++i)
        {
            std::string_view input = column_input.getDataAt(i).toView();
            tokens = token_extractor->getTokens(input.data(), input.size());
            tokens_count += tokens.size();

            for (const auto & token : tokens)
                column_result.insertData(token.data(), token.size());
            tokens.clear();

            offsets_data[i] = tokens_count;
        }
    }
};

REGISTER_FUNCTION(Tokens)
{
    factory.registerFunction<FunctionTokens>(FunctionDocumentation{
        .description = "Splits the text into tokens by a given tokenizer.",
        .category = FunctionDocumentation::Category::StringSplitting});
}
}
