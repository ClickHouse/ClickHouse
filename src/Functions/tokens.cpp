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
    extern const int ILLEGAL_COLUMN;
}

class FunctionTokens : public IFunction
{
    static constexpr size_t arg_value = 0;
    static constexpr size_t arg_tokenizer = 1;
    static constexpr size_t arg_ngrams = 2;
    static constexpr size_t arg_separators = 2;

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
                    optional_args.emplace_back("ngrams", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt8), isColumnConst, "const UInt8");
                else if (tokenizer == SplitTokenExtractor::getExternalName())
                    optional_args.emplace_back("separators", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), isColumnConst, "const Array");
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

        const auto tokenizer_arg = arguments.size() < 2 ? DefaultTokenExtractor::getExternalName()
                                                        : arguments[arg_tokenizer].column->getDataAt(0).toView();

        if (tokenizer_arg == DefaultTokenExtractor::getExternalName())
        {
            token_extractor = std::make_unique<DefaultTokenExtractor>();
        }
        else if (tokenizer_arg == SplitTokenExtractor::getExternalName())
        {
            std::vector<String> separators;
            if (arguments.size() < 3)
                separators = {" "};
            else
            {
                const ColumnArray * col_separators_non_const = checkAndGetColumn<ColumnArray>(arguments[arg_separators].column.get());
                const ColumnConst * col_separators_const = checkAndGetColumnConst<ColumnArray>(arguments[arg_separators].column.get());

                if (col_separators_const)
                {
                    const Array & separators_array = col_separators_const->getValue<Array>();
                    for (const auto & separator : separators_array)
                        separators.emplace_back(separator.safeGet<String>());
                }
                else if (col_separators_non_const)
                {
                    Field separator_field;
                    col_separators_non_const->get(0, separator_field);
                    Array & separator_array = separator_field.safeGet<Array>();
                    for (const auto & separator : separator_array)
                        separators.emplace_back(separator.safeGet<String>());
                }
                else
                {
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "3rd argument of function {} should be Array(String), got: {}", name, arguments[arg_separators].column->getFamilyName());
                }
            }
            token_extractor = std::make_unique<SplitTokenExtractor>(separators);
        }
        else if (tokenizer_arg == NoOpTokenExtractor::getExternalName())
        {
            token_extractor = std::make_unique<NoOpTokenExtractor>();
        }
        else if (tokenizer_arg == NgramTokenExtractor::getExternalName())
        {
            auto ngrams = (arguments.size() < 3) ? 3 : arguments[arg_ngrams].column->getUInt(0);
            if (ngrams < 2 || ngrams > 8)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Ngrams argument of function {} should be between 2 and 8, got: {}", name, ngrams);
            token_extractor = std::make_unique<NgramTokenExtractor>(ngrams);
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function '{}' supports only tokenizers 'default', 'ngram', 'split', and 'no_op'", name);
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
        size_t input_rows_count,
        ColumnString & column_result) const
    {
        auto & offsets_data = column_offsets_input.getData();
        offsets_data.resize(input_rows_count);

        std::vector<std::string_view> tokens;
        size_t tokens_count = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view input = column_input.getDataAt(i).toView();
            tokens = token_extractor->getTokensView(input.data(), input.size());
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
    FunctionDocumentation::Description description = R"(
Splits a string into tokens using the given tokenizer.
The default tokenizer uses non-alphanumeric ASCII characters as separators.

In case of the `split` tokenizer, if the tokens do not form a [prefix code](https://en.wikipedia.org/wiki/Prefix_code), you likely want that the matching prefers longer separators first.
To do so, pass the separators in order of descending length.
For example, with separators = `['%21', '%']` string `%21abc` would be tokenized as `['abc']`, whereas separators = `['%', '%21']` would tokenize to `['21ac']` (which is likely not what you wanted).
)";
    FunctionDocumentation::Syntax syntax = "tokens(value[, tokenizer[, ngrams[, separators]]])";
    FunctionDocumentation::Arguments arguments = {
        {"value", "The input string.", {"String", "FixedString"}},
        {"tokenizer", "The tokenizer to use. Valid arguments are `default`, `ngram`, `split`, and `no_op`. Optional, if not set explicitly, defaults to `default`.", {"const String"}},
        {"ngrams", "Only relevant if argument `tokenizer` is `ngram`: An optional parameter which defines the length of the ngrams. If not set explicitly, defaults to `3`.", {"const UInt8"}},
        {"separators", "Only relevant if argument `tokenizer` is `split`: An optional parameter which defines the separator strings. If not set explicitly, defaults to `[' ']`.", {"const Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the resulting array of tokens from input string.", {"Array"}};
    FunctionDocumentation::Examples examples = {
    {
        "Default tokenizer",
        R"(SELECT tokens('test1,;\\\\ test2,;\\\\ test3,;\\\\   test4') AS tokens;)",
        R"(
['test1','test2','test3','test4']
        )"
    },
    {
        "Ngram tokenizer",
        "SELECT tokens('abc def', 'ngram', 3) AS tokens;",
        R"(
['abc','bc ','c d',' de','def']
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSplitting;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTokens>(documentation);
}
}
