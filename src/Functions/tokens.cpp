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

namespace
{

constexpr size_t arg_value = 0;
constexpr size_t arg_tokenizer = 1;
constexpr size_t arg_ngrams = 2;
constexpr size_t arg_separators = 2;

std::unique_ptr<ITokenExtractor> createTokenizer(const ColumnsWithTypeAndName & arguments, std::string_view name)
{
    const auto tokenizer_arg = arguments.size() < 2 ? DefaultTokenExtractor::getExternalName()
                                                        : arguments[arg_tokenizer].column->getDataAt(0).toView();

    if (tokenizer_arg == DefaultTokenExtractor::getExternalName())
    {
        return std::make_unique<DefaultTokenExtractor>();
    }
    if (tokenizer_arg == SplitTokenExtractor::getExternalName())
    {
        std::vector<String> separators;
        if (arguments.size() < 3)
        {
            separators = {" "};
        }
        else
        {
            const ColumnArray * col_separators = checkAndGetColumn<ColumnArray>(arguments[arg_separators].column.get());
            const ColumnArray * col_separators_const = checkAndGetColumnConstData<ColumnArray>(arguments[arg_separators].column.get());

            if (!col_separators_const && !col_separators)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "3rd argument of function {} should be Array(String), got: {}", name, arguments[arg_separators].column->getFamilyName());

            if (col_separators_const)
                col_separators = col_separators_const;

            Field separator_field = (*col_separators)[0];
            const Array & separator_array = separator_field.safeGet<Array>();

            for (const auto & separator : separator_array)
                separators.emplace_back(separator.safeGet<String>());
        }

        return std::make_unique<SplitTokenExtractor>(separators);
    }
    if (tokenizer_arg == NoOpTokenExtractor::getExternalName())
    {
        return std::make_unique<NoOpTokenExtractor>();
    }
    if (tokenizer_arg == NgramTokenExtractor::getExternalName())
    {
        auto ngrams = (arguments.size() < 3) ? 3 : arguments[arg_ngrams].column->getUInt(0);
        if (ngrams < 2 || ngrams > 8)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Ngrams argument of function {} should be between 2 and 8, got: {}", name, ngrams);
        return std::make_unique<NgramTokenExtractor>(ngrams);
    }
    if (tokenizer_arg == SparseGramTokenExtractor::getExternalName())
    {
        auto min_length = arguments.size() < 3 ? 3
            : arguments[2].column->getUInt(0);
        auto max_length = arguments.size() < 4 ? 100
            : arguments[3].column->getUInt(0);
        auto min_cutoff_length = arguments.size() < 5 ? std::nullopt
            : std::optional(arguments[4].column->getUInt(0));

        return std::make_unique<SparseGramTokenExtractor>(min_length, max_length, min_cutoff_length);
    }

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Function '{}' supports only tokenizers 'splitByNonAlpha', 'ngrams', 'splitByString', 'array', and 'sparseGrams'", name);
}

class ExecutableFunctionTokens : public IExecutableFunction
{
public:
    static constexpr auto name = "tokens";

    explicit ExecutableFunctionTokens(std::shared_ptr<const ITokenExtractor> token_extractor_)
        : token_extractor(std::move(token_extractor_))
    {
    }

    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_input = arguments[arg_value].column;
        auto col_result = ColumnString::create();
        auto col_offsets = ColumnArray::ColumnOffsets::create();

        if (input_rows_count == 0)
            return ColumnArray::create(std::move(col_result), std::move(col_offsets));

        if (token_extractor->getType() == ITokenExtractor::Type::SparseGram)
        {
            /// The sparse gram token extractor stores an internal state which modified during the execution.
            /// This leads to an error while executing this function multi-threaded because that state is not protected.
            /// To avoid this case, a clone of the sparse gram token extractor will be used.
            auto sparse_gram_extractor = token_extractor->clone();
            executeWithTokenizer(*sparse_gram_extractor, std::move(col_input), *col_offsets, input_rows_count, *col_result);
        }
        else
        {
            executeWithTokenizer(*token_extractor, std::move(col_input), *col_offsets, input_rows_count, *col_result);
        }

        return ColumnArray::create(std::move(col_result), std::move(col_offsets));
    }

private:
    void executeWithTokenizer(
        const ITokenExtractor & extractor,
        ColumnPtr col_input,
        ColumnArray::ColumnOffsets & col_offsets,
        size_t input_rows_count,
        ColumnString & col_result) const
    {
        if (const auto * column_string = checkAndGetColumn<ColumnString>(col_input.get()))
            executeImpl(extractor, *column_string, col_offsets, input_rows_count, col_result);
        else if (const auto * column_fixed_string = checkAndGetColumn<ColumnFixedString>(col_input.get()))
            executeImpl(extractor, *column_fixed_string, col_offsets, input_rows_count, col_result);
    }

    template <typename StringColumnType>
    void executeImpl(
        const ITokenExtractor & extractor,
        const StringColumnType & column_input,
        ColumnArray::ColumnOffsets & column_offsets_input,
        size_t input_rows_count,
        ColumnString & column_result) const
    {
        auto & offsets_data = column_offsets_input.getData();
        offsets_data.resize(input_rows_count);
        size_t tokens_count = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view input = column_input.getDataAt(i).toView();

            forEachTokenPadded(extractor, input.data(), input.size(), [&](const char * token_start, size_t token_len)
            {
                column_result.insertData(token_start, token_len);
                ++tokens_count;
                return false;
            });

            offsets_data[i] = tokens_count;
        }
    }

    std::shared_ptr<const ITokenExtractor> token_extractor;
};

class FunctionBaseTokens : public IFunctionBase
{
public:
    static constexpr auto name = "tokens";

    FunctionBaseTokens(std::shared_ptr<const ITokenExtractor> token_extractor_, DataTypes argument_types_, DataTypePtr result_type_)
        : token_extractor(std::move(token_extractor_))
        , argument_types(std::move(argument_types_))
        , result_type(std::move(result_type_))
    {
    }

    String getName() const override { return name; }
    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return result_type; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionTokens>(token_extractor);
    }

private:
    std::shared_ptr<const ITokenExtractor> token_extractor;
    DataTypes argument_types;
    DataTypePtr result_type;
};

class FunctionTokensOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "tokens";

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    static FunctionOverloadResolverPtr create(ContextPtr)
    {
        return std::make_unique<FunctionTokensOverloadResolver>();
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"value", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}};

        FunctionArgumentDescriptors optional_args;

        if (arguments.size() > 1)
        {
            optional_args.emplace_back("tokenizer", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "String");
            validateFunctionArguments(name, {arguments[arg_value], arguments[arg_tokenizer]}, mandatory_args, optional_args);

            if (arguments.size() == 3)
            {
                const auto tokenizer = arguments[arg_tokenizer].column->getDataAt(0).toString();

                if (tokenizer == NgramTokenExtractor::getExternalName())
                    optional_args.emplace_back("ngrams", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt8), isColumnConst, "const UInt8");
                else if (tokenizer == SplitTokenExtractor::getExternalName())
                    optional_args.emplace_back("separators", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), isColumnConst, "const Array");
            }

            if (arguments.size() == 4 || arguments.size() == 5)
            {
                const auto tokenizer = arguments[arg_tokenizer].column->getDataAt(0).toString();

                if (tokenizer == SparseGramTokenExtractor::getExternalName())
                {
                    optional_args.emplace_back("min_length", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt8), isColumnConst, "UInt8");
                    optional_args.emplace_back("max_length", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt8), isColumnConst, "UInt8");
                    if (arguments.size() == 5)
                        optional_args.emplace_back("min_cutoff_length", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt8), isColumnConst, "UInt8");
                }
            }
        }

        validateFunctionArguments(name, arguments, mandatory_args, optional_args);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        auto token_extractor = createTokenizer(arguments, getName());
        DataTypes argument_types{std::from_range_t{}, arguments | std::views::transform([](auto & elem) { return elem.type; })};
        return std::make_shared<FunctionBaseTokens>(std::move(token_extractor), std::move(argument_types), return_type);
    }
};

}

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

    factory.registerFunction<FunctionTokensOverloadResolver>(documentation);
}
}
