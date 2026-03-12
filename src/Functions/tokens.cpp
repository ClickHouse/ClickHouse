#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ITokenizer.h>
#include <Interpreters/TokenizerFactory.h>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

constexpr size_t arg_value = 0;
constexpr size_t arg_tokenizer = 1;

std::unique_ptr<ITokenizer> createTokenizer(const ColumnsWithTypeAndName & arguments, std::string_view function_name)
{
    const auto tokenizer_str = arguments.size() < 2 || !arguments[arg_tokenizer].column
        ? SplitByNonAlphaTokenizer::getExternalName()
        : arguments[arg_tokenizer].column->getDataAt(0);

    if (arguments.size() <= 2)
        return TokenizerFactory::instance().get(tokenizer_str);

    FieldVector params;
    for (size_t i = 2; i < arguments.size(); ++i)
    {
        const auto & col = arguments[i].column;
        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Invalid argument of function {}", function_name);

        WhichDataType which_type(arguments[i].type);
        if (which_type.isUInt())
        {
            params.push_back(col->getUInt(0));
        }
        else
        {
            const ColumnArray * col_separators = checkAndGetColumn<ColumnArray>(col.get());
            const ColumnArray * col_separators_const = checkAndGetColumnConstData<ColumnArray>(col.get());

            if (!col_separators && !col_separators_const)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Argument {} of function {} should be Array(String), got: {}",
                    i + 1 /*1-based*/,
                    function_name,
                    col->getFamilyName());

            if (col_separators_const)
                col_separators = col_separators_const;

            params.push_back((*col_separators)[0]);
        }
    }

    return TokenizerFactory::instance().get(tokenizer_str, params);
}

class ExecutableFunctionTokens : public IExecutableFunction
{
public:
    static constexpr auto name = "tokens";

    explicit ExecutableFunctionTokens(std::shared_ptr<const ITokenizer> tokenizer_)
        : tokenizer(std::move(tokenizer_))
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

        if (tokenizer->getType() == ITokenizer::Type::SparseGrams)
        {
            /// The sparse gram tokenizer stores an internal state which modified during the execution.
            /// This leads to an error while executing this function multi-threaded because that state is not protected.
            /// To avoid this case, a clone of the sparse gram tokenizer will be used.
            auto sparse_grams_tokenizer = tokenizer->clone();
            executeWithTokenizer(*sparse_grams_tokenizer, std::move(col_input), *col_offsets, input_rows_count, *col_result);
        }
        else
        {
            executeWithTokenizer(*tokenizer, std::move(col_input), *col_offsets, input_rows_count, *col_result);
        }

        return ColumnArray::create(std::move(col_result), std::move(col_offsets));
    }

private:
    void executeWithTokenizer(
        const ITokenizer & tokenizer_,
        ColumnPtr col_input,
        ColumnArray::ColumnOffsets & col_offsets,
        size_t input_rows_count,
        ColumnString & col_result) const
    {
        if (const auto * column_string = checkAndGetColumn<ColumnString>(col_input.get()))
            executeImpl(tokenizer_, *column_string, col_offsets, input_rows_count, col_result);
        else if (const auto * column_fixed_string = checkAndGetColumn<ColumnFixedString>(col_input.get()))
            executeImpl(tokenizer_, *column_fixed_string, col_offsets, input_rows_count, col_result);
    }

    template <typename StringColumnType>
    void executeImpl(
        const ITokenizer & tokenizer_,
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
            std::string_view input = column_input.getDataAt(i);

            forEachTokenPadded(tokenizer_, input.data(), input.size(), [&](const char * token_start, size_t token_len)
            {
                column_result.insertData(token_start, token_len);
                ++tokens_count;
                return false;
            });

            offsets_data[i] = tokens_count;
        }
    }

    std::shared_ptr<const ITokenizer> tokenizer;
};

class FunctionBaseTokens : public IFunctionBase
{
public:
    static constexpr auto name = "tokens";

    FunctionBaseTokens(std::shared_ptr<const ITokenizer> tokenizer_, DataTypes argument_types_, DataTypePtr result_type_)
        : tokenizer(std::move(tokenizer_))
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
        return std::make_unique<ExecutableFunctionTokens>(tokenizer);
    }

private:
    std::shared_ptr<const ITokenizer> tokenizer;
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
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3, 4}; }

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
            optional_args.emplace_back("tokenizer", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String");
            validateFunctionArguments(name, {arguments[arg_value], arguments[arg_tokenizer]}, mandatory_args, optional_args);

            if (arguments.size() == 3)
            {
                const std::string tokenizer{arguments[arg_tokenizer].column->getDataAt(0)};

                if (tokenizer == NgramsTokenizer::getExternalName())
                    optional_args.emplace_back("ngrams", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt8), isColumnConst, "const UInt8");
                else if (tokenizer == SplitByStringTokenizer::getExternalName())
                    optional_args.emplace_back("separators", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), isColumnConst, "const Array");
            }
            else if (arguments.size() == 4 || arguments.size() == 5)
            {
                const auto tokenizer = arguments[arg_tokenizer].column->getDataAt(0);

                if (tokenizer == SparseGramsTokenizer::getExternalName())
                {
                    optional_args.emplace_back("min_length", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt8), isColumnConst, "const UInt8");
                    optional_args.emplace_back("max_length", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt8), isColumnConst, "const UInt8");
                    if (arguments.size() == 5)
                        optional_args.emplace_back("min_cutoff_length", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt8), isColumnConst, "const UInt8");
                }
            }
        }

        validateFunctionArguments(name, arguments, mandatory_args, optional_args);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        auto tokenizer = createTokenizer(arguments, getName());
        DataTypes argument_types{std::from_range_t{}, arguments | std::views::transform([](auto & elem) { return elem.type; })};
        return std::make_shared<FunctionBaseTokens>(std::move(tokenizer), std::move(argument_types), return_type);
    }
};

}

REGISTER_FUNCTION(Tokens)
{
    FunctionDocumentation::Description description = R"(
Splits a string into tokens using the given tokenizer.

Available tokenizers:
- `splitByNonAlpha` splits strings along non-alphanumeric ASCII characters (also see function [splitByNonAlpha](/sql-reference/functions/splitting-merging-functions.md/#splitByNonAlpha)).
- `splitByString(S)` splits strings along certain user-defined separator strings `S` (also see function [splitByString](/sql-reference/functions/splitting-merging-functions.md/#splitByString)). The separators can be specified using an optional parameter, for example, `tokens(value, 'splitByString', [', ', '; ', '\n', '\\'])`. Note that each string can consist of multiple characters (`', '` in the example). The default separator list, if not specified explicitly, is a single whitespace `[' ']`.
- `ngrams(N)` splits strings into equally large `N`-grams (also see function [ngrams](/sql-reference/functions/splitting-merging-functions.md/#ngrams)). The ngram length can be specified using an optional integer parameter between 1 and 8, for example, `tokens(value, 'ngrams', 3)`. The default ngram size, if not specified explicitly, is 3.
- `sparseGrams(min_length, max_length, min_cutoff_length)` splits strings into variable-length n-grams of at least `min_length` and at most `max_length` (inclusive) characters (also see function [sparseGrams](/sql-reference/functions/string-functions#sparseGrams)). Unless specified explicitly, `min_length` and `max_length` default to 3 and 100. If parameter `min_cutoff_length` is provided, only n-grams with length greater or equal than `min_cutoff_length` are returned. Compared to `ngrams(N)`, the `sparseGrams` tokenizer produces variable-length N-grams, allowing for a more flexible representation of the original text. For example, `tokens(value, 'sparseGrams', 3, 5, 4)` internally generates 3-, 4-, 5-grams from the input string but only the 4- and 5-grams are returned.
- `array` performs no tokenization, i.e. every row value is a token (also see function [array](/sql-reference/functions/array-functions.md/#array)).

In case of the `splitByString` tokenizer, if the tokens do not form a [prefix code](https://en.wikipedia.org/wiki/Prefix_code), you likely want that the matching prefers longer separators first.
To do so, pass the separators in order of descending length.
For example, with separators = `['%21', '%']` string `%21abc` would be tokenized as `['abc']`, whereas separators = `['%', '%21']` would tokenize to `['21ac']` (which is likely not what you wanted).
)";
    FunctionDocumentation::Syntax syntax = R"(
tokens(value) -- 'splitByNonAlpha' tokenizer
tokens(value, 'splitByNonAlpha')
tokens(value, 'splitByString'[, separators])
tokens(value, 'ngrams'[, n])
tokens(value, 'sparseGrams'[, min_length, max_length[, min_cutoff_length]])
tokens(value, 'array')
)";
    FunctionDocumentation::Arguments arguments = {
        {"value", "The input string.", {"String", "FixedString"}},
        {"tokenizer", "The tokenizer to use. Valid arguments are `splitByNonAlpha`, `ngrams`, `splitByString`, `array`, and `sparseGrams`. Optional, if not set explicitly, defaults to `splitByNonAlpha`.", {"const String"}},
        {"n", "Only relevant if argument `tokenizer` is `ngrams`: An optional parameter which defines the length of the ngrams. If not set explicitly, defaults to `3`.", {"const UInt8"}},
        {"separators", "Only relevant if argument `tokenizer` is `split`: An optional parameter which defines the separator strings. If not set explicitly, defaults to `[' ']`.", {"const Array(String)"}},
        {"min_length", "Only relevant if argument `tokenizer` is `sparseGrams`: An optional parameter which defines the minimum gram length, defaults to 3.", {"const UInt8"}},
        {"max_length", "Only relevant if argument `tokenizer` is `sparseGrams`: An optional parameter which defines the maximum gram length, defaults to 100.", {"const UInt8"}},
        {"min_cutoff_length", "Only relevant if argument `tokenizer` is `sparseGrams`: An optional parameter which defines the minimum cutoff length.", {"const UInt8"}}
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
        "SELECT tokens('abc def', 'ngrams', 3) AS tokens;",
        R"(
['abc','bc ','c d',' de','def']
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSplitting;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTokensOverloadResolver>(documentation);
}
}
