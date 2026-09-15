#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/BPETokenizer.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/BPEVocabularyFactory.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// The vocabulary a call names. It is a constant argument: a vocabulary is loaded once and shared,
/// and picking a different one per row would mean loading one per row.
BPEVocabularyPtr getVocabulary(const ColumnsWithTypeAndName & arguments, size_t argument, const String & function_name)
{
    const ColumnConst * column = checkAndGetColumnConst<ColumnString>(arguments[argument].column.get());
    if (!column)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "Argument {} of function {} must be a constant string naming a BPE vocabulary", argument + 1, function_name);

    /// Vocabularies are declared server-wide rather than per query, so they are read from the
    /// configuration of the global context and the function itself holds no context at all.
    return BPEVocabularyFactory::instance().get(column->getValue<String>(), Context::getGlobalContextInstance()->getConfigRef());
}


class FunctionTokenizeBPE : public IFunction
{
public:
    static constexpr auto name = "tokenizeBPE";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTokenizeBPE>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"text", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
            {"vocabulary", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"},
        };
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const BPEVocabularyPtr vocabulary = getVocabulary(arguments, 1, getName());

        const ColumnPtr text_column = arguments[0].column->convertToFullColumnIfConst();
        const ColumnString * text = checkAndGetColumn<ColumnString>(text_column.get());
        if (!text)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument 1 of function {} must be a string", getName());

        auto ids_column = ColumnUInt32::create();
        auto offsets_column = ColumnArray::ColumnOffsets::create();
        auto & offsets = offsets_column->getData();
        offsets.resize(input_rows_count);

        auto & data = ids_column->getData();
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            vocabulary->encode(text->getDataAt(row), data);
            offsets[row] = data.size();
        }

        return ColumnArray::create(std::move(ids_column), std::move(offsets_column));
    }
};


class FunctionDetokenizeBPE : public IFunction
{
public:
    static constexpr auto name = "detokenizeBPE";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDetokenizeBPE>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto is_array_of_unsigned = [](const IDataType & type)
        {
            const auto * array = typeid_cast<const DataTypeArray *>(&type);
            /// `Array(Nothing)` is the type of the literal empty array, which decodes to an empty string.
            return array && (isUInt(array->getNestedType()) || isNothing(array->getNestedType()));
        };

        FunctionArgumentDescriptors args{
            {"ids", static_cast<FunctionArgumentDescriptor::TypeValidator>(is_array_of_unsigned), nullptr, "Array of unsigned integers"},
            {"vocabulary", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"},
        };
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const BPEVocabularyPtr vocabulary = getVocabulary(arguments, 1, getName());

        const ColumnPtr ids_column = arguments[0].column->convertToFullColumnIfConst();
        const ColumnArray * ids_array = checkAndGetColumn<ColumnArray>(ids_column.get());
        if (!ids_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument 1 of function {} must be an array", getName());

        const IColumn & nested = ids_array->getData();
        const ColumnArray::Offsets & offsets = ids_array->getOffsets();

        auto result = ColumnString::create();
        result->reserve(input_rows_count);

        PODArray<UInt32> ids;
        String text;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const size_t begin = offsets[row - 1];
            const size_t end = offsets[row];

            ids.resize(end - begin);
            for (size_t i = begin; i < end; ++i)
            {
                const UInt64 id = nested.getUInt(i);
                /// The ids of a vocabulary fit in `UInt32`, and a wider array is accepted for
                /// convenience, so a value out of range is rejected by name rather than truncated.
                if (id > std::numeric_limits<UInt32>::max())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no token with id {} in the BPE vocabulary", id);
                ids[i - begin] = static_cast<UInt32>(id);
            }

            {
                WriteBufferFromString out(text);
                vocabulary->decode(ids, out);
                out.finalize();
            }
            result->insertData(text.data(), text.size());
        }

        return result;
    }
};

}

REGISTER_FUNCTION(TokenizeBPE)
{
    FunctionDocumentation::Description description_tokenize = R"(
Splits text into the tokens of a byte pair encoding vocabulary and returns their ids. This is the
tokenization the OpenAI models use, so the number of tokens a text costs one of them is the length of
the result.

Vocabularies are declared in the `bpe_vocabularies` section of the server configuration, each with
the file it is read from, in the `.tiktoken` format, and the pre-tokenizer it goes with (`r50k` for
the GPT-2, `r50k_base` and `p50k_base` vocabularies, `cl100k` for `cl100k_base`, `o200k` for
`o200k_base`):

```xml
<bpe_vocabularies>
    <cl100k_base>
        <path>/var/lib/clickhouse/tokenizers/cl100k_base.tiktoken</path>
        <pretokenizer>cl100k</pretokenizer>
    </cl100k_base>
</bpe_vocabularies>
```

The text is tokenized as text: a vocabulary has no special tokens, so a piece of the text that reads
like one, such as `<|endoftext|>`, is tokenized the way any other text is.

The examples below name a small vocabulary that the tests of ClickHouse declare, which holds every
single byte and a few merges, one of which is `banana`.
)";
    FunctionDocumentation::Syntax syntax_tokenize = "tokenizeBPE(text, vocabulary)";
    FunctionDocumentation::Arguments arguments_tokenize = {
        {"text", "The text to tokenize.", {"String"}},
        {"vocabulary", "The name of a vocabulary declared in the server configuration.", {"const String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_tokenize = {"Returns the ids of the tokens of the text.", {"Array(UInt32)"}};
    FunctionDocumentation::Examples examples_tokenize = {
        {"Tokenize a string", "SELECT tokenizeBPE('a banana', 'example_vocabulary')", "[97,32,260]"},
        {"Count the tokens of a string", "SELECT length(tokenizeBPE('a banana', 'example_vocabulary'))", "3"},
    };
    FunctionDocumentation::IntroducedIn introduced_in_tokenize = {26, 9};
    FunctionDocumentation::Category category_tokenize = FunctionDocumentation::Category::StringSplitting;
    FunctionDocumentation documentation_tokenize = {description_tokenize, syntax_tokenize, arguments_tokenize, {}, returned_value_tokenize, examples_tokenize, introduced_in_tokenize, category_tokenize};

    factory.registerFunction<FunctionTokenizeBPE>(documentation_tokenize);

    FunctionDocumentation::Description description_detokenize = R"(
Turns the token ids of a byte pair encoding vocabulary back into text. It is the inverse of
[`tokenizeBPE`](#tokenizeBPE) with the same vocabulary.
)";
    FunctionDocumentation::Syntax syntax_detokenize = "detokenizeBPE(ids, vocabulary)";
    FunctionDocumentation::Arguments arguments_detokenize = {
        {"ids", "The token ids.", {"Array(UInt32)"}},
        {"vocabulary", "The name of a vocabulary declared in the server configuration.", {"const String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_detokenize = {"Returns the text of the tokens.", {"String"}};
    FunctionDocumentation::Examples examples_detokenize = {
        {"Round trip", "SELECT detokenizeBPE(tokenizeBPE('a banana', 'example_vocabulary'), 'example_vocabulary')", "a banana"},
    };
    FunctionDocumentation::IntroducedIn introduced_in_detokenize = {26, 9};
    FunctionDocumentation::Category category_detokenize = FunctionDocumentation::Category::StringSplitting;
    FunctionDocumentation documentation_detokenize = {description_detokenize, syntax_detokenize, arguments_detokenize, {}, returned_value_detokenize, examples_detokenize, introduced_in_detokenize, category_detokenize};

    factory.registerFunction<FunctionDetokenizeBPE>(documentation_detokenize);
}

}
