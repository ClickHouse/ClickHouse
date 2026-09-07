#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Dictionaries/NaiveBayesModel.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
}

namespace
{

/// Splits `text` into the n-grams a `NAIVE_BAYES` dictionary would produce, reusing the dictionary's own
/// tokenizer so the result matches what `naiveBayesClassifier` sees at query time. The intended use is building
/// the pre-aggregated `(ngram, class_id, count)` training data: `ARRAY JOIN naiveBayesNgrams(text, n, mode)`
/// then `GROUP BY`.
class FunctionNaiveBayesNgrams : public IFunction
{
public:
    static constexpr auto name = "naiveBayesNgrams";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionNaiveBayesNgrams>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3, 4}; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"text", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
            {"n", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeUInt), &isColumnConst, "const UInt"},
            {"mode", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
        };
        FunctionArgumentDescriptors optional_args{
            {"start_token", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
            {"end_token", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const UInt64 n = arguments[1].column->getUInt(0);
        if (n == 0 || n > MAX_NGRAM_SIZE)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {}: n must be in range [1, {}], got {}", name, MAX_NGRAM_SIZE, n);

        /// The mode and the padding tokens use the same encoding as the dictionary's layout parameters, so the
        /// same values can be passed to both and produce identical n-grams.
        const TokenizerMode mode = parseTokenizerMode(String(arguments[2].column->getDataAt(0)));

        String start_token;
        String end_token;
        if (arguments.size() > 3)
        {
            const String raw(arguments[3].column->getDataAt(0));
            if (!raw.empty())
                start_token = parsePaddingToken(raw, mode, "start_token");
        }
        if (arguments.size() > 4)
        {
            const String raw(arguments[4].column->getDataAt(0));
            if (!raw.empty())
                end_token = parsePaddingToken(raw, mode, "end_token");
        }

        const auto * text_column = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!text_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Function {}: the first argument must be a String column", name);

        auto result_strings = ColumnString::create();
        auto result_offsets = ColumnArray::ColumnOffsets::create();

        switch (mode)
        {
            case TokenizerMode::Byte:
                appendNgrams(
                    BytePolicy{},
                    *text_column,
                    static_cast<UInt32>(n),
                    start_token,
                    end_token,
                    *result_strings,
                    *result_offsets,
                    input_rows_count);
                break;
            case TokenizerMode::CodePoint:
                appendNgrams(
                    CodePointPolicy{},
                    *text_column,
                    static_cast<UInt32>(n),
                    start_token,
                    end_token,
                    *result_strings,
                    *result_offsets,
                    input_rows_count);
                break;
            case TokenizerMode::Token:
                appendNgrams(
                    TokenPolicy{},
                    *text_column,
                    static_cast<UInt32>(n),
                    start_token,
                    end_token,
                    *result_strings,
                    *result_offsets,
                    input_rows_count);
                break;
        }

        return ColumnArray::create(std::move(result_strings), std::move(result_offsets));
    }

private:
    template <typename Policy>
    static void appendNgrams(
        const Policy & policy,
        const ColumnString & text_column,
        UInt32 n,
        std::string_view start_token,
        std::string_view end_token,
        ColumnString & result_strings,
        ColumnArray::ColumnOffsets & result_offsets,
        size_t input_rows_count)
    {
        NaiveBayesScratch scratch;
        auto & offsets = result_offsets.getData();
        offsets.resize(input_rows_count);

        size_t total = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const std::string_view text = text_column.getDataAt(i);
            policy.enumerateNgrams(
                text,
                n,
                start_token,
                end_token,
                scratch,
                [&](std::string_view ngram)
                {
                    result_strings.insertData(ngram.data(), ngram.size());
                    ++total;
                });
            offsets[i] = total;
        }
    }
};

}

REGISTER_FUNCTION(NaiveBayesNgrams)
{
    factory.registerFunction<FunctionNaiveBayesNgrams>(FunctionDocumentation{
        .description = "Splits text into n-grams using the same tokenization as a Naive Bayes dictionary (the "
                       "[`NAIVE_BAYES`](/sql-reference/statements/create/dictionary/layouts/naive-bayes) layout): "
                       "`byte`, `codepoint`, or `token` mode, with optional boundary padding. Use it to build the "
                       "pre-aggregated `(ngram, class_id, count)` training data such a dictionary consumes, so the training "
                       "n-grams match exactly what [`naiveBayesClassifier`](/sql-reference/functions/machine-learning-functions#naivebayesclassifier) produces at query time.",
        .syntax = "naiveBayesNgrams(text, n, mode[, start_token, end_token])",
        .arguments
        = {{"text", "Text to split into n-grams.", {"String"}},
           {"n", "N-gram size, from 1 to 1024.", {"const UInt"}},
           {"mode", "Tokenization mode: 'byte', 'codepoint', or 'token'.", {"const String"}},
           {"start_token",
            "Optional. Boundary token prepended (n-1) times to the input; a number for 'byte'/'codepoint', a literal for "
            "'token'. Empty means no padding.",
            {"const String"}},
           {"end_token", "Optional. Boundary token appended (n-1) times to the input.", {"const String"}}},
        .returned_value = {"The n-grams of the input text.", {"Array(String)"}},
        .examples
        = {{"Token bigrams", "SELECT naiveBayesNgrams('the cat sat', 2, 'token');", "['the cat','cat sat']"},
           {"Token bigrams with boundary padding", "SELECT naiveBayesNgrams('cat', 2, 'token', '<s>', '</s>');", "['<s> cat','cat </s>']"},
           {"Byte bigrams with boundary padding (result shown as hex)",
            "SELECT arrayMap(x -> hex(x), naiveBayesNgrams('xy', 2, 'byte', '0x01', '0xFF'));",
            "['0178','7879','79FF']"},
           {"Code-point bigrams with boundary padding (result shown as hex)",
            "SELECT arrayMap(x -> hex(x), naiveBayesNgrams('ab', 2, 'codepoint', '0x10FFFE', '0x10FFFF'));",
            "['F48FBFBE61','6162','62F48FBFBF']"}},
        .introduced_in = {26, 7},
        .category = FunctionDocumentation::Category::StringSplitting});
}

}
