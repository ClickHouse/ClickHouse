#pragma once

#include <Formats/FormatSettings.h>
#include <Interpreters/Context_fwd.h>
#include <IO/PeekableReadBuffer.h>
#include <Parsers/ExpressionListParsers.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Processors/Formats/Impl/ConstantExpressionTemplate.h>

namespace DB
{

class ReadBuffer;

/** Stream to read data in VALUES format (as in INSERT query).
  */
class ValuesBlockInputFormat final : public IInputFormat
{
public:
    /** Data is parsed using fast, streaming parser.
      * If interpret_expressions is true, it will, in addition, try to use SQL parser and interpreter
      *  in case when streaming parser could not parse field (this is very slow).
      * If deduce_templates_of_expressions is true, try to deduce template of expression in some row and use it
      * to parse and interpret expressions in other rows (in most cases it's faster
      * than interpreting expressions in each row separately, but it's still slower than streaming parsing)
      */
    ValuesBlockInputFormat(ReadBuffer & in_, SharedHeader header_, const RowInputFormatParams & params_,
                           const FormatSettings & format_settings_);

    String getName() const override { return "ValuesBlockInputFormat"; }

    void resetParser() override;
    void setReadBuffer(ReadBuffer & in_) override;
    void resetReadBuffer() override;

    /// TODO: remove context somehow.
    void setContext(const ContextPtr & context_);
    void setQueryParameters(const NameToNameMap & parameters);

    const BlockMissingValues * getMissingValues() const override { return &block_missing_values; }

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

    static bool skipToNextRow(ReadBuffer * buf, size_t min_chunk_bytes, int balance);

private:
    ValuesBlockInputFormat(std::unique_ptr<PeekableReadBuffer> buf_, SharedHeader header_, const RowInputFormatParams & params_,
                           const FormatSettings & format_settings_);

    enum class ParserType : uint8_t
    {
        Streaming,
        BatchTemplate,
        SingleExpressionEvaluation
    };

    using ConstantExpressionTemplates = std::vector<std::optional<ConstantExpressionTemplate>>;

    Chunk read() override;

    void readRow(MutableColumns & columns, size_t row_num);
    void readUntilTheEndOfRowAndReTokenize(size_t current_column_idx);

    bool tryParseExpressionUsingTemplate(MutableColumnPtr & column, size_t column_idx);
    ALWAYS_INLINE inline bool tryReadValue(IColumn & column, size_t column_idx);

    /// Streaming parser helpers for tryReadValue. On success they return whether the value was read
    /// (false means a default value was inserted). On failure they return std::nullopt with the column
    /// unchanged and the buffer rolled back to the checkpoint at the beginning of the value.
    std::optional<bool> tryReadValueStreaming(IColumn & column, size_t column_idx);
    std::optional<bool> tryReadValueStreamingWithExceptions(IColumn & column, size_t column_idx);

    bool parseExpression(IColumn & column, size_t column_idx);

    ALWAYS_INLINE inline void assertDelimiterAfterValue(size_t column_idx);
    ALWAYS_INLINE inline bool checkDelimiterAfterValue(size_t column_idx);

    bool shouldDeduceNewTemplate(size_t column_idx);

    void readPrefix();
    void readSuffix();

    size_t countRows(size_t max_block_size);

    std::unique_ptr<PeekableReadBuffer> buf;
    std::optional<IParser::Pos> token_iterator{};
    std::optional<Tokens> tokens{};

    const RowInputFormatParams params;

    ContextPtr context;   /// pimpl
    const FormatSettings format_settings;

    const size_t num_columns;
    size_t total_rows = 0;

    std::vector<ParserType> parser_type_for_column;
    std::vector<size_t> attempts_to_deduce_template;
    std::vector<size_t> attempts_to_deduce_template_cached;
    std::vector<size_t> rows_parsed_using_template;

    ParserExpression parser;
    ConstantExpressionTemplates templates;
    ConstantExpressionTemplate::Cache templates_cache;

    const DataTypes types;
    Serializations serializations;

    /// Whether the column type is a Decimal or contains a nested Decimal. For such columns
    /// the streaming parser uses the code path with exceptions to distinguish a decimal
    /// overflow (which must fail the query) from a genuine parse failure.
    std::vector<UInt8> column_nests_decimal;

    BlockMissingValues block_missing_values;
    size_t approx_bytes_read_for_chunk = 0;
};

class ValuesSchemaReader final : public IRowSchemaReader
{
public:
    ValuesSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings);

private:
    std::optional<DataTypes> readRowAndGetDataTypes() override;
    void transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type) override;

    PeekableReadBuffer buf;
    ParserExpression parser;
    bool first_row = true;
    bool end_of_data = false;
};

}
