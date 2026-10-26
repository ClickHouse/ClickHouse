#pragma once

#include <Processors/Formats/RowInputFormatWithDiagnosticInfo.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <Formats/ParsedTemplateFormatString.h>
#include <Formats/SchemaInferenceUtils.h>
#include <IO/ReadHelpers.h>
#include <IO/PeekableReadBuffer.h>


namespace DB
{

class TemplateFormatReader;

class TemplateRowInputFormat final : public RowInputFormatWithDiagnosticInfo
{
    using EscapingRule = FormatSettings::EscapingRule;
public:
    TemplateRowInputFormat(SharedHeader header_, ReadBuffer & in_, const Params & params_,
                           FormatSettings settings_, bool ignore_spaces_,
                           ParsedTemplateFormatString format_, ParsedTemplateFormatString row_format_,
                           std::string row_between_delimiter);

    String getName() const override { return "TemplateRowInputFormat"; }

    void resetParser() override;
    void setReadBuffer(ReadBuffer & in_) override;
    void resetReadBuffer() override;

private:
    TemplateRowInputFormat(SharedHeader header_, std::unique_ptr<PeekableReadBuffer> buf_, const Params & params_,
                           FormatSettings settings_, bool ignore_spaces_,
                           ParsedTemplateFormatString format_, ParsedTemplateFormatString row_format_,
                           std::string row_between_delimiter);

    bool readRow(MutableColumns & columns, RowReadExtension & extra) override;

    void readPrefix() override;

    bool allowSyncAfterError() const override;
    void syncAfterError() override;

    bool deserializeField(const DataTypePtr & type,
        const SerializationPtr & serialization, IColumn & column, size_t file_column);

    bool parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out) override;
    void tryDeserializeField(const DataTypePtr & type, IColumn & column, size_t file_column) override;

    bool isGarbageAfterField(size_t after_col_idx, ReadBuffer::Position pos) override;

    std::unique_ptr<PeekableReadBuffer> buf;
    const DataTypes data_types;

    FormatSettings settings;
    const bool ignore_spaces;
    const ParsedTemplateFormatString format;
    const ParsedTemplateFormatString row_format;

    bool end_of_stream = false;
    std::vector<size_t> always_default_columns;
    const char default_csv_delimiter;

    const std::string row_between_delimiter;

    std::unique_ptr<TemplateFormatReader> format_reader;
    /// Error recovery consumes the row-between delimiter before returning to normal row parsing.
    bool row_between_delimiter_already_skipped = false;
};

class TemplateFormatReader
{
    using EscapingRule = FormatSettings::EscapingRule;

public:
    TemplateFormatReader(
        PeekableReadBuffer & buf_,
        bool ignore_spaces_,
        const ParsedTemplateFormatString & format_,
        const ParsedTemplateFormatString & row_format_,
        std::string row_between_delimiter,
        const FormatSettings & format_settings_);

    void readPrefix();
    void skipField(EscapingRule escaping_rule);
    void skipSpaces() { if (ignore_spaces) skipWhitespaceIfAny(*buf); }

    template <typename ReturnType = void>
    ReturnType tryReadPrefixOrSuffix(size_t & input_part_beg, size_t input_part_end);
    bool checkForSuffix();

    void setReadBuffer(PeekableReadBuffer & buf_) { buf = &buf_; }

    void skipDelimiter(size_t index);
    void skipRowEndDelimiter();
    void skipRowBetweenDelimiter();

    size_t getFormatDataIdx() const { return format_data_idx; }

private:
    PeekableReadBuffer * buf;
    bool ignore_spaces;
    const ParsedTemplateFormatString & format;
    const ParsedTemplateFormatString & row_format;
    const std::string row_between_delimiter;
    const FormatSettings & format_settings;
    size_t format_data_idx;
    size_t row_num{};
};

class TemplateSchemaReader final : public IRowSchemaReader
{
public:
    TemplateSchemaReader(ReadBuffer & in_,
                         bool ignore_spaces_,
                         const ParsedTemplateFormatString & format_,
                         const ParsedTemplateFormatString & row_format_,
                         std::string row_between_delimiter,
                         const FormatSettings & format_settings_);

    std::optional<DataTypes> readRowAndGetDataTypes() override;

private:
    bool readsNumericValueIntoBoolColumn() const override { return false; }

    /// `TemplateRowInputFormat::deserializeField` picks an escaping rule per placeholder
    /// (`row_format.escaping_rules[file_column]`), so a single format-level value-form contract
    /// exists only when every placeholder uses the same rule; the capabilities are then derived
    /// from that rule exactly as in `CustomSeparated`. A row format mixing different rules cannot be
    /// described by any format-level contract in either direction (e.g. with `${b:Quoted}|${n:Escaped}`
    /// the quoted `'true'` is a valid `Bool`, while the raw-text defaults would reject the inferred
    /// `String`), so `schemaDescribesParsedData` below is false for it and no conclusion is drawn.
    bool readsTypedJSONValueTokens() const override { return homogeneous_escaping_rule == FormatSettings::EscapingRule::JSON; }
    bool readsQuotedTextValues() const override { return homogeneous_escaping_rule == FormatSettings::EscapingRule::Quoted; }
    BoolValueIntoNumericColumn readsBoolValueIntoNumericColumn() const override
    {
        return !homogeneous_escaping_rule || *homogeneous_escaping_rule == FormatSettings::EscapingRule::JSON
            ? BoolValueIntoNumericColumn::AllNumeric
            : BoolValueIntoNumericColumn::None;
    }

    /// `TemplateRowInputFormat` resolves every named placeholder against the destination columns
    /// (`format_idx_to_column_idx`), so the placeholders may be reordered relative to the destination
    /// and omitted destination columns are filled with defaults. The schema reader names the inferred
    /// columns after the placeholders, so comparing them by name mirrors the parser. A row format with
    /// a placeholder without a name (`${:CSV}`, which the parser skips) cannot be represented by the
    /// inferred schema, so then no conclusion is drawn from it, nor from a row format mixing
    /// different escaping rules (see above).
    bool mapsColumnsByName() const override { return all_placeholders_are_named; }
    bool schemaDescribesParsedData() const override { return all_placeholders_are_named && homogeneous_escaping_rule.has_value(); }

    void transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type) override;

    PeekableReadBuffer buf;
    const ParsedTemplateFormatString format;
    const ParsedTemplateFormatString row_format;
    const std::optional<FormatSettings::EscapingRule> homogeneous_escaping_rule;
    const bool all_placeholders_are_named;
    TemplateFormatReader format_reader;
    bool first_row = true;
    JSONInferenceInfo json_inference_info;
    const char default_csv_delimiter;
};

bool parseDelimiterWithDiagnosticInfo(WriteBuffer & out, ReadBuffer & buf, const String & delimiter, const String & description, bool skip_spaces);

}
