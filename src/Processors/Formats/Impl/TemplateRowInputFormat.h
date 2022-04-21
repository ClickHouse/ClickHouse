#pragma once

#include <Core/Block.h>
#include <Processors/Formats/RowInputFormatWithDiagnosticInfo.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <Formats/ParsedTemplateFormatString.h>
#include <IO/ReadHelpers.h>
#include <IO/PeekableReadBuffer.h>
#include <Interpreters/Context.h>


namespace DB
{

class TemplateFormatReader;

class TemplateRowInputFormat final : public RowInputFormatWithDiagnosticInfo
{
    using EscapingRule = FormatSettings::EscapingRule;
public:
    TemplateRowInputFormat(const Block & header_, ReadBuffer & in_, const Params & params_,
                           FormatSettings settings_, bool ignore_spaces_,
                           ParsedTemplateFormatString format_, ParsedTemplateFormatString row_format_,
                           std::string row_between_delimiter);

    String getName() const override { return "TemplateRowInputFormat"; }

    void resetParser() override;

private:
    TemplateRowInputFormat(const Block & header_, std::unique_ptr<PeekableReadBuffer> buf_, const Params & params_,
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

    void setReadBuffer(ReadBuffer & in_) override;

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
    inline void skipSpaces() { if (ignore_spaces) skipWhitespaceIfAny(*buf); }

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
    size_t row_num;
};

class TemplateSchemaReader : public IRowSchemaReader
{
public:
    TemplateSchemaReader(ReadBuffer & in_,
                         bool ignore_spaces_,
                         const ParsedTemplateFormatString & format_,
                         const ParsedTemplateFormatString & row_format_,
                         std::string row_between_delimiter,
                         const FormatSettings & format_settings_);

    DataTypes readRowAndGetDataTypes() override;

private:
    PeekableReadBuffer buf;
    const ParsedTemplateFormatString format;
    const ParsedTemplateFormatString row_format;
    FormatSettings format_settings;
    TemplateFormatReader format_reader;
    bool first_row = true;
};

bool parseDelimiterWithDiagnosticInfo(WriteBuffer & out, ReadBuffer & buf, const String & delimiter, const String & description, bool skip_spaces);

}
