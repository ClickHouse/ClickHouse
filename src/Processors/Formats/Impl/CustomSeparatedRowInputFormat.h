#pragma once

#include <Processors/Formats/RowInputFormatWithNamesAndTypes.h>
#include <Formats/ParsedTemplateFormatString.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/ReadHelpers.h>

namespace DB
{

class CustomSeparatedRowInputFormat final : public RowInputFormatWithNamesAndTypes
{
public:
    CustomSeparatedRowInputFormat(
        const Block & header_,
        ReadBuffer & in_,
        const Params & params_,
        bool with_names_, bool with_types_, bool ignore_spaces_, const FormatSettings & format_settings_);

    void resetParser() override;
    String getName() const override { return "CustomSeparatedRowInputFormat"; }
    void setReadBuffer(ReadBuffer & in_) override;

private:
    CustomSeparatedRowInputFormat(
        const Block & header_,
        std::unique_ptr<PeekableReadBuffer> in_buf_,
        const Params & params_,
        bool with_names_, bool with_types_, bool ignore_spaces_, const FormatSettings & format_settings_);

    bool allowSyncAfterError() const override;
    void syncAfterError() override;

    std::unique_ptr<PeekableReadBuffer> buf;
    bool ignore_spaces;
};

class CustomSeparatedFormatReader final : public FormatWithNamesAndTypesReader
{
public:
    CustomSeparatedFormatReader(PeekableReadBuffer & buf_, bool ignore_spaces_, const FormatSettings & format_settings_);

    using EscapingRule = FormatSettings::EscapingRule;

    bool readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool is_last_file_column, const String & column_name) override;

    void skipField(size_t /*file_column*/) override { skipField(); }
    void skipField();
    void skipNames() override { skipHeaderRow(); }
    void skipTypes() override { skipHeaderRow(); }
    void skipHeaderRow();

    void skipPrefixBeforeHeader() override;
    void skipRowStartDelimiter() override;
    void skipFieldDelimiter() override;
    void skipRowEndDelimiter() override;
    void skipRowBetweenDelimiter() override;

    bool checkForSuffix() override;

    bool parseRowStartWithDiagnosticInfo(WriteBuffer & out) override;
    bool parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out) override;
    bool parseRowEndWithDiagnosticInfo(WriteBuffer & out) override;
    bool parseRowBetweenDelimiterWithDiagnosticInfo(WriteBuffer & out) override;
    bool tryParseSuffixWithDiagnosticInfo(WriteBuffer & out) override;

    std::vector<String> readNames() override { return readHeaderRow(); }
    std::vector<String> readTypes() override { return readHeaderRow(); }
    std::vector<String> readHeaderRow() {return readRowImpl<true>(); }

    std::vector<String> readRow() { return readRowImpl<false>(); }

    bool checkEndOfRow();
    bool checkForSuffixImpl(bool check_eof);
    inline void skipSpaces() { if (ignore_spaces) skipWhitespaceIfAny(*buf); }

    EscapingRule getEscapingRule() { return format_settings.custom.escaping_rule; }

    void setReadBuffer(ReadBuffer & in_) override;
private:
    template <bool is_header>
    std::vector<String> readRowImpl();

    template <bool read_string>
    String readFieldIntoString(bool is_first);

    PeekableReadBuffer * buf;
    bool ignore_spaces;
    size_t columns = 0;
};

class CustomSeparatedSchemaReader : public FormatWithNamesAndTypesSchemaReader
{
public:
    CustomSeparatedSchemaReader(ReadBuffer & in_, bool with_names_, bool with_types_, bool ignore_spaces_, const FormatSettings & format_setting_, ContextPtr context_);

private:
    DataTypes readRowAndGetDataTypes() override;

    PeekableReadBuffer buf;
    CustomSeparatedFormatReader reader;
    ContextPtr context;
    bool first_row = true;
};

}
