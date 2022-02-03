#pragma once

#include <Processors/Formats/RowInputFormatWithNamesAndTypes.h>
#include <Formats/ParsedTemplateFormatString.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/ReadHelpers.h>

namespace DB
{

class CustomSeparatedRowInputFormat : public RowInputFormatWithNamesAndTypes
{
public:
    CustomSeparatedRowInputFormat(
        const Block & header_,
        ReadBuffer & in_,
        const Params & params_,
        bool with_names_, bool with_types_, bool ignore_spaces_, const FormatSettings & format_settings_);

    void resetParser() override;
    String getName() const override { return "CustomSeparatedRowInputFormat"; }

private:
    CustomSeparatedRowInputFormat(
        const Block & header_,
        std::unique_ptr<PeekableReadBuffer> in_,
        const Params & params_,
        bool with_names_, bool with_types_, bool ignore_spaces_, const FormatSettings & format_settings_);
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

    bool allowSyncAfterError() const override;
    void syncAfterError() override;

    bool parseRowStartWithDiagnosticInfo(WriteBuffer & out) override;
    bool parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out) override;
    bool parseRowEndWithDiagnosticInfo(WriteBuffer & out) override;
    bool parseRowBetweenDelimiterWithDiagnosticInfo(WriteBuffer & out) override;
    bool tryParseSuffixWithDiagnosticInfo(WriteBuffer & out) override;

    std::vector<String> readNames() override { return readHeaderRow(); }
    std::vector<String> readTypes() override { return readHeaderRow(); }
    std::vector<String> readHeaderRow();

    bool checkEndOfRow();
    bool checkForSuffixImpl(bool check_eof);
    inline void skipSpaces() { if (ignore_spaces) skipWhitespaceIfAny(buf); }

    PeekableReadBuffer buf;
    bool ignore_spaces;
    EscapingRule escaping_rule;
};

}
