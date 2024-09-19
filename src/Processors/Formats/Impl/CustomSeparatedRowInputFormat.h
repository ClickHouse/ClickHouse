#pragma once

#include <Processors/Formats/RowInputFormatWithNamesAndTypes.h>
#include <Formats/ParsedTemplateFormatString.h>
#include <Formats/SchemaInferenceUtils.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/ReadHelpers.h>

namespace DB
{

class CustomSeparatedFormatReader;
class CustomSeparatedRowInputFormat final : public RowInputFormatWithNamesAndTypes<CustomSeparatedFormatReader>
{
public:
    CustomSeparatedRowInputFormat(
        const Block & header_,
        ReadBuffer & in_,
        const Params & params_,
        bool with_names_, bool with_types_, bool ignore_spaces_, const FormatSettings & format_settings_);

    String getName() const override { return "CustomSeparatedRowInputFormat"; }
    void setReadBuffer(ReadBuffer & in_) override;
    void resetReadBuffer() override;

private:
    CustomSeparatedRowInputFormat(
        const Block & header_,
        std::unique_ptr<PeekableReadBuffer> in_buf_,
        const Params & params_,
        bool with_names_, bool with_types_, bool ignore_spaces_, const FormatSettings & format_settings_);

    bool allowSyncAfterError() const override;
    void syncAfterError() override;
    void readPrefix() override;

    bool supportsCountRows() const override { return true; }

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
    void skipNames() override { skipRow(); }
    void skipTypes() override { skipRow(); }
    void skipRow() override;

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
    std::vector<String> readHeaderRow() {return readRowImpl<ReadFieldMode::AS_STRING>(); }

    std::vector<String> readRow() { return readRowImpl<ReadFieldMode::AS_FIELD>(); }

    std::vector<String> readRowForHeaderDetection() override { return readRowImpl<ReadFieldMode::AS_POSSIBLE_STRING>(); }

    bool checkForEndOfRow() override;
    bool allowVariableNumberOfColumns() const override { return format_settings.custom.allow_variable_number_of_columns; }

    bool checkForSuffixImpl(bool check_eof);
    void skipSpaces() { if (ignore_spaces) skipWhitespaceIfAny(*buf, true); }

    EscapingRule getEscapingRule() const override { return format_settings.custom.escaping_rule; }

    void setReadBuffer(ReadBuffer & in_) override;

private:
    enum class ReadFieldMode : uint8_t
    {
        AS_STRING,
        AS_FIELD,
        AS_POSSIBLE_STRING,
    };

    template <ReadFieldMode mode>
    std::vector<String> readRowImpl();

    template <ReadFieldMode mode>
    String readFieldIntoString(bool is_first, bool is_last, bool is_unknown);

    void updateFormatSettings(bool is_last_column);

    PeekableReadBuffer * buf;
    bool ignore_spaces;
    size_t columns = 0;
};

class CustomSeparatedSchemaReader : public FormatWithNamesAndTypesSchemaReader
{
public:
    CustomSeparatedSchemaReader(ReadBuffer & in_, bool with_names_, bool with_types_, bool ignore_spaces_, const FormatSettings & format_setting_);

private:
    bool allowVariableNumberOfColumns() const override { return format_settings.custom.allow_variable_number_of_columns; }

    std::optional<DataTypes> readRowAndGetDataTypesImpl() override;

    std::optional<std::pair<std::vector<String>, DataTypes>> readRowAndGetFieldsAndDataTypes() override;

    void transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type) override;

    PeekableReadBuffer buf;
    CustomSeparatedFormatReader reader;
    bool first_row = true;
    JSONInferenceInfo json_inference_info;
    bool no_more_data = false;
};

}
