#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/RowInputFormatWithNamesAndTypes.h>
#include <Processors/Formats/ISchemaReader.h>
#include <IO/PeekableReadBuffer.h>


namespace DB
{

class TabSeparatedFormatReader;

/** A stream to input data in tsv format.
  */
class TabSeparatedRowInputFormat final : public RowInputFormatWithNamesAndTypes<TabSeparatedFormatReader>
{
public:
    /** with_names - the first line is the header with the names of the columns
      * with_types - on the next line header with type names
      */
    TabSeparatedRowInputFormat(const Block & header_, ReadBuffer & in_, const Params & params_,
                               bool with_names_, bool with_types_, bool is_raw, const FormatSettings & format_settings_);

    String getName() const override { return "TabSeparatedRowInputFormat"; }

    void setReadBuffer(ReadBuffer & in_) override;
    void resetReadBuffer() override;

private:
    TabSeparatedRowInputFormat(const Block & header_, std::unique_ptr<PeekableReadBuffer> in_, const Params & params_,
                               bool with_names_, bool with_types_, bool is_raw, const FormatSettings & format_settings_);

    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;
    bool isGarbageAfterField(size_t, ReadBuffer::Position pos) override { return *pos != '\n' && *pos != '\t'; }

    bool supportsCountRows() const override { return true; }

    std::unique_ptr<PeekableReadBuffer> buf;
};

class TabSeparatedFormatReader final : public FormatWithNamesAndTypesReader
{
public:
    TabSeparatedFormatReader(PeekableReadBuffer & in_, const FormatSettings & format_settings, bool is_raw_);

    bool readField(IColumn & column, const DataTypePtr & type,
                   const SerializationPtr & serialization, bool is_last_file_column, const String & column_name) override;

    void skipField(size_t /*file_column*/) override { skipField(); }
    void skipField();
    void skipHeaderRow();
    void skipNames() override { skipHeaderRow(); }
    void skipTypes() override { skipHeaderRow(); }
    void skipFieldDelimiter() override;
    void skipRowEndDelimiter() override;
    void skipPrefixBeforeHeader() override;

    std::vector<String> readRow() { return readRowImpl<false>(); }
    std::vector<String> readNames() override { return readHeaderRow(); }
    std::vector<String> readTypes() override { return readHeaderRow(); }
    std::vector<String> readHeaderRow() { return readRowImpl<true>(); }

    void skipRow() override;

    template <bool read_string>
    String readFieldIntoString();

    std::vector<String> readRowForHeaderDetection() override { return readHeaderRow(); }

    void checkNullValueForNonNullable(DataTypePtr type) override;

    bool parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out) override;
    bool parseRowEndWithDiagnosticInfo(WriteBuffer & out) override;
    FormatSettings::EscapingRule getEscapingRule() const override
    {
        return is_raw ? FormatSettings::EscapingRule::Raw : FormatSettings::EscapingRule::Escaped;
    }

    void setReadBuffer(ReadBuffer & in_) override;

    bool checkForSuffix() override;
    bool checkForEndOfRow() override;

    bool allowVariableNumberOfColumns() const override { return format_settings.tsv.allow_variable_number_of_columns; }

private:
    template <bool is_header>
    std::vector<String> readRowImpl();

    PeekableReadBuffer * buf;
    bool is_raw;
    bool first_row = true;
};

class TabSeparatedSchemaReader : public FormatWithNamesAndTypesSchemaReader
{
public:
    TabSeparatedSchemaReader(ReadBuffer & in_, bool with_names_, bool with_types_, bool is_raw_, const FormatSettings & format_settings);

private:
    bool allowVariableNumberOfColumns() const override { return format_settings.tsv.allow_variable_number_of_columns; }

    std::optional<DataTypes> readRowAndGetDataTypesImpl() override;
    std::optional<std::pair<std::vector<String>, DataTypes>> readRowAndGetFieldsAndDataTypes() override;

    PeekableReadBuffer buf;
    TabSeparatedFormatReader reader;
};

}
