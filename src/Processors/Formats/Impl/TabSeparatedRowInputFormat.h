#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/RowInputFormatWithNamesAndTypes.h>
#include <Processors/Formats/ISchemaReader.h>


namespace DB
{

/** A stream to input data in tsv format.
  */
class TabSeparatedRowInputFormat final : public RowInputFormatWithNamesAndTypes
{
public:
    /** with_names - the first line is the header with the names of the columns
      * with_types - on the next line header with type names
      */
    TabSeparatedRowInputFormat(const Block & header_, ReadBuffer & in_, const Params & params_,
                               bool with_names_, bool with_types_, bool is_raw, const FormatSettings & format_settings_);

    String getName() const override { return "TabSeparatedRowInputFormat"; }

private:
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;
    bool isGarbageAfterField(size_t, ReadBuffer::Position pos) override { return *pos != '\n' && *pos != '\t'; }
};

class TabSeparatedFormatReader final : public FormatWithNamesAndTypesReader
{
public:
    TabSeparatedFormatReader(ReadBuffer & in_, const FormatSettings & format_settings, bool is_raw_);

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

    std::vector<String> readRow();
    std::vector<String> readNames() override { return readRow(); }
    std::vector<String> readTypes() override { return readRow(); }
    String readFieldIntoString();

    void checkNullValueForNonNullable(DataTypePtr type) override;

    bool parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out) override;
    bool parseRowEndWithDiagnosticInfo(WriteBuffer & out) override;
    FormatSettings::EscapingRule getEscapingRule() const
    {
        return is_raw ? FormatSettings::EscapingRule::Raw : FormatSettings::EscapingRule::Escaped;
    }

private:
    bool is_raw;
    bool first_row = true;
};

class TabSeparatedSchemaReader : public FormatWithNamesAndTypesSchemaReader
{
public:
    TabSeparatedSchemaReader(ReadBuffer & in_, bool with_names_, bool with_types_, bool is_raw_, const FormatSettings & format_settings);

private:
    DataTypes readRowAndGetDataTypes() override;

    TabSeparatedFormatReader reader;
};

}
