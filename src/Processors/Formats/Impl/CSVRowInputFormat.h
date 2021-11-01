#pragma once

#include <optional>
#include <unordered_map>

#include <Core/Block.h>
#include <Processors/Formats/RowInputFormatWithNamesAndTypes.h>
#include <Formats/FormatSettings.h>


namespace DB
{

/** A stream for inputting data in csv format.
  * Does not conform with https://tools.ietf.org/html/rfc4180 because it skips spaces and tabs between values.
  */
class CSVRowInputFormat : public RowInputFormatWithNamesAndTypes
{
public:
    /** with_names - in the first line the header with column names
      * with_types - on the next line header with type names
      */
    CSVRowInputFormat(const Block & header_, ReadBuffer & in_, const Params & params_,
                      bool with_names_, bool with_types_, const FormatSettings & format_settings_);

    String getName() const override { return "CSVRowInputFormat"; }

    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

private:
    bool parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out) override;
    bool parseRowEndWithDiagnosticInfo(WriteBuffer & out) override;
    bool isGarbageAfterField(size_t, ReadBuffer::Position pos) override
    {
        return *pos != '\n' && *pos != '\r' && *pos != format_settings.csv.delimiter && *pos != ' ' && *pos != '\t';
    }

    bool readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool is_last_file_column, const String & column_name) override;

    void skipField(size_t /*file_column*/) override { skipField(); }
    void skipField();

    void skipHeaderRow() ;
    void skipNames() override { skipHeaderRow(); }
    void skipTypes() override { skipHeaderRow(); }
    void skipFieldDelimiter() override;
    void skipRowEndDelimiter() override;

    std::vector<String> readHeaderRow();
    std::vector<String> readNames() override { return readHeaderRow(); }
    std::vector<String> readTypes() override { return readHeaderRow(); }

    String readFieldIntoString();
};

}
