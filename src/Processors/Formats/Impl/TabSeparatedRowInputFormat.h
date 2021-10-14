#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/RowInputFormatWithNamesAndTypes.h>


namespace DB
{

/** A stream to input data in tsv format.
  */
class TabSeparatedRowInputFormat : public RowInputFormatWithNamesAndTypes
{
public:
    /** with_names - the first line is the header with the names of the columns
      * with_types - on the next line header with type names
      */
    TabSeparatedRowInputFormat(const Block & header_, ReadBuffer & in_, const Params & params_,
                               bool with_names_, bool with_types_, bool is_raw, const FormatSettings & format_settings_);

    String getName() const override { return "TabSeparatedRowInputFormat"; }

    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

private:
    bool is_raw;

    bool readField(IColumn & column, const DataTypePtr & type,
                   const SerializationPtr & serialization, bool is_last_file_column, const String & column_name) override;

    void skipField(const String & /*column_name*/) override { skipField(); }
    void skipField();
    void skipRow() override;
    void skipFieldDelimiter() override;
    void skipRowEndDelimiter() override;

    Names readHeaderRow() override;
    String readFieldIntoString();

    void checkNullValueForNonNullable(DataTypePtr type) override;

    bool parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out) override;
    bool parseRowEndWithDiagnosticInfo(WriteBuffer & out) override;
    bool isGarbageAfterField(size_t, ReadBuffer::Position pos) override { return *pos != '\n' && *pos != '\t'; }
};

}
