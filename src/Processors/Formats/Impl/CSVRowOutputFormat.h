#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class WriteBuffer;


/** The stream for outputting data in csv format.
  * Does not conform with https://tools.ietf.org/html/rfc4180 because it uses LF, not CR LF.
  */
class CSVRowOutputFormat : public IRowOutputFormat
{
public:
    /** with_names - output in the first line a header with column names
      * with_types - output in the next line header with the names of the types
      */
    CSVRowOutputFormat(WriteBuffer & out_, const Block & header_, bool with_names_, FormatFactory::WriteCallback callback, const FormatSettings & format_settings_);

    String getName() const override { return "CSVRowOutputFormat"; }

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeBeforeTotals() override;
    void writeBeforeExtremes() override;

    void doWritePrefix() override;

    /// https://www.iana.org/assignments/media-types/text/csv
    String getContentType() const override
    {
        return String("text/csv; charset=UTF-8; header=") + (with_names ? "present" : "absent");
    }

protected:

    bool with_names;
    const FormatSettings format_settings;
    DataTypes data_types;
};

}
