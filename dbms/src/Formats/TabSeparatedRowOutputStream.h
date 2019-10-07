#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Formats/IRowOutputStream.h>


namespace DB
{

class WriteBuffer;

/** A stream for outputting data in tsv format.
  */
class TabSeparatedRowOutputStream : public IRowOutputStream
{
public:
    /** with_names - output in the first line a header with column names
      * with_types - output the next line header with the names of the types
      */
    TabSeparatedRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool with_names_, bool with_types_, const FormatSettings & format_settings);

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowEndDelimiter() override;
    void writePrefix() override;
    void writeSuffix() override;

    void flush() override;

    void setTotals(const Block & totals_) override { totals = totals_; }
    void setExtremes(const Block & extremes_) override { extremes = extremes_; }

    /// https://www.iana.org/assignments/media-types/text/tab-separated-values
    String getContentType() const override { return "text/tab-separated-values; charset=UTF-8"; }

protected:
    void writeTotals();
    void writeExtremes();

    WriteBuffer & ostr;
    const Block sample;
    bool with_names;
    bool with_types;
    const FormatSettings format_settings;
    Block totals;
    Block extremes;
};

}

