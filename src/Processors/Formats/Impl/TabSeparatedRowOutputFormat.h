#pragma once

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

class WriteBuffer;

/** A stream for outputting data in tsv format.
  */
class TabSeparatedRowOutputFormat : public IRowOutputFormat
{
public:
    /** with_names - output in the first line a header with column names
      * with_types - output the next line header with the names of the types
      */
    TabSeparatedRowOutputFormat(
        WriteBuffer & out_,
        const Block & header_,
        bool with_names_,
        bool with_types_,
        bool is_raw_,
        const FormatSettings & format_settings_);

    String getName() const override { return "TabSeparatedRowOutputFormat"; }

protected:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeFieldDelimiter() final;
    void writeRowEndDelimiter() override;

    bool supportTotals() const override { return true; }
    bool supportExtremes() const override { return true; }

    void writeBeforeTotals() final;
    void writeBeforeExtremes() final;

    void writePrefix() override;
    void writeLine(const std::vector<String> & values);

    bool with_names;
    bool with_types;
    bool is_raw;
    const FormatSettings format_settings;
};

}
