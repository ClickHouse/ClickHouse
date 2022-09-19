#pragma once

#include <Formats/FormatSettings.h>
#include <Processors/Formats/Impl/TabSeparatedRowOutputFormat.h>


namespace DB
{

/** The stream for outputting data in the TSKV format.
  * TSKV is similar to TabSeparated, but before every value, its name and equal sign are specified: name=value.
  * This format is very inefficient.
  */
class TSKVRowOutputFormat final : public TabSeparatedRowOutputFormat
{
public:
    TSKVRowOutputFormat(WriteBuffer & out_, const Block & header, const RowOutputFormatParams & params_, const FormatSettings & format_settings);

    String getName() const override { return "TSKVRowOutputFormat"; }

private:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeRowEndDelimiter() override;

    NamesAndTypes fields;
    size_t field_number = 0;
};

}
