#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class WriteBuffer;


/** The stream for outputting data in Hive text format.
  *
  * Fields of a row are separated by the fields delimiter ('\001' by default), rows are
  * separated by the rows delimiter ('\n' by default). Values of complex types (Array, Map,
  * Tuple) are written without any quoting and are separated by a delimiter derived from the
  * fields delimiter by their nesting level, the same way Apache Hive does it.
  */
class HiveTextRowOutputFormat final : public IRowOutputFormat
{
public:
    HiveTextRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_);

    String getName() const override { return "HiveTextRowOutputFormat"; }

private:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowEndDelimiter() override;

    const FormatSettings format_settings;
};

}
