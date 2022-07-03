#pragma once

#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/JSONRowOutputFormat.h>


namespace DB
{

struct FormatSettings;

/** The stream for outputting data in the JSONCompact- formats.
  */
class JSONCompactRowOutputFormat final : public JSONRowOutputFormat
{
public:
    JSONCompactRowOutputFormat(
        WriteBuffer & out_,
        const Block & header,
        const RowOutputFormatParams & params_,
        const FormatSettings & settings_,
        bool yield_strings_);

    String getName() const override { return "JSONCompactRowOutputFormat"; }

private:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;

    void writeBeforeTotals() override;
    void writeAfterTotals() override;

    void writeExtremesElement(const char * title, const Columns & columns, size_t row_num) override;

    void writeTotalsField(const IColumn & column, const ISerialization & serialization, size_t row_num) override
    {
        return writeField(column, serialization, row_num);
    }

    void writeTotalsFieldDelimiter() override;
};

}
