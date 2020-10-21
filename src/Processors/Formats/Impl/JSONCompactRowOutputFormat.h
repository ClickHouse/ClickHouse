#pragma once

#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/JSONRowOutputFormat.h>


namespace DB
{

struct FormatSettings;

/** The stream for outputting data in the JSONCompact format.
  */
class JSONCompactRowOutputFormat : public JSONRowOutputFormat
{
public:
    JSONCompactRowOutputFormat(WriteBuffer & out_, const Block & header, FormatFactory::WriteCallback callback, const FormatSettings & settings_);

    String getName() const override { return "JSONCompactRowOutputFormat"; }

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;

    void writeBeforeTotals() override;
    void writeAfterTotals() override;

protected:
    void writeExtremesElement(const char * title, const Columns & columns, size_t row_num) override;

    void writeTotalsField(const IColumn & column, const IDataType & type, size_t row_num) override
    {
        return writeField(column, type, row_num);
    }

    void writeTotalsFieldDelimiter() override;

};

}
