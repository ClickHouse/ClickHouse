#pragma once

#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Formats/FormatSettings.h>


namespace DB
{

/** The stream for outputting data in JSON format, by object per line.
  * Does not validate UTF-8.
  */
class JSONCompactEachRowRowOutputFormat : public IRowOutputFormat
{
public:
    JSONCompactEachRowRowOutputFormat(WriteBuffer & out_, const Block & header_, FormatFactory::WriteCallback callback, const FormatSettings & settings_);

    String getName() const override { return "JSONCompactEachRowRowOutputFormat"; }

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;

protected:
    /// No totals and extremes.
    //void consumeTotals(Chunk) override {}
    void consumeExtremes(Chunk) override {}

private:
    FormatSettings settings;
};
}
