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
class JSONEachRowRowOutputFormat : public IRowOutputFormat
{
public:
    JSONEachRowRowOutputFormat(
        WriteBuffer & out_,
        const Block & header_,
        const RowOutputFormatParams & params_,
        const FormatSettings & settings_);

    String getName() const override { return "JSONEachRowRowOutputFormat"; }

public:
    /// Content-Type to set when sending HTTP response.
    String getContentType() const override
    {
        return settings.json.array_of_rows ? "application/json; charset=UTF-8" : "application/x-ndjson; charset=UTF-8" ;
    }

protected:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeRowBetweenDelimiter() override;
    void writePrefix() override;
    void writeSuffix() override;

    /// No totals and extremes.
    void consumeTotals(Chunk) override {}
    void consumeExtremes(Chunk) override {}

    size_t field_number = 0;

private:
    Names fields;

    FormatSettings settings;
};

}
