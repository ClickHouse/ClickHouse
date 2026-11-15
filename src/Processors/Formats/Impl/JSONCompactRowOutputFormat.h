#pragma once

#include <Processors/Formats/Impl/JSONRowOutputFormat.h>

namespace DB
{

class Block;
struct FormatSettings;
class WriteBuffer;

/** The stream for outputting data in the JSONCompact- formats.
  */
class JSONCompactRowOutputFormat final : public JSONRowOutputFormat
{
public:
    JSONCompactRowOutputFormat(
        WriteBuffer & out_,
        SharedHeader header,
        const FormatSettings & settings_,
        bool yield_strings_);

    String getName() const override { return "JSONCompactRowOutputFormat"; }

private:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;

    bool supportTotals() const override { return true; }
    bool supportExtremes() const override { return true; }

    void writeBeforeTotals() override;
    void writeAfterTotals() override;

    void writeExtremesElement(const char * title, const Columns & columns, size_t row_num) override;

    void writeTotals(const Columns & columns, size_t row_num) override;
};

}
