#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/IRowOutputFormat.h>

namespace DB
{

class BSONEachRowRowOutputFormat final : public IRowOutputFormat
{
public:
    BSONEachRowRowOutputFormat(
        WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSettings & settings_);

    String getName() const override { return "BSONEachRowRowOutputFormat"; }

protected:
    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const ISerialization &, size_t) override { }
    void serializeField(const IColumn & column, size_t row_num, const String & name);

    /// No totals and extremes.
    void consumeTotals(Chunk) override { }
    void consumeExtremes(Chunk) override { }

private:
    Names fields;

    UInt32 obj_size = 0;

    FormatSettings settings;
};

}
