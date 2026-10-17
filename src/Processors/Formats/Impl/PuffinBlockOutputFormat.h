#pragma once

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IOutputFormat.h>

#include <map>
#include <vector>
#include <roaring/roaring.hh>

namespace DB
{

class PuffinBlockOutputFormat final : public IOutputFormat
{
public:
    PuffinBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_);

    String getName() const override { return "PuffinBlockOutputFormat"; }

private:
    void writePrefix() override;
    void consume(Chunk chunk) override;
    void finalizeImpl() override;
    void resetFormatterImpl() override;

    void addPositions(const IColumn & positions, size_t begin, size_t end);

    String referenced_data_file;
    Int64 snapshot_id;
    Int64 sequence_number;
    std::vector<Int32> field_ids;
    bool positions_are_arrays = false;
    bool positions_are_unsigned = false;
    std::map<UInt32, roaring::Roaring> bitmaps;
};

class FormatFactory;
void registerOutputFormatPuffin(FormatFactory & factory);

}
