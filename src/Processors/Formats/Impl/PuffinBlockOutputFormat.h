#pragma once

#include <Processors/Formats/IOutputFormat.h>

#include <Common/MapWithMemoryTracking.h>
#include <roaring/roaring64map.hh>

namespace DB
{

class PuffinBlockOutputFormat final : public IOutputFormat
{
public:
    PuffinBlockOutputFormat(WriteBuffer & out_, SharedHeader header_);

    String getName() const override { return "PuffinBlockOutputFormat"; }

private:
    void consume(Chunk chunk) override;
    void finalizeImpl() override;
    void resetFormatterImpl() override;

    void addPositions(roaring::Roaring64Map & bitmap, const IColumn & positions, size_t begin, size_t end) const;

    bool positions_are_arrays = false;
    bool positions_are_unsigned = false;
    MapWithMemoryTracking<String, roaring::Roaring64Map> deletion_vectors;
};

class FormatFactory;
void registerOutputFormatPuffin(FormatFactory & factory);

}
