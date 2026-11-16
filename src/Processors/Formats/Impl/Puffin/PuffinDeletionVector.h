#pragma once

#include <Processors/Formats/Impl/Puffin/PuffinBlobBuilder.h>
#include <Processors/Formats/Impl/Puffin/PuffinCommon.h>

#include <roaring/roaring64map.hh>

namespace DB
{

class IColumn;

class PuffinDeletionVectorBuilder final : public IPuffinBlobBuilder
{
public:
    explicit PuffinDeletionVectorBuilder(const Block & header);

    void consume(const Chunk & chunk) override;
    void write(PuffinWriter & writer) override;
    void reset() override;

private:
    void addPositions(roaring::Roaring64Map & bitmap, const IColumn & positions, size_t begin, size_t end) const;

    bool positions_are_arrays = false;
    bool positions_are_unsigned = false;
    MapWithMemoryTracking<String, roaring::Roaring64Map> deletion_vectors;
};

}
