#pragma once

#include "config.h"

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/Impl/PuffinThetaSketch.h>

#include <Common/MapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <roaring/roaring64map.hh>

namespace DB
{

class PuffinBlockOutputFormat final : public IOutputFormat
{
public:
    PuffinBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_);

    String getName() const override { return "PuffinBlockOutputFormat"; }

private:
    void consume(Chunk chunk) override;
    void finalizeImpl() override;
    void resetFormatterImpl() override;

    void initDeletionVectors(const Block & header);
    void initThetaSketches(const Block & header);
    void consumeDeletionVectors(const Chunk & chunk);
    void addPositions(roaring::Roaring64Map & bitmap, const IColumn & positions, size_t begin, size_t end) const;

    const FormatSettings format_settings;
    bool write_theta_sketches = false;

    bool positions_are_arrays = false;
    bool positions_are_unsigned = false;
    MapWithMemoryTracking<String, roaring::Roaring64Map> deletion_vectors;

#if USE_DATASKETCHES
    VectorWithMemoryTracking<PuffinThetaSketch> theta_sketches;
#endif
    VectorWithMemoryTracking<Int32> theta_field_ids;
};

class FormatFactory;
void registerOutputFormatPuffin(FormatFactory & factory);

}
