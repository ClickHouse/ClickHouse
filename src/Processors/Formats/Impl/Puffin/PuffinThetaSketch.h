#pragma once

#include "config.h"

#if USE_DATASKETCHES

#include <Processors/Formats/Impl/Puffin/PuffinBlobBuilder.h>
#include <Processors/Formats/Impl/Puffin/PuffinCommon.h>
#include <DataTypes/IDataType.h>

#include <theta_sketch.hpp>

namespace DB
{

class IColumn;

class PuffinThetaSketchBuilder final : public IPuffinBlobBuilder
{
public:
    PuffinThetaSketchBuilder(const Block & header, const FormatSettings & format_settings);

    void consume(const Chunk & chunk) override;
    void write(PuffinWriter & writer) override;
    void reset() override;

private:
    class ColumnSketch
    {
    public:
        ColumnSketch(const String & column_name, const DataTypePtr & type);

        void add(const IColumn & column);
        const datasketches::update_theta_sketch & get() const { return sketch; }
        void reset() { sketch.reset(); }

    private:
        void update(const void * data, size_t size);

        DataTypePtr type;
        datasketches::update_theta_sketch sketch;
    };

    VectorWithMemoryTracking<ColumnSketch> sketches;
    VectorWithMemoryTracking<Int32> field_ids;
    Int64 snapshot_id = -1;
    Int64 sequence_number = -1;
};

}

#endif
