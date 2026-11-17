#pragma once

#include <Processors/Formats/Impl/Puffin/PuffinBlobBuilder.h>
#include <Processors/Formats/Impl/Puffin/PuffinCommon.h>
#include <DataTypes/IDataType.h>

#include <memory>
#include <vector>

namespace DB
{

class IColumn;

class PuffinColumnThetaSketch
{
public:
    struct Serialized
    {
        String bytes;
        UInt64 ndv = 0;
    };

    static bool isSupportedType(const DataTypePtr & type);

    explicit PuffinColumnThetaSketch(const DataTypePtr & type);
    PuffinColumnThetaSketch(PuffinColumnThetaSketch && other) noexcept;
    PuffinColumnThetaSketch & operator=(PuffinColumnThetaSketch && other) noexcept;
    ~PuffinColumnThetaSketch();

    void update(const IColumn & column);
    [[nodiscard]] Serialized serialize() const;
    void reset();

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

PuffinBlob makeThetaSketchBlobMetadata(Int32 field_id, Int64 snapshot_id, Int64 sequence_number, UInt64 ndv);

struct PuffinThetaSketchBlobOptions
{
    std::vector<Int32> field_ids;
    Int64 snapshot_id;
    Int64 sequence_number;
};

PuffinBlobBuilderPtr createPuffinThetaSketchBuilder(const Block & header, PuffinThetaSketchBlobOptions options);

}
