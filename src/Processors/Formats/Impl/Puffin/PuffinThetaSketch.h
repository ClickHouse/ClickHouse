#pragma once

#include <Processors/Formats/Impl/Puffin/PuffinBlobBuilder.h>
#include <Processors/Formats/Impl/Puffin/PuffinCommon.h>
#include <DataTypes/IDataType.h>

#include <memory>

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
    Serialized serialize() const;
    void reset();

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

PuffinBlob makeThetaSketchBlobMetadata(Int32 field_id, Int64 snapshot_id, Int64 sequence_number, UInt64 ndv);

PuffinBlobBuilderPtr createPuffinThetaSketchBuilder(const Block & header, const FormatSettings & format_settings);

}
