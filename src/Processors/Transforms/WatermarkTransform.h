#pragma once

#include <Processors/ISimpleTransform.h>
#include <Common/SetWithMemoryTracking.h>

namespace DB
{

class StorageWindowView;

class WatermarkTransform final : public ISimpleTransform
{
public:
    WatermarkTransform(
        SharedHeader header_,
        StorageWindowView & storage_,
        const String & window_column_name_,
        UInt32 lateness_upper_bound_);

    String getName() const override { return "WatermarkTransform"; }

    ~WatermarkTransform() override;

protected:
    void transform(Chunk & chunk) override;

    SharedHeader block_header;

    StorageWindowView & storage;
    String window_column_name;

    UInt32 lateness_upper_bound = 0;
    UInt32 max_watermark = 0;

    SetWithMemoryTracking<UInt32> late_signals;
};

}
