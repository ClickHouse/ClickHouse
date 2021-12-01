#pragma once

#include <Processors/ISimpleTransform.h>

namespace DB
{

class StorageWindowView;

class WatermarkTransform : public ISimpleTransform
{
public:
    WatermarkTransform(
        const Block & header_,
        StorageWindowView & storage_,
        const String & window_column_name_,
        UInt32 lateness_upper_bound_);

    String getName() const override { return "WatermarkTransform"; }

    ~WatermarkTransform() override;

protected:
    void transform(Chunk & chunk) override;

    Block block_header;

    StorageWindowView & storage;
    String window_column_name;

    UInt32 lateness_upper_bound = 0;
    UInt32 max_watermark = 0;

    std::set<UInt32> late_signals;
};

}
