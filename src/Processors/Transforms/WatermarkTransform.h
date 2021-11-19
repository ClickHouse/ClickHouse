#pragma once

#include <Processors/ISimpleTransform.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>


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
        UInt32 max_timestamp_,
        UInt32 lateness_upper_bound_)
        : ISimpleTransform(header_, header_, true)
        , block_header(header_)
        , storage(storage_)
        , window_column_name(window_column_name_)
        , max_timestamp(max_timestamp_)
        , lateness_upper_bound(lateness_upper_bound_)
        , allowed_lateness(lateness_upper_bound)
    {
    }

    String getName() const override { return "WatermarkTransform"; }

protected:
    void transform(Chunk & chunk) override
    {
        auto num_rows = chunk.getNumRows();
        auto columns = chunk.detachColumns();

        auto column_window_idx = block_header.getPositionByName(window_column_name);
        const auto & window_column = columns[column_window_idx];
        const ColumnUInt32::Container & wend_data = static_cast<const ColumnUInt32 &>(*window_column).getData();
        for (const auto & ts : wend_data)
        {
            if (ts > max_watermark)
                max_watermark = ts;
            if (allowed_lateness && ts <= lateness_upper_bound)
                late_signals.insert(ts);
        }

        chunk.setColumns(std::move(columns), num_rows);
    }

    Block block_header;

    StorageWindowView & storage;
    String window_column_name;

    UInt32 max_timestamp;
    UInt32 lateness_upper_bound = 0;
    UInt32 max_watermark = 0;

    std::set<UInt32> late_signals;

    bool allowed_lateness = false;
    bool update_timestamp = false;
};

}
