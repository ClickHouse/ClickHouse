#pragma once

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/WindowView/StorageWindowView.h>

namespace DB
{

class WatermarkBlockInputStream : public IBlockInputStream
{
public:
    WatermarkBlockInputStream(
        BlockInputStreamPtr input_,
        StorageWindowView& storage_,
        String& window_column_name_)
        : allowed_lateness(false)
        , update_timestamp(false)
        , watermark_specified(false)
        , storage(storage_)
        , window_column_name(window_column_name_)
        , lateness_upper_bound(0)
        , max_timestamp(0)
        , max_watermark(0)
    {
        children.push_back(input_);
    }

    WatermarkBlockInputStream(
        BlockInputStreamPtr input_,
        StorageWindowView& storage_,
        String& window_column_name_,
        UInt32 max_watermark_)
        : allowed_lateness(false)
        , update_timestamp(false)
        , watermark_specified(true)
        , storage(storage_)
        , window_column_name(window_column_name_)
        , lateness_upper_bound(0)
        , max_timestamp(0)
        , max_watermark(max_watermark_)
    {
        children.push_back(input_);
    }

    String getName() const override { return "Watermark"; }

    Block getHeader() const override
    {
        return children.back()->getHeader();
    }

    void setAllowedLateness(UInt32 upper_bound)
    {
        allowed_lateness = true;
        lateness_upper_bound = upper_bound;
    }

    void setMaxTimestamp(UInt32 timestamp)
    {
        update_timestamp = true;
        max_timestamp = timestamp;
    }

protected:
    Block readImpl() override
    {
        Block res = children.back()->read();
        if (!res)
            return res;

        auto & column_window = res.getByName(window_column_name).column;
        const ColumnTuple & column_tuple = typeid_cast<const ColumnTuple &>(*column_window);
        const ColumnUInt32::Container & wend_data = static_cast<const ColumnUInt32 &>(*column_tuple.getColumnPtr(1)).getData();
        for (size_t i = 0; i < wend_data.size(); ++i)
        {
            if (!watermark_specified && wend_data[i] > max_watermark)
                max_watermark = wend_data[i];
            if (allowed_lateness && wend_data[i] <= lateness_upper_bound)
                late_signals.insert(wend_data[i]);
        }
        return res;
    }

    void readSuffix() override
    {
        if (update_timestamp)
            storage.updateMaxTimestamp(max_timestamp);
        if (max_watermark > 0)
            storage.updateMaxWatermark(max_watermark);
        if (allowed_lateness)
            storage.addFireSignal(late_signals);
    }

private:
    bool allowed_lateness;
    bool update_timestamp;
    bool watermark_specified;
    std::set<UInt32> late_signals;
    StorageWindowView & storage;
    String window_column_name;
    UInt32 lateness_upper_bound;
    UInt32 max_timestamp;
    UInt32 max_watermark;
};
}
