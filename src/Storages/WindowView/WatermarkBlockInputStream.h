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
        , is_tumble(true)
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
        , is_tumble(true)
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

    void setHopWindow()
    {
        is_tumble = false;
        slice_num_units = std::gcd(storage.hop_num_units, storage.window_num_units);
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
        if (is_tumble)
        {
            const ColumnTuple & column_tuple = typeid_cast<const ColumnTuple &>(*column_window);
            const ColumnUInt32::Container & wend_data = static_cast<const ColumnUInt32 &>(*column_tuple.getColumnPtr(1)).getData();
            for (size_t i = 0; i < wend_data.size(); ++i)
            {
                if (!watermark_specified && wend_data[i] > max_watermark)
                    max_watermark = wend_data[i];
                if (allowed_lateness && wend_data[i] <= lateness_upper_bound)
                    late_signals.insert(wend_data[i]);
            }
        }
        else
        {
            const ColumnUInt32::Container & slice_data = static_cast<const ColumnUInt32 &>(*column_window).getData();
            for (size_t i = 0; i < slice_data.size(); ++i)
            {
                UInt32 w_start = storage.addTime(slice_data[i], storage.hop_kind, -1 * slice_num_units);
                w_start = storage.getWindowLowerBound(w_start);
                UInt32 w_start_latest;
                do
                {
                    w_start_latest = w_start;
                    w_start = storage.addTime(w_start, storage.hop_kind, storage.hop_num_units);
                } while (w_start < slice_data[i]);

                UInt32 w_end = storage.addTime(w_start_latest, storage.window_kind, storage.window_num_units);

                if (!watermark_specified && w_end > max_watermark)
                {
                    max_watermark = w_end;
                }
                if (allowed_lateness && w_end <= lateness_upper_bound)
                    late_signals.insert(w_end);
            }
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
    bool is_tumble;
    std::set<UInt32> late_signals;
    StorageWindowView & storage;
    String window_column_name;
    UInt32 lateness_upper_bound;
    UInt32 max_timestamp;
    UInt32 max_watermark;
    Int64 slice_num_units;
};
}
