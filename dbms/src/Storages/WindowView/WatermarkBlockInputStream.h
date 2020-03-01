#pragma once

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
        StorageWindowView& storage_)
        : need_late_signal(false)
        , watermark_specified(false)
        , storage(storage_)
        , max_watermark(0)
    {
        children.push_back(input_);
    }

    WatermarkBlockInputStream(
        BlockInputStreamPtr input_,
        StorageWindowView& storage_,
        UInt32 max_watermark_)
        : need_late_signal(false)
        , watermark_specified(true)
        , storage(storage_)
        , max_watermark(max_watermark_)
    {
        children.push_back(input_);
    }

    String getName() const override { return "Watermark"; }

    Block getHeader() const override
    {
        return children.back()->getHeader();
    }

protected:
    Block readImpl() override
    {
        Block res = children.back()->read();
        if (!res)
            return res;

        auto & column_wend = res.getByName("____w_end").column;
        const ColumnUInt32::Container & wend_data = static_cast<const ColumnUInt32 &>(*column_wend).getData();
        for (size_t i = 0; i < wend_data.size(); ++i)
        {
            if (!watermark_specified && wend_data[i] > max_watermark)
                max_watermark = wend_data[i];
            if (need_late_signal && wend_data[i] < late_timestamp)
                late_signals.push_back(wend_data[i]);
        }
        return res;
    }

    void readSuffix() override
    {
        if (need_late_signal)
            storage.addFireSignal(late_signals);
        if (max_watermark > 0)
            storage.updateMaxWatermark(max_watermark);
    }

private:
    bool need_late_signal;
    bool watermark_specified;
    std::deque<UInt32> late_signals;
    StorageWindowView & storage;
    UInt32 late_timestamp;
    UInt32 max_watermark;
};
}
