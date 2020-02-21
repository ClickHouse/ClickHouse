#pragma once

#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/WindowView/StorageWindowView.h>

namespace DB
{

/** Adds a materialized const column to the block with a specified value.
  */
class WatermarkBlockInputStream : public IBlockInputStream
{
public:
    WatermarkBlockInputStream(
        BlockInputStreamPtr input_,
        StorageWindowView& storage_,
        UInt32 timestamp_)
        : storage(storage_), timestamp(timestamp_)
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

        auto column_wend = res.getByName("____w_end").column;
        const ColumnUInt32::Container & wend_data = static_cast<const ColumnUInt32 &>(*column_wend).getData();
        for (size_t i = 0; i < wend_data.size(); ++i)
        {
            if (wend_data[i] < timestamp)
                signals.push_back(wend_data[i]);
        }
        return res;
    }

    void readSuffix() override
    {
        // while (!signal.empty())
        for (auto signal : signals)
            storage.addFireSignal(signal);
        signals.clear();
    }

private:
    StorageWindowView & storage;
    UInt32 timestamp;
    std::deque<UInt32> signals;
};
}
