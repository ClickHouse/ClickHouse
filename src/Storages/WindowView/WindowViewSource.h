#pragma once

#include <Storages/WindowView/StorageWindowView.h>
#include <Processors/Sources/SourceWithProgress.h>


namespace DB
{

class WindowViewSource : public SourceWithProgress
{
public:
    WindowViewSource(
        std::shared_ptr<StorageWindowView> storage_,
        const bool is_events_,
        String window_view_timezone_,
        const bool has_limit_,
        const UInt64 limit_,
        const UInt64 heartbeat_interval_sec_)
        : SourceWithProgress(
            is_events_ ? Block(
                {ColumnWithTypeAndName(ColumnUInt32::create(), std::make_shared<DataTypeDateTime>(window_view_timezone_), "watermark")})
                       : storage_->getHeader())
        , storage(storage_)
        , is_events(is_events_)
        , window_view_timezone(window_view_timezone_)
        , has_limit(has_limit_)
        , limit(limit_)
        , heartbeat_interval_usec(heartbeat_interval_sec_ * 1000000)
    {
        if (is_events)
            header.insert(
                ColumnWithTypeAndName(ColumnUInt32::create(), std::make_shared<DataTypeDateTime>(window_view_timezone_), "watermark"));
        else
            header = storage->getHeader();
    }

    String getName() const override { return "WindowViewSource"; }

    void addBlock(Block block_, UInt32 watermark)
    {
        std::lock_guard lock(blocks_mutex);
        blocks_with_watermark.push_back({std::move(block_), watermark});
    }

protected:
    Block getHeader() const { return header; }

    Chunk generate() override
    {
        Block block;
        UInt32 watermark;
        std::tie(block, watermark) = generateImpl();
        if (!block)
            return Chunk();
        if (is_events)
        {
            return Chunk(
                {DataTypeDateTime(window_view_timezone).createColumnConst(block.rows(), watermark)->convertToFullColumnIfConst()},
                block.rows());
        }
        else
        {
            return Chunk(block.getColumns(), block.rows());
        }
    }

    std::pair<Block, UInt32> generateImpl()
    {
        if (has_limit && num_updates == static_cast<Int64>(limit))
            return {Block(), 0};

        if (isCancelled() || storage->shutdown_called)
            return {Block(), 0};

        std::unique_lock lock(blocks_mutex);
        if (blocks_with_watermark.empty())
        {
            if (!end_of_blocks)
            {
                end_of_blocks = true;
                num_updates += 1;
                return {getHeader(), 0};
            }

            while ((Poco::Timestamp().epochMicroseconds() - last_heartbeat_timestamp_usec) < heartbeat_interval_usec)
            {
                bool signaled = std::cv_status::no_timeout == storage->fire_condition.wait_for(lock, std::chrono::microseconds(1000));
                if (signaled)
                    break;
                if (isCancelled() || storage->shutdown_called)
                    return {Block(), 0};
            }
        }

        if (!blocks_with_watermark.empty())
        {
            end_of_blocks = false;
            auto res = blocks_with_watermark.front();
            blocks_with_watermark.pop_front();
            return res;
        }
        else
        {
            last_heartbeat_timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds());
            return {getHeader(), 0};
        }
    }

private:
    std::shared_ptr<StorageWindowView> storage;

    std::list<std::pair<Block, UInt32>> blocks_with_watermark;

    Block header;
    const bool is_events;
    String window_view_timezone;
    const bool has_limit;
    const UInt64 limit;
    Int64 num_updates = -1;
    bool end_of_blocks = false;
    std::mutex blocks_mutex;
    UInt64 heartbeat_interval_usec;
    UInt64 last_heartbeat_timestamp_usec = 0;
};
}
