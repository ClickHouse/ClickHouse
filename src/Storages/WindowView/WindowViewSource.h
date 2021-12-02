#pragma once

#include <Storages/WindowView/StorageWindowView.h>
#include <Processors/Sources/SourceWithProgress.h>


namespace DB
{

class WindowViewSource : public SourceWithProgress
{
public:
    WindowViewSource(
        StorageWindowView & storage_,
        const bool has_limit_,
        const UInt64 limit_,
        const UInt64 heartbeat_interval_sec_)
        : SourceWithProgress(storage_.getHeader())
        , storage(storage_)
        , has_limit(has_limit_)
        , limit(limit_)
        , heartbeat_interval_sec(heartbeat_interval_sec_) {}

    String getName() const override { return "WindowViewSource"; }

    void addBlock(Block block_)
    {
        std::lock_guard lock(blocks_mutex);
        blocks.push_back(std::move(block_));
    }

protected:
    Block getHeader() const { return storage.getHeader(); }

    Chunk generate() override
    {
        auto block = generateImpl();
        return Chunk(block.getColumns(), block.rows());
    }

    Block generateImpl()
    {
        Block res;

        if (has_limit && num_updates == static_cast<Int64>(limit))
            return Block();

        if (isCancelled() || storage.shutdown_called)
            return Block();

        std::unique_lock lock(blocks_mutex);
        if (blocks.empty())
        {
            if (!end_of_blocks)
            {
                end_of_blocks = true;
                num_updates += 1;
                return getHeader();
            }

            storage.fire_condition.wait_for(lock, std::chrono::seconds(heartbeat_interval_sec));

            if (isCancelled() || storage.shutdown_called)
            {
                return Block();
            }

            if (blocks.empty())
                return getHeader();
            else
            {
                end_of_blocks = false;
                res = blocks.front();
                blocks.pop_front();
                return res;
            }
        }
        else
        {
            res = blocks.front();
            blocks.pop_front();
            return res;
        }
    }

private:
    StorageWindowView & storage;

    BlocksList blocks;

    const bool has_limit;
    const UInt64 limit;
    Int64 num_updates = -1;
    bool end_of_blocks = false;
    std::mutex blocks_mutex;
    UInt64 heartbeat_interval_sec;
};
}
