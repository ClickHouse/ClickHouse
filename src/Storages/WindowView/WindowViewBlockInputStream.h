#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>


namespace DB
{
/** Implements WINDOW VIEW table WATCH input stream.
 *  Keeps stream alive by outputting blocks with no rows
 *  based on window interval.
 */
class WindowViewBlockInputStream : public IBlockInputStream
{
public:
    WindowViewBlockInputStream(
        std::shared_ptr<StorageWindowView> storage_,
        const bool has_limit_,
        const UInt64 limit_,
        const UInt64 heartbeat_interval_sec_)
        : storage(std::move(storage_))
        , has_limit(has_limit_)
        , limit(limit_)
        , heartbeat_interval_sec(heartbeat_interval_sec_) {}

    String getName() const override { return "WindowViewBlock"; }

    void cancel(bool kill) override
    {
        if (isCancelled() || storage->is_dropped)
            return;
        IBlockInputStream::cancel(kill);
    }

    Block getHeader() const override { return storage->getHeader(); }

    void addBlock(Block block_)
    {
        std::lock_guard lock(blocks_mutex);
        blocks.push_back(std::move(block_));
    }

protected:
    Block readImpl() override
    {
        return tryReadImpl();
    }

    Block tryReadImpl()
    {
        Block res;

        if (has_limit && num_updates == static_cast<Int64>(limit))
            return Block();

        if (isCancelled() || storage->is_dropped)
            return Block();

        std::unique_lock lock_(blocks_mutex);
        if (blocks.empty())
        {
            if (!end_of_blocks)
            {
                end_of_blocks = true;
                num_updates += 1;
                return getHeader();
            }

            storage->fire_condition.wait_for(lock_, std::chrono::seconds(heartbeat_interval_sec));

            if (isCancelled() || storage->is_dropped)
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
    std::shared_ptr<StorageWindowView> storage;
    BlocksList blocks;
    const bool has_limit;
    const UInt64 limit;
    Int64 num_updates = -1;
    bool end_of_blocks = false;
    std::mutex blocks_mutex;
    UInt64 heartbeat_interval_sec;
};
}
