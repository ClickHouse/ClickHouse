#pragma once

#include <DataStreams/IBlockInputStream.h>


namespace DB
{
/** Implements WINDOW VIEW table WATCH input stream.
 *  Keeps stream alive by outputing blocks with no rows
 *  based on period specified by the heartbeat interval.
 */
class WindowViewBlockInputStream : public IBlockInputStream
{
public:
    WindowViewBlockInputStream(
        std::shared_ptr<StorageWindowView> storage_,
        std::shared_ptr<bool> active_ptr_,
        const bool has_limit_,
        const UInt64 limit_)
        : storage(std::move(storage_))
        , active(std::move(active_ptr_))
        , has_limit(has_limit_)
        , limit(limit_) {}

    String getName() const override { return "WindowViewBlockInputStream"; }

    void cancel(bool kill) override
    {
        if (isCancelled() || storage->is_dropped)
            return;
        IBlockInputStream::cancel(kill);
        std::lock_guard lock(storage->mutex);
        storage->condition.notify_all();
    }

    Block getHeader() const override { return storage->getHeader(); }

protected:
    Block readImpl() override
    {
        /// try reading
        return tryReadImpl();
    }

    /** tryRead method attempts to read a block in either blocking
     *  or non-blocking mode. If blocking is set to false
     *  then method return empty block with flag set to false
     *  to indicate that method would block to get the next block.
     */
    Block tryReadImpl()
    {
        Block res;

        if (has_limit && num_updates == static_cast<Int64>(limit))
        {
            return Block();
        }
        /// If blocks were never assigned get blocks
        if (!in_stream)
        {
            std::unique_lock lock(storage->mutex);
            in_stream = storage->getNewBlocksInputStreamPtr();
        }
        if (isCancelled() || storage->is_dropped)
        {
            return Block();
        }

        res = in_stream->read();
        if (!res)
        {
            if (!active)
                return Block();

            if (!end_of_blocks)
            {
                end_of_blocks = true;
                num_updates += 1;
                return getHeader();
            }

            std::unique_lock lock(storage->flushTableMutex);
            UInt64 timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds());
            UInt64 w_end = static_cast<UInt64>(storage->getWindowUpperBound(static_cast<UInt32>(timestamp_usec / 1000000))) * 1000000;
            storage->condition.wait_for(lock, std::chrono::microseconds(w_end - timestamp_usec));

            if (isCancelled() || storage->is_dropped)
            {
                return Block();
            }
            {
                std::unique_lock lock_(storage->mutex);
                in_stream = storage->getNewBlocksInputStreamPtr();
            }

            res = in_stream->read();
            if (res)
            {
                end_of_blocks = false;
                return res;
            }
            else
            {
                return getHeader();
            }
        }

        return res;
    }

private:
    std::shared_ptr<StorageWindowView> storage;
    std::shared_ptr<bool> active;
    const bool has_limit;
    const UInt64 limit;
    Int64 num_updates = -1;
    bool end_of_blocks = false;
    BlockInputStreamPtr in_stream;
};
}
