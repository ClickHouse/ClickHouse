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
    ~WindowViewBlockInputStream() override
    {
        /// Start storage no users thread
        /// if we are the last active user
        if (!storage->is_dropped)
            storage->startNoUsersThread(temporary_window_view_timeout_sec);
    }

    WindowViewBlockInputStream(
        std::shared_ptr<StorageWindowView> storage_,
        std::shared_ptr<bool> active_ptr_,
        const bool has_limit_,
        const UInt64 limit_,
        // const UInt64 heartbeat_interval_sec_,
        const UInt64 temporary_window_view_timeout_sec_)
        : storage(std::move(storage_))
        // , active_ptr(std::move(active_ptr_))
        , active(std::move(active_ptr_))
        , has_limit(has_limit_)
        , limit(limit_)
        , temporary_window_view_timeout_sec(temporary_window_view_timeout_sec_)
    {
        /// grab active pointer
        // active = active_ptr.lock();
    }

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
        if (!blocks)
        {
            if (!active)
                return Block();
            // std::unique_lock lock(storage->mutex);
            blocks = storage->getNewBlocks();
            it = blocks->begin();
            begin = blocks->begin();
            end = blocks->end();
        }

        if (isCancelled() || storage->is_dropped)
        {
            return Block();
        }

        if (it == end)
        {
            if (!active)
                return Block();
            if (storage->refreshBlockStatus())
            {
                // std::unique_lock lock(storage->mutex);
                blocks = storage->getNewBlocks();
                it = blocks->begin();
                begin = blocks->begin();
                end = blocks->end();
            }
            /// No new blocks available wait for new ones
            else
            {
                std::unique_lock lock(storage->flushTableMutex);
                // std::unique_lock lock(storage->mutex);
                if (!end_of_blocks)
                {
                    end_of_blocks = true;
                    return getHeader();
                }
                while (true)
                {
                    UInt64 timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds());
                    UInt64 w_end = static_cast<UInt64>(storage->getWindowUpperBound((UInt32)(timestamp_usec / 1000000))) * 1000000;
                    storage->condition.wait_for(lock, std::chrono::microseconds(w_end - timestamp_usec));

                    if (isCancelled() || storage->is_dropped)
                    {
                        std::cout << "AAAAAAAAAAAAAAAA Cancelled:" << std::endl;
                        return Block();
                    }
                    if (storage->refreshBlockStatus())
                    {
                        break;
                    }
                    else
                    {
                        return getHeader();
                    }
                }
            }
            return tryReadImpl();
        }

        res = *it;

        ++it;

        if (it == end)
        {
            end_of_blocks = false;
            num_updates += 1;
        }

        // last_event_timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds());
        // last_event_timestamp_usec = timestamp_usec;
        return res;
    }

private:
    std::shared_ptr<StorageWindowView> storage;
    // std::weak_ptr<bool> active_ptr;
    std::shared_ptr<bool> active;
    BlocksPtr blocks;

    // std::mutex mutex;
    Blocks::iterator it;
    Blocks::iterator end;
    Blocks::iterator begin;
    const bool has_limit;
    const UInt64 limit;
    Int64 num_updates = 0;
    bool end_of_blocks = false;
    // UInt64 heartbeat_interval_usec;
    UInt64 temporary_window_view_timeout_sec;
    // UInt64 last_event_timestamp_usec = 0;
    // UInt64 reschedule_us;
};
}
