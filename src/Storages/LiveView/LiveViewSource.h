#pragma once

#include <Storages/LiveView/StorageLiveView.h>
#include <Processors/ISource.h>


namespace DB
{

/** Implements LIVE VIEW table WATCH input stream.
 *  Keeps stream alive by outputting blocks with no rows
 *  based on period specified by the heartbeat interval.
 */
class LiveViewSource : public ISource
{

using NonBlockingResult = std::pair<Block, bool>;

public:
    LiveViewSource(std::shared_ptr<StorageLiveView> storage_,
        std::shared_ptr<BlocksPtr> blocks_ptr_,
        std::shared_ptr<BlocksMetadataPtr> blocks_metadata_ptr_,
        std::shared_ptr<bool> active_ptr_,
        const bool has_limit_, const UInt64 limit_,
        const UInt64 heartbeat_interval_sec_)
        : ISource(storage_->getHeader())
        , storage(std::move(storage_)), blocks_ptr(std::move(blocks_ptr_)),
          blocks_metadata_ptr(std::move(blocks_metadata_ptr_)),
          active_ptr(active_ptr_),
          has_limit(has_limit_), limit(limit_),
          heartbeat_interval_usec(heartbeat_interval_sec_ * 1000000)
    {
        /// grab active pointer
        active = active_ptr.lock();
    }

    String getName() const override { return "LiveViewSource"; }

    void onCancel() noexcept override
    {
        if (storage->shutdown_called)
            return;

        std::lock_guard lock(storage->mutex);
        storage->condition.notify_all();
    }

    void refresh()
    {
        if (active && blocks && it == end)
            it = blocks->begin();
    }

    void suspend()
    {
        active.reset();
    }

    void resume()
    {
        active = active_ptr.lock();
        {
            if (!blocks || blocks.get() != (*blocks_ptr).get())
                blocks = (*blocks_ptr);
        }
        it = blocks->begin();
        begin = blocks->begin();
        end = blocks->end();
    }

    NonBlockingResult tryRead()
    {
        return tryReadImpl(false);
    }

protected:
    Chunk generate() override
    {
        /// try reading
        auto block = tryReadImpl(true).first;
        return Chunk(block.getColumns(), block.rows());
    }

    /** tryRead method attempts to read a block in either blocking
     *  or non-blocking mode. If blocking is set to false
     *  then method return empty block with flag set to false
     *  to indicate that method would block to get the next block.
     */
    NonBlockingResult tryReadImpl(bool blocking)
    {
        Block res;

        if (has_limit && num_updates == static_cast<Int64>(limit))
        {
            return { Block(), true };
        }
        /// If blocks were never assigned get blocks
        if (!blocks)
        {
            std::lock_guard lock(storage->mutex);
            if (!active)
                return { Block(), false };
            blocks = (*blocks_ptr);
            it = blocks->begin();
            begin = blocks->begin();
            end = blocks->end();
        }

        if (isCancelled() || storage->shutdown_called)
        {
            return { Block(), true };
        }

        if (it == end)
        {
            {
                std::unique_lock lock(storage->mutex);
                if (!active)
                    return { Block(), false };
                /// If we are done iterating over our blocks
                /// and there are new blocks available then get them
                if (blocks.get() != (*blocks_ptr).get())
                {
                    blocks = (*blocks_ptr);
                    it = blocks->begin();
                    begin = blocks->begin();
                    end = blocks->end();
                }
                /// No new blocks available wait for new ones
                else
                {
                    if (!blocking)
                    {
                        return { Block(), false };
                    }
                    if (!end_of_blocks)
                    {
                        end_of_blocks = true;
                        return { getPort().getHeader(), true };
                    }
                    while (true)
                    {
                        UInt64 timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds());

                        /// Or spurious wakeup.
                        bool signaled = std::cv_status::no_timeout == storage->condition.wait_for(lock,
                            std::chrono::microseconds(std::max(UInt64(0), heartbeat_interval_usec - (timestamp_usec - last_event_timestamp_usec))));
                        if (isCancelled() || storage->shutdown_called)
                        {
                            return { Block(), true };
                        }
                        if (signaled)
                        {
                            break;
                        }

                        // heartbeat
                        last_event_timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds());
                        return {getPort().getHeader(), true};
                    }
                }
            }
            return tryReadImpl(blocking);
        }

        res = *it;

        ++it;

        if (it == end)
        {
            end_of_blocks = false;
            num_updates += 1;
        }

        last_event_timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds());
        return { res, true };
    }

private:
    std::shared_ptr<StorageLiveView> storage;
    std::shared_ptr<BlocksPtr> blocks_ptr;
    std::shared_ptr<BlocksMetadataPtr> blocks_metadata_ptr;
    std::weak_ptr<bool> active_ptr;
    std::shared_ptr<bool> active;
    BlocksPtr blocks;
    BlocksMetadataPtr blocks_metadata;
    Blocks::iterator it;
    Blocks::iterator end;
    Blocks::iterator begin;
    const bool has_limit;
    const UInt64 limit;
    Int64 num_updates = -1;
    bool end_of_blocks = false;
    UInt64 heartbeat_interval_usec;
    UInt64 last_event_timestamp_usec = 0;
};

}
