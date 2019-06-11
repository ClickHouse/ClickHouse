/* Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#pragma once

#include <limits>

#include <Common/ConcurrentBoundedQueue.h>
#include <Poco/Condition.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/StorageLiveView.h>


namespace DB
{

/**
  */

class LiveViewBlockInputStream : public IBlockInputStream
{

using NonBlockingResult = std::pair<Block, bool>;

public:
    ~LiveViewBlockInputStream() override
    {
        /// Start storage no users thread
        /// if we are the last active user
        if (!storage.is_dropped && blocks_ptr.use_count() < 3)
            storage.startNoUsersThread(temporary_live_view_timeout);
    }
    /// length default -2 because we want LIMIT to specify number of updates so that LIMIT 1 waits for 1 update
    /// and LIMIT 0 just returns data without waiting for any updates
    LiveViewBlockInputStream(StorageLiveView & storage_, std::shared_ptr<BlocksPtr> blocks_ptr_,
        std::shared_ptr<BlocksMetadataPtr> blocks_metadata_ptr_,
        std::shared_ptr<bool> active_ptr_, Poco::Condition & condition_, Poco::FastMutex & mutex_,
        int64_t length_, const UInt64 & heartbeat_interval_,
        const UInt64 & temporary_live_view_timeout_)
        : storage(storage_), blocks_ptr(blocks_ptr_), blocks_metadata_ptr(blocks_metadata_ptr_), active_ptr(active_ptr_), condition(condition_), mutex(mutex_), length(length_ + 1), heartbeat_interval(heartbeat_interval_ * 1000000), temporary_live_view_timeout(temporary_live_view_timeout_),
        blocks_hash("")
    {
        /// grab active pointer
        active = active_ptr.lock();
    }

    String getName() const override { return "LiveViewBlockInputStream"; }

    void cancel(bool kill) override
    {
        if (isCancelled() || storage.is_dropped)
            return;
        IBlockInputStream::cancel(kill);
        Poco::FastMutex::ScopedLock lock(mutex);
        condition.broadcast();
    }

    Block getHeader() const override { return storage.getHeader(); }

    void refresh()
    {
        if (active && blocks && it == end)
            it =  blocks->begin();
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
        return tryRead_(false);
    }

protected:
    Block readImpl() override
    {
        /// try reading
        return tryRead_(true).first;
    }

    /** tryRead method attempts to read a block in either blocking
     *  or non-blocking mode. If blocking is set to false
     *  then method return empty block with flag set to false
     *  to indicate that method would block to get the next block.
     */
    NonBlockingResult tryRead_(bool blocking)
    {
        Block res;

        if (length == 0)
        {
            return { Block(), true };
        }
        /// If blocks were never assigned get blocks
        if (!blocks)
        {
            Poco::FastMutex::ScopedLock lock(mutex);
            if (!active)
                return { Block(), false };
            blocks = (*blocks_ptr);
            it = blocks->begin();
            begin = blocks->begin();
            end = blocks->end();
        }

        if (isCancelled() || storage.is_dropped)
        {
            return { Block(), true };
        }

        if (it == end)
        {
            {
                Poco::FastMutex::ScopedLock lock(mutex);
                if (!active)
                    return { Block(), false };
                /// If we are done iterating over our blocks
                /// and there are new blocks availble then get them
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
                        return { getHeader(), true };
                    }
                    while (true)
                    {
                        UInt64 timestamp_usec = static_cast<UInt64>(timestamp.epochMicroseconds());
                        bool signaled = condition.tryWait(mutex, std::max(static_cast<UInt64>(0), heartbeat_interval - (timestamp_usec - last_event_timestamp)) / 1000);

                        if (isCancelled() || storage.is_dropped)
                        {
                            return { Block(), true };
                        }
                        if (signaled)
                        {
                            break;
                        }
                        else
                        {
                            // heartbeat
                            last_event_timestamp = static_cast<UInt64>(timestamp.epochMicroseconds());
                            return { getHeader(), true };
                        }
                    }
                }
            }
            return tryRead_(blocking);
        }

        res = *it;

        ++it;

        if (it == end)
        {
            end_of_blocks = false;
            if (length > 0)
                --length;
        }

        last_event_timestamp = static_cast<UInt64>(timestamp.epochMicroseconds());
        return { res, true };
    }

private:
    StorageLiveView & storage;
    std::shared_ptr<BlocksPtr> blocks_ptr;
    std::shared_ptr<BlocksMetadataPtr> blocks_metadata_ptr;
    std::weak_ptr<bool> active_ptr;
    std::shared_ptr<bool> active;
    BlocksPtr blocks;
    BlocksMetadataPtr blocks_metadata;
    Blocks::iterator it;
    Blocks::iterator end;
    Blocks::iterator begin;
    Poco::Condition & condition;
    Poco::FastMutex & mutex;
    /// Length specifies number of updates to send, default -1 (no limit)
    int64_t length;
    bool end_of_blocks{0};
    UInt64 heartbeat_interval;
    UInt64 temporary_live_view_timeout;
    String blocks_hash;
    UInt64 last_event_timestamp{0};
    Poco::Timestamp timestamp;
};

}
