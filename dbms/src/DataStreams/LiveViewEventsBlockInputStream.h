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
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/StorageLiveView.h>


namespace DB
{

/**
  */

class LiveViewEventsBlockInputStream : public IBlockInputStream
{

using NonBlockingResult = std::pair<Block, bool>;

public:
    ~LiveViewEventsBlockInputStream() override
    {
        /// Start storage no users thread
        /// if we are the last active user
        if (!storage.is_dropped && blocks_ptr.use_count() < 3)
            storage.startNoUsersThread();
    }
    /// length default -2 because we want LIMIT to specify number of updates so that LIMIT 1 waits for 1 update
    /// and LIMIT 0 just returns data without waiting for any updates
    LiveViewEventsBlockInputStream(StorageLiveView & storage_, std::shared_ptr<BlocksPtr> blocks_ptr_, std::shared_ptr<BlocksMetadataPtr> blocks_metadata_ptr_, std::shared_ptr<bool> active_ptr_, Poco::Condition & condition_, Poco::FastMutex & mutex_,
        int64_t length_, const UInt64 & heartbeat_delay_)
        : storage(storage_), blocks_ptr(blocks_ptr_), blocks_metadata_ptr(blocks_metadata_ptr_), active_ptr(active_ptr_), condition(condition_), mutex(mutex_), length(length_ + 1), heartbeat_delay(heartbeat_delay_)
    {
        /// grab active pointer
        active = active_ptr.lock();
    }

    String getName() const override { return "LiveViewEventsBlockInputStream"; }

    void cancel(bool kill) override
    {
        if (isCancelled() || storage.is_dropped)
            return;
        IBlockInputStream::cancel(kill);
        Poco::FastMutex::ScopedLock lock(mutex);
        condition.broadcast();
    }

    Block getHeader() const override {
        return {
            ColumnWithTypeAndName(
                ColumnUInt64::create(),
                std::make_shared<DataTypeUInt64>(),
                "version"),
            ColumnWithTypeAndName(
                ColumnString::create(),
                std::make_shared<DataTypeString>(),
                "hash")
        };
    }

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
            if (!blocks || blocks.get() != (*blocks_ptr).get()) {
                blocks = (*blocks_ptr);
                blocks_metadata = (*blocks_metadata_ptr);
            }
        }
        it = blocks->begin();
        begin = blocks->begin();
        end = blocks->end();
    }

    NonBlockingResult tryRead()
    {
        return tryRead_(false);
    }

    Block getEventBlock()
    {
        Block res{
            ColumnWithTypeAndName(
                DataTypeUInt64().createColumnConst(1, blocks_metadata->version)->convertToFullColumnIfConst(),
                std::make_shared<DataTypeUInt64>(),
                "version"),
            ColumnWithTypeAndName(
                DataTypeString().createColumnConst(1, blocks_metadata->hash)->convertToFullColumnIfConst(),
                std::make_shared<DataTypeString>(),
                "hash"),

        };
        return res;
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
            blocks_metadata = (*blocks_metadata_ptr);
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
                    blocks_metadata = (*blocks_metadata_ptr);
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
                        bool signaled = condition.tryWait(mutex, std::max((UInt64)0, heartbeat_delay - ((UInt64)timestamp.epochMicroseconds() - last_event_timestamp)) / 1000);

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
                            // repeat the event block as a heartbeat
                            last_event_timestamp = (UInt64)timestamp.epochMicroseconds();
                            return { getHeader(), true };
                        }
                    }
                }
            }
            return tryRead_(blocking);
        }

        // move right to the end
        it = end;

        if (it == end)
        {
            end_of_blocks = false;
            if (length > 0)
                --length;
        }

        last_event_timestamp = (UInt64)timestamp.epochMicroseconds();

        return { getEventBlock(), true };
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
    UInt64 heartbeat_delay;
    UInt64 last_event_timestamp{0};
    Poco::Timestamp timestamp;
};

}
