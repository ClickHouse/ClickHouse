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

#include <Poco/Condition.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/LiveView/StorageLiveView.h>


namespace DB
{

/** Implements LIVE VIEW table WATCH EVENTS input stream.
 *  Keeps stream alive by outputing blocks with no rows
 *  based on period specified by the heartbeat interval.
 */
class LiveViewEventsBlockInputStream : public IBlockInputStream
{

using NonBlockingResult = std::pair<Block, bool>;

public:
    ~LiveViewEventsBlockInputStream() override
    {
        /// Start storage no users thread
        /// if we are the last active user
        if (!storage->is_dropped && blocks_ptr.use_count() < 3)
            storage->startNoUsersThread(temporary_live_view_timeout_sec);
    }
    /// length default -2 because we want LIMIT to specify number of updates so that LIMIT 1 waits for 1 update
    /// and LIMIT 0 just returns data without waiting for any updates
    LiveViewEventsBlockInputStream(std::shared_ptr<StorageLiveView> storage_,
        std::shared_ptr<BlocksPtr> blocks_ptr_,
        std::shared_ptr<BlocksMetadataPtr> blocks_metadata_ptr_,
        std::shared_ptr<bool> active_ptr_,
        const bool has_limit_, const UInt64 limit_,
        const UInt64 heartbeat_interval_sec_,
        const UInt64 temporary_live_view_timeout_sec_)
        : storage(std::move(storage_)), blocks_ptr(std::move(blocks_ptr_)),
          blocks_metadata_ptr(std::move(blocks_metadata_ptr_)),
          active_ptr(std::move(active_ptr_)), has_limit(has_limit_),
          limit(limit_),
          heartbeat_interval_usec(heartbeat_interval_sec_ * 1000000),
          temporary_live_view_timeout_sec(temporary_live_view_timeout_sec_)
    {
        /// grab active pointer
        active = active_ptr.lock();
    }

    String getName() const override { return "LiveViewEventsBlockInputStream"; }

    void cancel(bool kill) override
    {
        if (isCancelled() || storage->shutdown_called)
            return;
        IBlockInputStream::cancel(kill);
        std::lock_guard lock(storage->mutex);
        storage->condition.notify_all();
    }

    Block getHeader() const override
    {
        return {ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "version")};
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
            {
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
        return tryReadImpl(false);
    }

    Block getEventBlock()
    {
        Block res{
            ColumnWithTypeAndName(
                DataTypeUInt64().createColumnConst(1, blocks_metadata->version)->convertToFullColumnIfConst(),
                std::make_shared<DataTypeUInt64>(),
                "version")
        };
        return res;
    }
protected:
    Block readImpl() override
    {
        /// try reading
        return tryReadImpl(true).first;
    }

    /** tryRead method attempts to read a block in either blocking
     *  or non-blocking mode. If blocking is set to false
     *  then method return empty block with flag set to false
     *  to indicate that method would block to get the next block.
     */
    NonBlockingResult tryReadImpl(bool blocking)
    {
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
            blocks_metadata = (*blocks_metadata_ptr);
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
                        UInt64 timestamp_usec = static_cast<UInt64>(timestamp.epochMicroseconds());

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
                        else
                        {
                            // repeat the event block as a heartbeat
                            last_event_timestamp_usec = static_cast<UInt64>(timestamp.epochMicroseconds());
                            return { getHeader(), true };
                        }
                    }
                }
            }
            return tryReadImpl(blocking);
        }

        // move right to the end
        it = end;
        end_of_blocks = false;
        num_updates += 1;

        last_event_timestamp_usec = static_cast<UInt64>(timestamp.epochMicroseconds());

        return { getEventBlock(), true };
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
    UInt64 temporary_live_view_timeout_sec;
    UInt64 last_event_timestamp_usec = 0;
    Poco::Timestamp timestamp;
};

}
