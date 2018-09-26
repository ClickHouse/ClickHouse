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

#include <Core/Field.h>
#include <Core/Heartbeat.h>
#include <Poco/Condition.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/StorageLiveView.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Functions/FunctionHelpers.h>
#include <DataStreams/LiveBlockInputStream.h>
#include <Storages/StorageLiveChannel.h>


namespace DB
{

/**
  */

class LiveChannelBlockInputStream : public IProfilingBlockInputStream
{

using StorageList = std::list<StoragePtr>;
using StorageListPtr = std::shared_ptr<StorageList>;

using StorageListWithLocks = std::list<std::pair<StoragePtr, TableStructureReadLockPtr>>;
using StorageListWithLocksPtr = std::shared_ptr<std::list<std::pair<StoragePtr, TableStructureReadLockPtr>>>;

using StorageMapWithStreams = std::unordered_map<String, std::pair<StoragePtr, std::shared_ptr<LiveBlockInputStream>>>;
using StorageMapWithStreamsPtr = std::shared_ptr<StorageMapWithStreams>;
using NonBlockingResult = std::pair<Block, bool>;

public:
    ~LiveChannelBlockInputStream() override
    {
        /// Start storage no users thread
        /// if we are the last active user
        if ( !storage.is_dropped && version_ptr.use_count() < 3 )
            storage.startNoUsersThread();
    }

    /// length default -2 because we want LIMIT to specify number of updates so that LIMIT 1 waits for 1 update
    /// and LIMIT 0 just returns data without waiting for any updates
    LiveChannelBlockInputStream(StorageLiveChannel & storage_,
        std::shared_ptr<StorageListWithLocksPtr> selected_tables_ptr_,
        std::shared_ptr<StorageListPtr> suspend_tables_ptr_,
        std::shared_ptr<StorageListPtr> resume_tables_ptr_,
        std::shared_ptr<StorageListPtr> refresh_tables_ptr_,
        std::shared_ptr<UInt64> version_ptr_,
        std::shared_ptr<UInt64> storage_list_version_ptr_,
        std::shared_ptr<UInt64> suspend_list_version_ptr_,
        std::shared_ptr<UInt64> resume_list_version_ptr_,
        std::shared_ptr<UInt64> refresh_list_version_ptr_,
        Poco::Condition & condition_, Poco::FastMutex & mutex_, int64_t length_)
        : storage(storage_),
        selected_tables_ptr(selected_tables_ptr_),
        suspend_tables_ptr(suspend_tables_ptr_),
        resume_tables_ptr(resume_tables_ptr_),
        refresh_tables_ptr(refresh_tables_ptr_),
        version_ptr(version_ptr_),
        storage_list_version_ptr(storage_list_version_ptr_),
        suspend_list_version_ptr(suspend_list_version_ptr_),
        resume_list_version_ptr(resume_list_version_ptr_),
        refresh_list_version_ptr(refresh_list_version_ptr_),
        condition(condition_), mutex(mutex_), length(length_ + 1)
    {
        {
            Poco::FastMutex::ScopedLock lock(mutex);
            /// set version so they will not match to force
            /// evaluation of all the tables
            version = (*version_ptr) - 1;
            storage_list_version = (*storage_list_version_ptr) - 1;
            suspend_list_version = (*suspend_list_version_ptr) - 1;
            resume_list_version = (*resume_list_version_ptr) - 1;
            refresh_list_version = (*refresh_list_version_ptr) - 1;

            /// set selected tables and selected streams
            selected_tables = (*selected_tables_ptr);
            suspend_tables = (*suspend_tables_ptr);
            resume_tables = (*resume_tables_ptr);
            refresh_tables = (*refresh_tables_ptr);
        }
        selected_streams = std::make_shared<StorageMapWithStreams>();
        setSelectedStreams();

        it = selected_streams->end();
        end = selected_streams->end();
    }

    String getName() const override { return "LiveChannelBlockInputStream"; }

    String getID() const override
    {
        std::stringstream res;
        res << this;
        return res.str();
    }

    void cancel() override
    {
        if (isCancelled() || storage.is_dropped)
            return;
        IProfilingBlockInputStream::cancel();
        Poco::FastMutex::ScopedLock lock(mutex);
        condition.broadcast();
    }

    void setHeartbeatCallback(const HeartbeatCallback & callback) {
        heartbeat_callback = callback;
    }

    void setHeartbeatDelay(const UInt64 & delay) {
        heartbeat_delay = delay;
    }

    virtual void heartbeat(const Heartbeat & value)
    {
        if (heartbeat_callback)
            heartbeat_callback(value);
    }

protected:
    HeartbeatCallback heartbeat_callback;
    UInt64 heartbeat_delay;

    void setSelectedStreams()
    {
        std::string database_and_table_name;
        auto new_selected_streams = std::make_shared<StorageMapWithStreams>();

        for (auto table : (*selected_tables))
        {
            auto & live_view = dynamic_cast<StorageLiveView &>(*table.first);

            database_and_table_name = live_view.getDatabaseName() + "." + live_view.getTableName();

            auto it = selected_streams->find(database_and_table_name);
            if (it != selected_streams->end())
            {
                new_selected_streams->insert({database_and_table_name, std::move(it->second)});
            }
            else
            {
                std::shared_ptr<BlocksPtr> blocks_ptr = live_view.getBlocksPtr();
                std::shared_ptr<bool> active_ptr = live_view.getActivePtr();

                /// check if live view hasn't set blocks pointer
                {
                    Poco::FastMutex::ScopedLock lock(live_view.mutex);
                    if (!(*blocks_ptr))
                    {
                        live_view.getNewBlocks();
                        live_view.condition.broadcast();
                    }
                    auto stream = std::make_shared<LiveBlockInputStream>(live_view, blocks_ptr, active_ptr, live_view.condition, live_view.mutex, -2);
                    new_selected_streams->insert({database_and_table_name, {table.first, stream}});
                }
            }
        }
        selected_streams = new_selected_streams;
    }

    /**
     *  Read method implementation.
     **/
    Block readImpl() override
    {
        NonBlockingResult res;

        if (length == 0)
            return Block();

        if (isCancelled() || storage.is_dropped)
            return Block();

        if (it == end)
        {
            {
                Poco::FastMutex::ScopedLock lock(mutex);
                /// Check if there have been any updates to the selected tables
                /// by comparing our version to the table's version
                if (storage_list_version != *(storage_list_version_ptr))
                {
                    storage_list_version = (*storage_list_version_ptr);
                    selected_tables = (*selected_tables_ptr);
                    setSelectedStreams();
                    it = selected_streams->begin();
                    end = selected_streams->end();
                }

                if (refresh_list_version != *(refresh_list_version_ptr))
                {
                    refresh_list_version = (*refresh_list_version_ptr);
                    refresh_tables = (*refresh_tables_ptr);
                    std::string database_and_table_name;
                    for (auto table : (*refresh_tables))
                    {
                        auto & live_view = dynamic_cast<StorageLiveView &>(*table);
                        database_and_table_name = live_view.getDatabaseName() + "." + live_view.getTableName();

                        auto _it = selected_streams->find(database_and_table_name);
                        if (_it != selected_streams->end())
                        {
                            Poco::FastMutex::ScopedLock lock(live_view.mutex);
                            _it->second.second->refresh();
                        }
                    }
                    it = selected_streams->begin();
                    end = selected_streams->end();
                }

                if (suspend_list_version != *(suspend_list_version_ptr))
                {
                    suspend_list_version = (*suspend_list_version_ptr);
                    suspend_tables = (*suspend_tables_ptr);

                    std::string database_and_table_name;
                    for (auto table : (*suspend_tables))
                    {
                        auto & live_view = dynamic_cast<StorageLiveView &>(*table);
                        database_and_table_name = live_view.getDatabaseName() + "." + live_view.getTableName();

                        auto _it = selected_streams->find(database_and_table_name);
                        if (_it != selected_streams->end())
                        {
                            Poco::FastMutex::ScopedLock lock(live_view.mutex);
                            _it->second.second->suspend();
                        }
                    }
                }

                if (resume_list_version != *(resume_list_version_ptr))
                {
                    resume_list_version = (*resume_list_version_ptr);
                    resume_tables = (*resume_tables_ptr);

                    std::string database_and_table_name;
                    for (auto table : (*resume_tables))
                    {
                        auto & live_view = dynamic_cast<StorageLiveView &>(*table);
                        database_and_table_name = live_view.getDatabaseName() + "." + live_view.getTableName();

                        auto it = selected_streams->find(database_and_table_name);
                        if (it != selected_streams->end())
                        {
                            /// check if live view hasn't set blocks pointer
                            {
                                Poco::FastMutex::ScopedLock lock(live_view.mutex);
                                if (!(*live_view.getBlocksPtr()))
                                {
                                    live_view.getNewBlocks();
                                    live_view.condition.broadcast();
                                }
                                it->second.second->resume();
                            }
                        }
                    }
                    it = selected_streams->begin();
                    end = selected_streams->end();
                }

                if (version != *(version_ptr))
                {
                    version = (*version_ptr);
                    it = selected_streams->begin();
                    end = selected_streams->end();
                }

                /// No new version so we need to sleep until there is an update
                if (it == end)
                {
                    while(1)
                    {
                        bool signaled = condition.tryWait(mutex, std::max(0, heartbeat_delay - ((UInt64)timestamp.epochMicroseconds() - last_event_timestamp)) / 1000);

                        if (isCancelled() || storage.is_dropped)
                        {
                            return Block();
                        }

                        if (signaled)
                        {
                            break;
                        }
                        else
                        {
                            last_event_timestamp = (UInt64)timestamp.epochMicroseconds();
                            heartbeat(Heartbeat(last_event_timestamp, hashmap));
                        }
                    }
                }
            }
            return readImpl();
        }

        res  = it->second.second->tryRead();

        if ( !res.second )
        {
            ++it;

            if ( length > 0 )
                --length;

            return readImpl();
        }

        if ( res.first.info.is_start_frame )
        {
            res.first.info.table = it->first;
            hashmap[it->first] = res.first.info.hash;
        }

        last_event_timestamp = (UInt64)timestamp.epochMicroseconds();
        return res.first;
    }

private:
    StorageLiveChannel & storage;
    StorageListWithLocksPtr selected_tables;
    std::shared_ptr<StorageListWithLocksPtr> selected_tables_ptr;

    StorageListPtr suspend_tables;
    std::shared_ptr<StorageListPtr> suspend_tables_ptr;
    StorageListPtr resume_tables;
    std::shared_ptr<StorageListPtr> resume_tables_ptr;
    StorageListPtr refresh_tables;
    std::shared_ptr<StorageListPtr> refresh_tables_ptr;

    StorageMapWithStreamsPtr selected_streams;
    std::shared_ptr<UInt64> version_ptr;
    std::shared_ptr<UInt64> storage_list_version_ptr;
    std::shared_ptr<UInt64> suspend_list_version_ptr;
    std::shared_ptr<UInt64> resume_list_version_ptr;
    std::shared_ptr<UInt64> refresh_list_version_ptr;

    UInt64 version;
    UInt64 storage_list_version;
    UInt64 suspend_list_version;
    UInt64 resume_list_version;
    UInt64 refresh_list_version;

    StorageMapWithStreams::iterator it;
    StorageMapWithStreams::iterator end;
    Poco::Condition & condition;
    Poco::FastMutex & mutex;
    /// Length specifies number of updates to send, default -2 (no limit)
    int64_t length;
    HeartbeatHashMap hashmap;
    UInt64 last_event_timestamp{0};
    Poco::Timestamp timestamp;
};

}
