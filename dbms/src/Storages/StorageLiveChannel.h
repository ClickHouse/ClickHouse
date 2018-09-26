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
#include <Interpreters/InterpreterSelectQuery.h>
#include <DataStreams/ChannelBlocksBlockInputStream.h>
#include <ext/shared_ptr_helper.h>
#include <Common/SipHash.h>
#include <Storages/IStorage.h>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using LiveChannelTables = std::set<std::pair<String, String>>;

class StorageLiveChannel : public ext::shared_ptr_helper<StorageLiveChannel>, public IStorage
{
friend class ext::shared_ptr_helper<StorageLiveChannel>;
friend class LiveChannelBlockOutputStream;

public:
    ~StorageLiveChannel() override;
    std::string getName() const override { return "LiveChannel"; }
    std::string getTableName() const override { return table_name; }
    std::string getDatabaseName() const { return database_name; }
    const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }

    /// It is passed inside the query and solved at its level.
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsParallelReplicas() const override { return true; }

    /// Mutex for the blocks and ready condition
    Poco::FastMutex mutex;
    /// New blocks ready condition to broadcast to readers
    /// that new blocks are available
    Poco::Condition condition;

    bool isTemporary() { return is_temporary; }

    /// Check if we have any readers
    bool hasUsers()
    {
       return version.use_count() > 1;
    }

    /// Background thread for temporary tables
    /// which drops this table if there are no users
    void startNoUsersThread();
    Poco::FastMutex noUsersThreadMutex;
    bool noUsersThreadWakeUp{false};
    Poco::Condition noUsersThreadCondition;

    bool isMultiplexer() const override { return true; }
    void drop() override;
    void startup() override;
    void shutdown() override;

    /// Run the ADD request. That is, add live views to channel
    void addToChannel(const ASTPtr & values, const Context & context);

    /// Run the DROP request. That is, drop live views from channel
    void dropFromChannel(const ASTPtr & values, const Context & context);

    /// Run the SUSPEND request. That is, suspend live views in channel
    void suspendInChannel(const ASTPtr & values, const Context & context);

    /// Run the RESUME request. That is, resume live views in channel
    void resumeInChannel(const ASTPtr & values, const Context & context);

    /// Run the REFRESH request. That is, refresh live views in channel
    void refreshInChannel(const ASTPtr & values, const Context & context);

    /// Run the MODIFY request. That is, modify which live views belong to channel
    void modifyChannel(const ASTPtr & values, const Context & context);

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    BlockInputStreams watch(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams);

private:
    String table_name;
    String database_name;
    Context & global_context;
    NamesAndTypesListPtr columns;
    LiveChannelTables tables;
    bool is_temporary {false};

    /// Blocks version. Any update must be
    /// done while holding mutex
    std::shared_ptr<UInt64> version;

    /// Storage list version. Any update must be
    /// done while holding mutex
    std::shared_ptr<UInt64> storage_list_version;
    std::shared_ptr<UInt64> refresh_list_version;
    std::shared_ptr<UInt64> suspend_list_version;
    std::shared_ptr<UInt64> resume_list_version;

    void noUsersThread();
    std::thread no_users_thread;
    std::atomic<bool> shutdown_called {false};
    std::atomic<bool> startnousersthread_called{false};

    StorageLiveChannel(
        const String & table_name_,
        const String & database_name_,
        Context & local_context,
        const ASTCreateQuery & query,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_);

    using StorageList = std::list<StoragePtr>;
    using StorageListPtr = std::shared_ptr<StorageList>;
    using StorageListWithLocks = std::list<std::pair<StoragePtr, TableStructureReadLockPtr>>;
    using StorageListWithLocksPtr = std::shared_ptr<StorageListWithLocks>;

    void setSelectedTables();

    std::shared_ptr<StorageListWithLocksPtr> selected_tables_ptr;
    std::shared_ptr<StorageListPtr> refresh_tables_ptr;
    std::shared_ptr<StorageListPtr> suspend_tables_ptr;
    std::shared_ptr<StorageListPtr> resume_tables_ptr;
};

class LiveChannelBlockOutputStream : public IBlockOutputStream
{
public:
    explicit LiveChannelBlockOutputStream(StorageLiveChannel & storage_) : storage(storage_) {}

    void write(const Block & block) override
    {
        Poco::FastMutex::ScopedLock lock(storage.mutex);
        ++(*storage.version);
        /// notify all readers
        storage.condition.broadcast();
    }
private:
    StorageLiveChannel & storage;
};

}
