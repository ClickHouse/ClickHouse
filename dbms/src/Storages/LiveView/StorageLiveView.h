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

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>

#include <mutex>
#include <condition_variable>


namespace DB
{

struct BlocksMetadata
{
    String hash;
    UInt64 version;
};

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using BlocksMetadataPtr = std::shared_ptr<BlocksMetadata>;

class StorageLiveView : public ext::shared_ptr_helper<StorageLiveView>, public IStorage
{
friend struct ext::shared_ptr_helper<StorageLiveView>;
friend class LiveViewBlockInputStream;
friend class LiveViewEventsBlockInputStream;
friend class LiveViewBlockOutputStream;

public:
    ~StorageLiveView() override;
    String getName() const override { return "LiveView"; }
    String getTableName() const override { return table_name; }
    String getDatabaseName() const override { return database_name; }
    String getSelectDatabaseName() const { return select_database_name; }
    String getSelectTableName() const { return select_table_name; }

    NameAndTypePair getColumn(const String & column_name) const override;
    bool hasColumn(const String & column_name) const override;

    // const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }
    ASTPtr getInnerQuery() const { return inner_query->clone(); }

    /// It is passed inside the query and solved at its level.
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }

    bool isTemporary() { return is_temporary; }

    /// Check if we have any readers
    /// must be called with mutex locked
    bool hasUsers()
    {
        return blocks_ptr.use_count() > 1;
    }

    /// Check we have any active readers
    /// must be called with mutex locked
    bool hasActiveUsers()
    {
        return active_ptr.use_count() > 1;
    }
    /// No users thread mutex, predicate and wake up condition
    void startNoUsersThread(const UInt64 & timeout);
    std::mutex no_users_thread_wakeup_mutex;
    bool no_users_thread_wakeup = false;
    std::condition_variable no_users_thread_condition;
    /// Get blocks hash
    /// must be called with mutex locked
    String getBlocksHashKey()
    {
        if (*blocks_metadata_ptr)
            return (*blocks_metadata_ptr)->hash;
        return {};
    }
    /// Get blocks version
    /// must be called with mutex locked
    UInt64 getBlocksVersion()
    {
        if (*blocks_metadata_ptr)
            return (*blocks_metadata_ptr)->version;
        return 0;
    }

    /// Reset blocks
    /// must be called with mutex locked
    void reset()
    {
        (*blocks_ptr).reset();
        if (*blocks_metadata_ptr)
            (*blocks_metadata_ptr)->hash.clear();
        mergeable_blocks.reset();
    }

    void checkTableCanBeDropped() const override;
    void drop(TableStructureWriteLockHolder &) override;
    void startup() override;
    void shutdown() override;

    void refresh(const Context & context);

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockInputStreams watch(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    std::shared_ptr<BlocksPtr> getBlocksPtr() { return blocks_ptr; }
    BlocksPtrs getMergeableBlocks() { return mergeable_blocks; }
    void setMergeableBlocks(BlocksPtrs blocks) { mergeable_blocks = blocks; }
    std::shared_ptr<bool> getActivePtr() { return active_ptr; }

    /// Read new data blocks that store query result
    bool getNewBlocks();

    Block getHeader() const;

    static void writeIntoLiveView(
        StorageLiveView & live_view,
        const Block & block,
        const Context & context);

private:
    String select_database_name;
    String select_table_name;
    String table_name;
    String database_name;
    ASTPtr inner_query;
    Context & global_context;
    bool is_temporary = false;
    /// Mutex to protect access to sample block
    mutable std::mutex sample_block_lock;
    mutable Block sample_block;

    /// Mutex for the blocks and ready condition
    std::mutex mutex;
    /// New blocks ready condition to broadcast to readers
    /// that new blocks are available
    std::condition_variable condition;

    /// Active users
    std::shared_ptr<bool> active_ptr;
    /// Current data blocks that store query result
    std::shared_ptr<BlocksPtr> blocks_ptr;
    /// Current data blocks metadata
    std::shared_ptr<BlocksMetadataPtr> blocks_metadata_ptr;
    BlocksPtrs mergeable_blocks;

    /// Background thread for temporary tables
    /// which drops this table if there are no users
    static void noUsersThread(std::shared_ptr<StorageLiveView> storage, const UInt64 & timeout);
    std::mutex no_users_thread_mutex;
    std::thread no_users_thread;
    std::atomic<bool> shutdown_called = false;
    std::atomic<bool> start_no_users_thread_called = false;
    UInt64 temporary_live_view_timeout;

    StorageLiveView(
        const String & table_name_,
        const String & database_name_,
        Context & local_context,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns
    );
};

}
