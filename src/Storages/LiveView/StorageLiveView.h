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

struct MergeableBlocks
{
    BlocksPtrs blocks;
    Block sample_block;
};

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using BlocksMetadataPtr = std::shared_ptr<BlocksMetadata>;
using MergeableBlocksPtr = std::shared_ptr<MergeableBlocks>;

class StorageLiveView final : public ext::shared_ptr_helper<StorageLiveView>, public IStorage
{
friend struct ext::shared_ptr_helper<StorageLiveView>;
friend class LiveViewBlockInputStream;
friend class LiveViewEventsBlockInputStream;
friend class LiveViewBlockOutputStream;

public:
    ~StorageLiveView() override;
    String getName() const override { return "LiveView"; }
    bool isView() const override { return true; }
    String getBlocksTableName() const
    {
        return getStorageID().table_name + "_blocks";
    }
    StoragePtr getParentStorage() const { return DatabaseCatalog::instance().getTable(select_table_id, global_context); }

    ASTPtr getInnerQuery() const { return inner_query->clone(); }
    ASTPtr getInnerSubQuery() const
    {
        if (inner_subquery)
            return inner_subquery->clone();
        return nullptr;
    }
    ASTPtr getInnerBlocksQuery();

    /// It is passed inside the query and solved at its level.
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }

    NamesAndTypesList getVirtuals() const override;

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
    void drop() override;
    void startup() override;
    void shutdown() override;

    void refresh(const Context & context);

    Pipes read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
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
    MergeableBlocksPtr getMergeableBlocks() { return mergeable_blocks; }

    /// Collect mergeable blocks and their sample. Must be called holding mutex
    MergeableBlocksPtr collectMergeableBlocks(const Context & context);
    /// Complete query using input streams from mergeable blocks
    BlockInputStreamPtr completeQuery(Pipes pipes);

    void setMergeableBlocks(MergeableBlocksPtr blocks) { mergeable_blocks = blocks; }
    std::shared_ptr<bool> getActivePtr() { return active_ptr; }

    /// Read new data blocks that store query result
    bool getNewBlocks();

    Block getHeader() const;

    /// convert blocks to input streams
    static Pipes blocksToPipes(BlocksPtrs blocks, Block & sample_block);

    static void writeIntoLiveView(
        StorageLiveView & live_view,
        const Block & block,
        const Context & context);

private:
    /// TODO move to common struct SelectQueryDescription
    StorageID select_table_id = StorageID::createEmpty();     /// Will be initialized in constructor
    ASTPtr inner_query; /// stored query : SELECT * FROM ( SELECT a FROM A)
    ASTPtr inner_subquery; /// stored query's innermost subquery if any
    ASTPtr inner_blocks_query; /// query over the mergeable blocks to produce final result
    Context & global_context;
    std::unique_ptr<Context> live_view_context;

    bool is_temporary = false;
    /// Mutex to protect access to sample block and inner_blocks_query
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
    MergeableBlocksPtr mergeable_blocks;

    /// Background thread for temporary tables
    /// which drops this table if there are no users
    static void noUsersThread(std::shared_ptr<StorageLiveView> storage, const UInt64 & timeout);
    std::mutex no_users_thread_mutex;
    std::thread no_users_thread;
    std::atomic<bool> shutdown_called = false;
    std::atomic<bool> start_no_users_thread_called = false;
    UInt64 temporary_live_view_timeout;

    StorageLiveView(
        const StorageID & table_id_,
        Context & local_context,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns
    );
};

}
