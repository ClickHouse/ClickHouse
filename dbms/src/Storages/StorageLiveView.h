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
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/BlocksBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypesNumber.h>
#include <ext/shared_ptr_helper.h>
#include <Common/SipHash.h>
#include <Storages/IStorage.h>
#include <Storages/ProxyStorage.h>

namespace DB
{

class IAST;

struct BlocksMetadata
{
    String hash;
    UInt64 version;
};

using ASTPtr = std::shared_ptr<IAST>;
using BlocksMetadataPtr = std::shared_ptr<BlocksMetadata>;
using SipHashPtr = std::shared_ptr<SipHash>;

class StorageLiveView : public ext::shared_ptr_helper<StorageLiveView>, public IStorage
{
friend struct ext::shared_ptr_helper<StorageLiveView>;
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

    /// Mutex for the blocks and ready condition
    Poco::FastMutex mutex;
    /// New blocks ready condition to broadcast to readers
    /// that new blocks are available
    Poco::Condition condition;

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
    /// Background thread for temporary tables
    /// which drops this table if there are no users
    void startNoUsersThread(const UInt64 & timeout);
    Poco::FastMutex noUsersThreadMutex;
    bool noUsersThreadWakeUp{false};
    Poco::Condition noUsersThreadCondition;
    /// Get blocks hash
    /// must be called with mutex locked
    String getBlocksHashKey()
    {
        if (*blocks_metadata_ptr)
            return (*blocks_metadata_ptr)->hash;
        return "";
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

    static void writeIntoLiveView(StorageLiveView & live_view,
                              const Block & block,
                              const Context & context,
                              BlockOutputStreamPtr & output)
    {
        /// Check if live view has any readers if not
        /// just reset blocks to empty and do nothing else
        /// When first reader comes the blocks will be read.
        {
            Poco::FastMutex::ScopedLock lock(live_view.mutex);
            if (!live_view.hasActiveUsers())
            {
                live_view.reset();
                return;
            }
        }

        bool is_block_processed = false;
        BlockInputStreams from;
        BlocksPtrs mergeable_blocks;
        BlocksPtr new_mergeable_blocks = std::make_shared<Blocks>();

        {
            Poco::FastMutex::ScopedLock lock(live_view.mutex);

            mergeable_blocks = live_view.getMergeableBlocks();
            if (!mergeable_blocks || mergeable_blocks->size() >= context.getGlobalContext().getSettingsRef().max_live_view_insert_blocks_before_refresh)
            {
                mergeable_blocks = std::make_shared<std::vector<BlocksPtr>>();
                BlocksPtr base_mergeable_blocks = std::make_shared<Blocks>();
                InterpreterSelectQuery interpreter(live_view.getInnerQuery(), context, SelectQueryOptions(QueryProcessingStage::WithMergeableState), Names());
                auto view_mergeable_stream = std::make_shared<MaterializingBlockInputStream>(
                    interpreter.execute().in);
                while (Block this_block = view_mergeable_stream->read())
                    base_mergeable_blocks->push_back(this_block);
                mergeable_blocks->push_back(base_mergeable_blocks);
                live_view.setMergeableBlocks(mergeable_blocks);

                /// Create from streams
                for (auto & blocks_ : *mergeable_blocks)
                {
                    if (blocks_->empty())
                        continue;
                    auto sample_block = blocks_->front().cloneEmpty();
                    BlockInputStreamPtr stream = std::make_shared<BlocksBlockInputStream>(std::make_shared<BlocksPtr>(blocks_), sample_block);
                    from.push_back(std::move(stream));
                }

                is_block_processed = true;
            }
        }

        if (!is_block_processed)
        {
            auto parent_storage = context.getTable(live_view.getSelectDatabaseName(), live_view.getSelectTableName());
            BlockInputStreams streams = {std::make_shared<OneBlockInputStream>(block)};
            auto proxy_storage = std::make_shared<ProxyStorage>(parent_storage, std::move(streams), QueryProcessingStage::FetchColumns);
            InterpreterSelectQuery select_block(live_view.getInnerQuery(),
                context, proxy_storage,
                QueryProcessingStage::WithMergeableState);
            auto data_mergeable_stream = std::make_shared<MaterializingBlockInputStream>(
                select_block.execute().in);
            while (Block this_block = data_mergeable_stream->read())
                new_mergeable_blocks->push_back(this_block);

            if (new_mergeable_blocks->empty())
                return;

            {
                Poco::FastMutex::ScopedLock lock(live_view.mutex);

                mergeable_blocks = live_view.getMergeableBlocks();
                mergeable_blocks->push_back(new_mergeable_blocks);

                /// Create from streams
                for (auto & blocks_ : *mergeable_blocks)
                {
                    if (blocks_->empty())
                        continue;
                    auto sample_block = blocks_->front().cloneEmpty();
                    BlockInputStreamPtr stream = std::make_shared<BlocksBlockInputStream>(std::make_shared<BlocksPtr>(blocks_), sample_block);
                    from.push_back(std::move(stream));
                }
            }
        }

        auto parent_storage = context.getTable(live_view.getSelectDatabaseName(), live_view.getSelectTableName());
        auto proxy_storage = std::make_shared<ProxyStorage>(parent_storage, std::move(from), QueryProcessingStage::WithMergeableState);
        InterpreterSelectQuery select(live_view.getInnerQuery(), context, proxy_storage, QueryProcessingStage::Complete);
        BlockInputStreamPtr data = std::make_shared<MaterializingBlockInputStream>(select.execute().in);

        /// Squashing is needed here because the view query can generate a lot of blocks
        /// even when only one block is inserted into the parent table (e.g. if the query is a GROUP BY
        /// and two-level aggregation is triggered).
        data = std::make_shared<SquashingBlockInputStream>(
            data, context.getGlobalContext().getSettingsRef().min_insert_block_size_rows, context.getGlobalContext().getSettingsRef().min_insert_block_size_bytes);

        copyData(*data, *output);
    }

private:
    String select_database_name;
    String select_table_name;
    String table_name;
    String database_name;
    ASTPtr inner_query;
    Context & global_context;
    bool is_temporary {false};
    mutable Block sample_block;

    /// Active users
    std::shared_ptr<bool> active_ptr;
    /// Current data blocks that store query result
    std::shared_ptr<BlocksPtr> blocks_ptr;
    /// Current data blocks metadata
    std::shared_ptr<BlocksMetadataPtr> blocks_metadata_ptr;
    BlocksPtrs mergeable_blocks;

    void noUsersThread(const UInt64 & timeout);
    std::thread no_users_thread;
    std::atomic<bool> shutdown_called{false};
    std::atomic<bool> startnousersthread_called{false};
    UInt64 temporary_live_view_timeout;

    StorageLiveView(
        const String & table_name_,
        const String & database_name_,
        Context & local_context,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns
    );
};

class LiveViewBlockOutputStream : public IBlockOutputStream
{
public:
    explicit LiveViewBlockOutputStream(StorageLiveView & storage_) : storage(storage_) {}

    void writePrefix() override
    {
        new_blocks = std::make_shared<Blocks>();
        new_blocks_metadata = std::make_shared<BlocksMetadata>();
        new_hash = std::make_shared<SipHash>();
    }

    void writeSuffix() override
    {
        UInt128 key;
        String key_str;

        new_hash->get128(key.low, key.high);
        key_str = key.toHexString();

        Poco::FastMutex::ScopedLock lock(storage.mutex);

        if (storage.getBlocksHashKey() != key_str)
        {
            new_blocks_metadata->hash = key_str;
            new_blocks_metadata->version = storage.getBlocksVersion() + 1;

            for (auto & block : *new_blocks)
            {
                block.insert({DataTypeUInt64().createColumnConst(
                    block.rows(), new_blocks_metadata->version)->convertToFullColumnIfConst(),
                    std::make_shared<DataTypeUInt64>(),
                    "_version"});
            }

            (*storage.blocks_ptr) = new_blocks;
            (*storage.blocks_metadata_ptr) = new_blocks_metadata;

            storage.condition.broadcast();
        }

        new_blocks.reset();
        new_blocks_metadata.reset();
        new_hash.reset();
    }

    void write(const Block & block) override
    {
        new_blocks->push_back(block);
        block.updateHash(*new_hash);
    }

    Block getHeader() const override { return storage.getHeader(); }

private:
    BlocksPtr new_blocks;
    BlocksMetadataPtr new_blocks_metadata;
    SipHashPtr new_hash;
    StorageLiveView & storage;
};

}
