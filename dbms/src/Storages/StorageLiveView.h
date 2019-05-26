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
#include <DataStreams/copyData.h>
#include <ext/shared_ptr_helper.h>
#include <Common/SipHash.h>
#include <Storages/IStorage.h>
#include <Storages/ProxyStorage.h>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;

class StorageLiveView : public ext::shared_ptr_helper<StorageLiveView>, public IStorage
{
friend struct ext::shared_ptr_helper<StorageLiveView>;
friend class LiveViewBlockOutputStream;

public:
    ~StorageLiveView() override;
    String getName() const override { return "LiveView"; }
    String getTableName() const override { return table_name; }
    String getDatabaseName() const { return database_name; }
    String getSelectDatabaseName() const { return select_database_name; }
    String getSelectTableName() const { return select_table_name; }

    // const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }
    ASTPtr getInnerQuery() const { return inner_query->clone(); };

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

    /// Check we we have any active readers
    /// must be called with mutex locked
    bool hasActiveUsers()
    {
        return active_ptr.use_count() > 1;
    }
    /// Background thread for temporary tables
    /// which drops this table if there are no users
    void startNoUsersThread();
    Poco::FastMutex noUsersThreadMutex;
    bool noUsersThreadWakeUp{false};
    Poco::Condition noUsersThreadCondition;

    String getBlocksHashKey()
    {
        return hash_key;
    }

    /// Reset blocks
    /// must be called with mutex locked
    void reset()
    {
        (*blocks_ptr).reset();
        mergeable_blocks.reset();
        hash_key = "";
    }

    void checkTableCanBeDropped() const override;
    void drop() override;
    void startup() override;
    void shutdown() override;

    void refresh(const Context & context);

    BlockOutputStreamPtr write(
        const ASTPtr &,
        const Context &) override;

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

        SipHash hash;
        UInt128 key;
        BlockInputStreams from;
        BlocksPtr blocks = std::make_shared<Blocks>();
        BlocksPtrs mergeable_blocks;
        BlocksPtr new_mergeable_blocks = std::make_shared<Blocks>();

        {
            auto parent_storage = context.getTable(live_view.getSelectDatabaseName(), live_view.getSelectTableName());
            BlockInputStreams streams = {std::make_shared<OneBlockInputStream>(block)};
            auto proxy_storage = std::make_shared<ProxyStorage>(parent_storage, std::move(streams), QueryProcessingStage::FetchColumns);
            InterpreterSelectQuery select_block(live_view.getInnerQuery(), context, proxy_storage,
                                                QueryProcessingStage::WithMergeableState);
            auto data_mergeable_stream = std::make_shared<MaterializingBlockInputStream>(select_block.execute().in);
            while (Block this_block = data_mergeable_stream->read())
                new_mergeable_blocks->push_back(this_block);
        }

        if (new_mergeable_blocks->empty())
            return;

        {
            Poco::FastMutex::ScopedLock lock(live_view.mutex);

            mergeable_blocks = live_view.getMergeableBlocks();
            if (!mergeable_blocks || mergeable_blocks->size() >= 64)
            {
                mergeable_blocks = std::make_shared<std::vector<BlocksPtr>>();
                BlocksPtr base_mergeable_blocks = std::make_shared<Blocks>();
                InterpreterSelectQuery interpreter(live_view.getInnerQuery(), context, SelectQueryOptions(QueryProcessingStage::WithMergeableState), Names{});
                auto view_mergeable_stream = std::make_shared<MaterializingBlockInputStream>(interpreter.execute().in);
                while (Block this_block = view_mergeable_stream->read())
                    base_mergeable_blocks->push_back(this_block);
                mergeable_blocks->push_back(base_mergeable_blocks);
            }

            /// Need make new mergeable block structure match the other mergeable blocks
            if (!mergeable_blocks->front()->empty() && !new_mergeable_blocks->empty())
            {
                auto sample_block = mergeable_blocks->front()->front();
                auto sample_new_block = new_mergeable_blocks->front();
                for (auto col : sample_new_block)
                {
                    for (auto & new_block : *new_mergeable_blocks)
                    {
                        if (!sample_block.has(col.name))
                            new_block.erase(col.name);
                    }
                }
            }

            mergeable_blocks->push_back(new_mergeable_blocks);

            /// Create from blocks streams
            for (auto & blocks : *mergeable_blocks)
            {
                auto sample_block = mergeable_blocks->front()->front().cloneEmpty();
                BlockInputStreamPtr stream = std::make_shared<BlocksBlockInputStream>(std::make_shared<BlocksPtr>(blocks), sample_block);
                from.push_back(std::move(stream));
            }
        }

        auto parent_storage = context.getTable(live_view.getSelectDatabaseName(), live_view.getSelectTableName());
        auto proxy_storage = std::make_shared<ProxyStorage>(parent_storage, std::move(from), QueryProcessingStage::WithMergeableState);
        InterpreterSelectQuery select(live_view.getInnerQuery(), context, proxy_storage, QueryProcessingStage::Complete);
        BlockInputStreamPtr data = std::make_shared<MaterializingBlockInputStream>(select.execute().in);
        while (Block this_block = data->read())
        {
            this_block.updateHash(hash);
            blocks->push_back(this_block);
        }
        /// get hash key
        hash.get128(key.low, key.high);
        /// Update blocks only if hash keys do not match
        /// NOTE: hash could be different for the same result
        ///       if blocks are not in the same order
        if (live_view.getBlocksHashKey() != key.toHexString())
        {
            auto sample_block = blocks->front().cloneEmpty();
            BlockInputStreamPtr new_data = std::make_shared<BlocksBlockInputStream>(std::make_shared<BlocksPtr>(blocks), sample_block);
            {
                Poco::FastMutex::ScopedLock lock(live_view.mutex);
                copyData(*new_data, *output);
            }
        }
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
    BlocksPtr new_blocks;
    BlocksPtrs mergeable_blocks;

    /// Current blocks hash key
    String hash_key;
    String new_hash_key;

    void noUsersThread();
    std::thread no_users_thread;
    std::atomic<bool> shutdown_called{false};
    std::atomic<bool> startnousersthread_called{false};

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

    void write(const Block & block) override
    {
        if (!new_blocks)
            new_blocks = std::make_shared<Blocks>();

        new_blocks->push_back(block);
        // FIXME: do I need to calculate block hash?
        (*storage.blocks_ptr) = new_blocks;
        new_blocks.reset();
        storage.condition.broadcast();
    }

    Block getHeader() const override { return storage.getHeader(); }

private:
    BlocksPtr new_blocks;
    String new_hash_key;
    StorageLiveView & storage;
};

}
