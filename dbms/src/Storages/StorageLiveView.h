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
#include <DataStreams/BlocksBlockInputStream.h>
#include <ext/shared_ptr_helper.h>
#include <Common/SipHash.h>
#include <Storages/IStorage.h>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;

class StorageLiveView : public ext::shared_ptr_helper<StorageLiveView>, public IStorage
{
friend class ext::shared_ptr_helper<StorageLiveView>;
friend class LiveBlockOutputStream;

public:
    ~StorageLiveView() override;
    std::string getName() const override { return "LiveView"; }
    std::string getTableName() const override { return table_name; }
    std::string getDatabaseName() const { return database_name; }

    const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }
    ASTPtr getInnerQuery() const { return inner_query->clone(); };

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

    bool checkTableCanBeDropped() const override;
    void drop() override;
    void startup() override;
    void shutdown() override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockInputStreams watch(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams);

    std::shared_ptr<BlocksPtr> getBlocksPtr() { return blocks_ptr; }
    BlocksPtrs getMergeableBlocks() { return mergeable_blocks; }
    std::shared_ptr<bool> getActivePtr() { return active_ptr; }

    /// Read new data blocks that store query result
    bool getNewBlocks();

private:
    String select_database_name;
    String select_table_name;
    String table_name;
    String database_name;
    ASTPtr inner_query;
    Context & global_context;
    NamesAndTypesListPtr columns;
    bool is_temporary {false};

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
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_);
};

class LiveBlockOutputStream : public IBlockOutputStream
{
public:
    explicit LiveBlockOutputStream(StorageLiveView & storage_) : storage(storage_) {}

    void write(const Block & block) override
    {
        if (!new_blocks)
            new_blocks = std::make_shared<Blocks>();

        new_blocks->push_back(block);

        if (block.info.is_start_frame)
            new_hash_key = block.info.hash;

        if (block.info.is_end_frame)
        {
            (*storage.blocks_ptr) = new_blocks;
            storage.hash_key = new_hash_key;
            new_blocks.reset();
            new_hash_key = "";
            storage.condition.broadcast();
        }
    }

private:
    BlocksPtr new_blocks;
    String new_hash_key;
    StorageLiveView & storage;
};

}
