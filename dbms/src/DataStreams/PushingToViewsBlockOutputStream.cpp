/* Some modifications Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <common/ThreadPool.h>
#include <Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageLiveView.h>
#include <Storages/StorageLiveChannel.h>
#include <DataStreams/SquashingBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_STORAGE;
}

class ProxyStorage : public IStorage
{
public:
    ProxyStorage(StoragePtr storage, BlockInputStreams streams, QueryProcessingStage::Enum to_stage)
    : storage(std::move(storage)), streams(std::move(streams)), to_stage(to_stage) {}

public:
    std::string getName() const override { return "ProxyStorage(" + storage->getName() + ")"; }
    std::string getTableName() const override { return storage->getTableName(); }

    bool isMultiplexer() const override { return storage->isMultiplexer(); }
    bool isRemote() const override { return storage->isRemote(); }
    bool supportsSampling() const override { return storage->supportsSampling(); }
    bool supportsFinal() const override { return storage->supportsFinal(); }
    bool supportsPrewhere() const override { return storage->supportsPrewhere(); }
    bool supportsReplication() const override { return storage->supportsReplication(); }
    bool supportsDeduplication() const override { return storage->supportsDeduplication(); }

    QueryProcessingStage::Enum getQueryProcessingStage(const Context & /*context*/) const override { return to_stage; }

    BlockInputStreams read(
            const Names & /*column_names*/,
            const SelectQueryInfo & /*query_info*/,
            const Context & /*context*/,
            QueryProcessingStage::Enum /*processed_stage*/,
            size_t /*max_block_size*/,
            unsigned /*num_streams*/) override
    {
        return streams;
    }

    bool supportsIndexForIn() const override { return storage->supportsIndexForIn(); }
    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand) const override { return storage->mayBenefitFromIndexForIn(left_in_operand); }
    ASTPtr getPartitionKeyAST() const override { return storage->getPartitionKeyAST(); }
    ASTPtr getSortingKeyAST() const override { return storage->getSortingKeyAST(); }
    ASTPtr getPrimaryKeyAST() const override { return storage->getPrimaryKeyAST(); }
    ASTPtr getSamplingKeyAST() const override { return storage->getSamplingKeyAST(); }
    Names getColumnsRequiredForPartitionKey() const override { return storage->getColumnsRequiredForPartitionKey(); }
    Names getColumnsRequiredForSortingKey() const override { return storage->getColumnsRequiredForSortingKey(); }
    Names getColumnsRequiredForPrimaryKey() const override { return storage->getColumnsRequiredForPrimaryKey(); }
    Names getColumnsRequiredForSampling() const override { return storage->getColumnsRequiredForSampling(); }
    Names getColumnsRequiredForFinal() const override { return storage->getColumnsRequiredForFinal(); }

    const ColumnsDescription & getColumns() const override { return storage->getColumns(); }
    void setColumns(ColumnsDescription columns_) override { return storage->setColumns(columns_); }
    NameAndTypePair getColumn(const String & column_name) const override { return storage->getColumn(column_name); }
    bool hasColumn(const String & column_name) const override { return storage->hasColumn(column_name); }

private:
    StoragePtr storage;
    BlockInputStreams streams;
    QueryProcessingStage::Enum to_stage;
};

StoragePtr createProxyStorage(StoragePtr storage, BlockInputStreams streams, QueryProcessingStage::Enum to_stage)
{
    return std::make_shared<ProxyStorage>(std::move(storage), std::move(streams), to_stage);
}

PushingToViewsBlockOutputStream::PushingToViewsBlockOutputStream(
    const String & database, const String & table, const StoragePtr & storage_,
    const Context & context_, const ASTPtr & query_ptr_, bool no_destination)
    : storage(storage_), context(context_), query_ptr(query_ptr_)
{
    /** TODO This is a very important line. At any insertion into the table one of streams should own lock.
      * Although now any insertion into the table is done via PushingToViewsBlockOutputStream,
      *  but it's clear that here is not the best place for this functionality.
      */
    addTableLock(storage->lockStructure(true));

    /// If the "root" table deduplactes blocks, there are no need to make deduplication for children
    /// Moreover, deduplication for AggregatingMergeTree children could produce false positives due to low size of inserting blocks
    bool disable_deduplication_for_children = !no_destination && storage->supportsDeduplication();

    if (!table.empty())
    {
        Dependencies dependencies = context.getDependencies(database, table);

        /// We need special context for materialized views insertions
        if (!dependencies.empty())
        {
            views_context = std::make_unique<Context>(context);
            // Do not deduplicate insertions into MV if the main insertion is Ok
            if (disable_deduplication_for_children)
                views_context->getSettingsRef().insert_deduplicate = false;
        }

        for (const auto & database_table : dependencies)
        {
            auto dependent_table = context.getTable(database_table.first, database_table.second);

            ASTPtr query;

            if (auto * materialized_view = dynamic_cast<const StorageMaterializedView *>(dependent_table.get()))
            {
                if (StoragePtr inner_table = materialized_view->tryGetTargetTable())
                    addTableLock(inner_table->lockStructure(true));

                query = materialized_view->getInnerQuery();
            }

            BlockOutputStreamPtr out = std::make_shared<PushingToViewsBlockOutputStream>(
                database_table.first, database_table.second, dependent_table, *views_context, ASTPtr());
            views.emplace_back(ViewInfo{std::move(query), database_table.first, database_table.second, std::move(out)});
        }
    }

    /* Do not push to destination table if the flag is set */
    if (!no_destination)
    {
        output = storage->write(query_ptr, context.getSettingsRef());
        replicated_output = dynamic_cast<ReplicatedMergeTreeBlockOutputStream *>(output.get());
    }
}

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
        if ( !live_view.hasActiveUsers() )
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
            InterpreterSelectQuery interpreter(live_view.getInnerQuery(), context, Names{}, QueryProcessingStage::WithMergeableState);
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
    /// mark last block as end of frame
    if (!blocks->empty())
        blocks->back().info.is_end_frame = true;
    /// Update blocks only if hash keys do not match
    /// NOTE: hash could be different for the same result
    ///       if blocks are not in the same order
    if (live_view.getBlocksHashKey() != key.toHexString())
    {
        if (!blocks->empty())
        {
            blocks->front().info.is_start_frame = true;
            blocks->front().info.hash = key.toHexString();
        }
        auto sample_block = blocks->front().cloneEmpty();
        BlockInputStreamPtr new_data = std::make_shared<BlocksBlockInputStream>(std::make_shared<BlocksPtr>(blocks), sample_block);
        {
            Poco::FastMutex::ScopedLock lock(live_view.mutex);
            copyData(*new_data, *output);
        }
    }
}

void PushingToViewsBlockOutputStream::write(const Block & block)
{
    /** Throw an exception if the sizes of arrays - elements of nested data structures doesn't match.
      * We have to make this assertion before writing to table, because storage engine may assume that they have equal sizes.
      * NOTE It'd better to do this check in serialization of nested structures (in place when this assumption is required),
      * but currently we don't have methods for serialization of nested structures "as a whole".
      */
    Nested::validateArraySizes(block);

    if (auto * live_view = dynamic_cast<StorageLiveView *>(storage.get()))
    {
        writeIntoLiveView(*live_view, block, context, output);
    }
    else
    {
        if (output)
            output->write(block);
    }

    /// Don't process materialized views if this block is duplicate
    if (replicated_output && replicated_output->lastBlockIsDuplicate())
        return;

    /// Insert data into materialized views only after successful insert into main table
    const Settings & settings = context.getSettingsRef();
    if (settings.parallel_view_processing && views.size() > 1)
    {
        // Push to views concurrently if enabled, and more than one view is attached
        ThreadPool pool(std::min(size_t(settings.max_threads), views.size()));
        for (size_t view_num = 0; view_num < views.size(); ++view_num)
        {
            auto thread_group = CurrentThread::getGroup();
            pool.schedule([=]
            {
                setThreadName("PushingToViewsBlockOutputStream");
                if (thread_group)
                    CurrentThread::attachToIfDetached(thread_group);
                process(block, view_num);
            });
        }
        // Wait for concurrent view processing
        pool.wait();
    }
    else
    {
        // Process sequentially
        for (size_t view_num = 0; view_num < views.size(); ++view_num)
            process(block, view_num);
    }
}

void PushingToViewsBlockOutputStream::writePrefix()
{
    if (output)
        output->writePrefix();

    for (auto & view : views)
    {
        try
        {
            view.out->writePrefix();
        }
        catch (Exception & ex)
        {
            ex.addMessage("while write prefix to view " + view.database + "." + view.table);
            throw;
        }
    }
}

void PushingToViewsBlockOutputStream::writeSuffix()
{
    if (output)
        output->writeSuffix();

    for (auto & view : views)
    {
        try
        {
            view.out->writeSuffix();
        }
        catch (Exception & ex)
        {
            ex.addMessage("while write prefix to view " + view.database + "." + view.table);
            throw;
        }
    }
}

void PushingToViewsBlockOutputStream::flush()
{
    if (output)
        output->flush();

    for (auto & view : views)
        view.out->flush();
}


void PushingToViewsBlockOutputStream::process(const Block & block, size_t view_num)
{
    auto & view = views[view_num];

    try
    {
        BlockInputStreamPtr from = std::make_shared<OneBlockInputStream>(block);
        BlockInputStreamPtr in = from;
        if (view.query)
        {
            InterpreterSelectQuery select(view.query, *views_context, from);
            in = std::make_shared<MaterializingBlockInputStream>(select.execute().in);
        }
        /// Squashing is needed here because the materialized view query can generate a lot of blocks
        /// even when only one block is inserted into the parent table (e.g. if the query is a GROUP BY
        /// and two-level aggregation is triggered).
        in = std::make_shared<SquashingBlockInputStream>(
            in, context.getSettingsRef().min_insert_block_size_rows, context.getSettingsRef().min_insert_block_size_bytes);

        in->readPrefix();

        while (Block result_block = in->read())
        {
            Nested::validateArraySizes(result_block);
            view.out->write(result_block);
        }

        in->readSuffix();
    }
    catch (Exception & ex)
    {
        ex.addMessage("while pushing to view " + backQuoteIfNeed(view.database) + "." + backQuoteIfNeed(view.table));
        throw;
    }
}

}
