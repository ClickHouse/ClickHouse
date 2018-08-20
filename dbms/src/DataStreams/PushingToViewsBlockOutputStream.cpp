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
#include "PushingToViewsBlockOutputStream.h"
#include <Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_STORAGE;
}

PushingToViewsBlockOutputStream::PushingToViewsBlockOutputStream(String database, String table, const Context & context_,
                                                                 const ASTPtr & query_ptr_, bool no_destination)
    : context(context_), query_ptr(query_ptr_)
{
    storage = context.getTable(database, table);

    /** TODO This is a very important line. At any insertion into the table one of streams should own lock.
      * Although now any insertion into the table is done via PushingToViewsBlockOutputStream,
      *  but it's clear that here is not the best place for this functionality.
      */
    addTableLock(storage->lockStructure(true, __PRETTY_FUNCTION__));

    Dependencies dependencies = context.getDependencies(database, table);

    /// We need special context for materialized views insertions
    if (!dependencies.empty())
    {
        views_context = std::make_unique<Context>(context);
        // Do not deduplicate insertions into MV if the main insertion is Ok
        views_context->getSettingsRef().insert_deduplicate = false;
    }

    for (const auto & database_table : dependencies)
    {

        auto storage = context.getTable(database_table.first, database_table.second);
        auto next = std::make_shared<PushingToViewsBlockOutputStream>(database_table.first, database_table.second, *views_context, ASTPtr());
        views.emplace_back(std::move(storage), std::move(next));

    }

    /* Do not push to destination table if the flag is set */
    if (!no_destination)
    {
        output = storage->write(query_ptr, context.getSettingsRef());
        replicated_output = dynamic_cast<ReplicatedMergeTreeBlockOutputStream *>(output.get());
    }
}


void PushingToViewsBlockOutputStream::write(const Block & block)
{
    if (output)
        output->write(block);

    /// Don't process materialized views if this block is duplicate
    if (replicated_output && replicated_output->lastBlockIsDuplicate())
        return;

    std::vector<std::pair<StoragePtr, BlockOutputStreamPtr>> materialized_views;
    std::vector<std::pair<StoragePtr, BlockOutputStreamPtr>> live_views;
    std::vector<std::pair<StoragePtr, BlockOutputStreamPtr>> live_channels;

    for (auto & view : views)
    {

        if ( dynamic_cast<const StorageMaterializedView *>(view.first.get()) )
            materialized_views.emplace_back(view);
        else if ( dynamic_cast<const StorageLiveView *>(view.first.get()) )
            live_views.emplace_back(view);
        else if (  dynamic_cast<const StorageLiveChannel *>(view.first.get()) )
            live_channels.emplace_back(view);
        else
            throw Exception("Unknown dependent table", ErrorCodes::UNKNOWN_STORAGE);
    }

    /// Insert data into materialized views only after successful insert into main table
    for (auto & view : materialized_views)
    {
        auto & materialized_view = dynamic_cast<const StorageMaterializedView &>(*view.first);
        BlockInputStreamPtr from = std::make_shared<OneBlockInputStream>(block);
        InterpreterSelectQuery select(materialized_view.getInnerQuery(), *views_context, QueryProcessingStage::Complete, 0, from);
        BlockInputStreamPtr data = std::make_shared<MaterializingBlockInputStream>(select.execute().in);
        copyData(*data, *view.second);
    }

    /// Insert data into live views only after successful insert into main table and materialized views
    for (auto & view : live_views)
    {
        StorageLiveView & live_view = dynamic_cast<StorageLiveView &>(*view.first);

        /// Check if live view has any readers if not
        /// just reset blocks to empty and do nothing else
        /// When first reader comes the blocks will be read.
        {
            Poco::FastMutex::ScopedLock lock(live_view.mutex);
            if ( !live_view.hasActiveUsers() )
            {
                live_view.reset();
                continue;
            }
        }

        SipHash hash;
        UInt128 key;
        BlockInputStreams from;
        BlocksPtr blocks = std::make_shared<Blocks>();
        BlocksPtrs mergeable_blocks;
        BlocksPtr new_mergeable_blocks = std::make_shared<Blocks>();

        InterpreterSelectQuery select_block(live_view.getInnerQuery(), *views_context, QueryProcessingStage::WithMergeableState, 0,
             std::make_shared<OneBlockInputStream>(block));
        auto data_mergeable_stream = std::make_shared<MaterializingBlockInputStream>(select_block.execute().in);
        while (Block this_block = data_mergeable_stream->read())
            new_mergeable_blocks->push_back(this_block);

        if (new_mergeable_blocks->empty())
            continue;

        {
            Poco::FastMutex::ScopedLock lock(live_view.mutex);

            mergeable_blocks = live_view.getMergeableBlocks();
            if (!mergeable_blocks || mergeable_blocks->size() >= 64)
            {
                mergeable_blocks = std::make_shared<std::vector<BlocksPtr>>();
                BlocksPtr base_mergeable_blocks = std::make_shared<Blocks>();
                InterpreterSelectQuery interpreter{live_view.getInnerQuery(), *views_context, QueryProcessingStage::WithMergeableState};
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
                for (auto col : sample_new_block.getColumns())
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
                BlockInputStreamPtr stream = std::make_shared<BlocksBlockInputStream>(std::make_shared<BlocksPtr>(blocks));
                from.push_back(std::move(stream));
            }
        }

        InterpreterSelectQuery select(live_view.getInnerQuery(), *views_context, QueryProcessingStage::Complete, 0,
            nullptr, std::move(from), QueryProcessingStage::WithMergeableState);
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
            BlockInputStreamPtr new_data = std::make_shared<BlocksBlockInputStream>(std::make_shared<BlocksPtr>(blocks));
            {
                Poco::FastMutex::ScopedLock lock(live_view.mutex);
                copyData(*new_data, *view.second);
            }
        }
    }

    /// Insert data into live channels only after successful insert into main table, materialized views, and live views
    for (auto & channel : live_channels)
    {
        /// Send only end of frame block to channel to signal that
        /// new data is available
        if ( block.info.is_end_frame == true )
            (*channel.second).write(block);
    }
}

}
