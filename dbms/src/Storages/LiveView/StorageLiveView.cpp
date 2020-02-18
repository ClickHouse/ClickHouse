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
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/BlocksBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Common/typeid_cast.h>
#include <Common/SipHash.h>

#include <Storages/LiveView/StorageLiveView.h>
#include <Storages/LiveView/LiveViewBlockInputStream.h>
#include <Storages/LiveView/LiveViewBlockOutputStream.h>
#include <Storages/LiveView/LiveViewEventsBlockInputStream.h>
#include <Storages/LiveView/StorageBlocks.h>

#include <Storages/StorageFactory.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/queryToString.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Access/AccessFlags.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int QUERY_IS_NOT_SUPPORTED_IN_LIVE_VIEW;
    extern const int SUPPORT_IS_DISABLED;
}


static StorageID extractDependentTable(ASTPtr & query, Context & context, const String & table_name, ASTPtr & inner_subquery)
{
    ASTSelectQuery & select_query = typeid_cast<ASTSelectQuery &>(*query);

    if (auto db_and_table = getDatabaseAndTable(select_query, 0))
    {
        String select_database_name = context.getCurrentDatabase();
        String select_table_name = db_and_table->table;

        if (db_and_table->database.empty())
        {
            db_and_table->database = select_database_name;
            AddDefaultDatabaseVisitor visitor(select_database_name);
            visitor.visit(select_query);
        }
        else
            select_database_name = db_and_table->database;

        select_query.replaceDatabaseAndTable("", table_name + "_blocks");
        return StorageID(select_database_name, select_table_name);
    }
    else if (auto subquery = extractTableExpression(select_query, 0))
    {
        auto * ast_select = subquery->as<ASTSelectWithUnionQuery>();
        if (!ast_select)
            throw Exception("Logical error while creating StorageLiveView."
                            " Could not retrieve table name from select query.",
                            DB::ErrorCodes::LOGICAL_ERROR);
        if (ast_select->list_of_selects->children.size() != 1)
            throw Exception("UNION is not supported for LIVE VIEW", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_LIVE_VIEW);

        inner_subquery = ast_select->list_of_selects->children.at(0)->clone();

        return extractDependentTable(ast_select->list_of_selects->children.at(0), context, table_name, inner_subquery);
    }
    else
    {
        /// If the table is not specified - use the table `system.one`
        return StorageID("system", "one");
    }
}

MergeableBlocksPtr StorageLiveView::collectMergeableBlocks(const Context & context)
{
    ASTPtr mergeable_query = inner_query;

    if (inner_subquery)
        mergeable_query = inner_subquery;

    MergeableBlocksPtr new_mergeable_blocks = std::make_shared<MergeableBlocks>();
    BlocksPtrs new_blocks = std::make_shared<std::vector<BlocksPtr>>();
    BlocksPtr base_blocks = std::make_shared<Blocks>();

    InterpreterSelectQuery interpreter(mergeable_query->clone(), context, SelectQueryOptions(QueryProcessingStage::WithMergeableState), Names());

    auto view_mergeable_stream = std::make_shared<MaterializingBlockInputStream>(interpreter.execute().in);

    while (Block this_block = view_mergeable_stream->read())
        base_blocks->push_back(this_block);

    new_blocks->push_back(base_blocks);

    new_mergeable_blocks->blocks = new_blocks;
    new_mergeable_blocks->sample_block = view_mergeable_stream->getHeader();

    return new_mergeable_blocks;
}

BlockInputStreams StorageLiveView::blocksToInputStreams(BlocksPtrs blocks, Block & sample_block)
{
    BlockInputStreams streams;
    for (auto & blocks_ : *blocks)
    {
        BlockInputStreamPtr stream = std::make_shared<BlocksBlockInputStream>(std::make_shared<BlocksPtr>(blocks_), sample_block);
        streams.push_back(std::move(stream));
    }
    return streams;
}

/// Complete query using input streams from mergeable blocks
BlockInputStreamPtr StorageLiveView::completeQuery(BlockInputStreams from)
{
    auto block_context = std::make_unique<Context>(global_context);
    block_context->makeQueryContext();

    auto blocks_storage_id = getBlocksStorageID();
    auto blocks_storage = StorageBlocks::createStorage(blocks_storage_id, getParentStorage()->getColumns(),
        std::move(from), QueryProcessingStage::WithMergeableState);

    block_context->addExternalTable(blocks_storage_id.table_name, blocks_storage);

    InterpreterSelectQuery select(getInnerBlocksQuery(), *block_context, StoragePtr(), SelectQueryOptions(QueryProcessingStage::Complete));
    BlockInputStreamPtr data = std::make_shared<MaterializingBlockInputStream>(select.execute().in);

    /// Squashing is needed here because the view query can generate a lot of blocks
    /// even when only one block is inserted into the parent table (e.g. if the query is a GROUP BY
    /// and two-level aggregation is triggered).
    data = std::make_shared<SquashingBlockInputStream>(
        data, global_context.getSettingsRef().min_insert_block_size_rows,
        global_context.getSettingsRef().min_insert_block_size_bytes);

    return data;
}

void StorageLiveView::writeIntoLiveView(
    StorageLiveView & live_view,
    const Block & block,
    const Context & context)
{
    BlockOutputStreamPtr output = std::make_shared<LiveViewBlockOutputStream>(live_view);

    /// Check if live view has any readers if not
    /// just reset blocks to empty and do nothing else
    /// When first reader comes the blocks will be read.
    {
        std::lock_guard lock(live_view.mutex);
        if (!live_view.hasActiveUsers())
        {
            live_view.reset();
            return;
        }
    }

    bool is_block_processed = false;
    BlockInputStreams from;
    MergeableBlocksPtr mergeable_blocks;
    BlocksPtr new_mergeable_blocks = std::make_shared<Blocks>();

    {
        std::lock_guard lock(live_view.mutex);

        mergeable_blocks = live_view.getMergeableBlocks();
        if (!mergeable_blocks || mergeable_blocks->blocks->size() >= context.getGlobalContext().getSettingsRef().max_live_view_insert_blocks_before_refresh)
        {
            mergeable_blocks = live_view.collectMergeableBlocks(context);
            live_view.setMergeableBlocks(mergeable_blocks);
            from = live_view.blocksToInputStreams(mergeable_blocks->blocks, mergeable_blocks->sample_block);
            is_block_processed = true;
        }
    }

    auto blocks_storage_id = live_view.getBlocksStorageID();

    if (!is_block_processed)
    {
        ASTPtr mergeable_query = live_view.getInnerQuery();

        if (live_view.getInnerSubQuery())
            mergeable_query = live_view.getInnerSubQuery();

        BlockInputStreams streams = {std::make_shared<OneBlockInputStream>(block)};

        auto blocks_storage = StorageBlocks::createStorage(blocks_storage_id,
            live_view.getParentStorage()->getColumns(), std::move(streams), QueryProcessingStage::FetchColumns);

        InterpreterSelectQuery select_block(mergeable_query, context, blocks_storage,
            QueryProcessingStage::WithMergeableState);

        auto data_mergeable_stream = std::make_shared<MaterializingBlockInputStream>(
            select_block.execute().in);

        while (Block this_block = data_mergeable_stream->read())
            new_mergeable_blocks->push_back(this_block);

        if (new_mergeable_blocks->empty())
            return;

        {
            std::lock_guard lock(live_view.mutex);

            mergeable_blocks = live_view.getMergeableBlocks();
            mergeable_blocks->blocks->push_back(new_mergeable_blocks);
            from = live_view.blocksToInputStreams(mergeable_blocks->blocks, mergeable_blocks->sample_block);
        }
    }

    BlockInputStreamPtr data = live_view.completeQuery(from);
    copyData(*data, *output);
}


StorageLiveView::StorageLiveView(
    const StorageID & table_id_,
    Context & local_context,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_)
    : IStorage(table_id_), global_context(local_context.getGlobalContext())
{
    live_view_context = std::make_unique<Context>(global_context);
    live_view_context->makeQueryContext();

    setColumns(columns_);

    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    /// Default value, if only table name exist in the query
    if (query.select->list_of_selects->children.size() != 1)
        throw Exception("UNION is not supported for LIVE VIEW", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_LIVE_VIEW);

    inner_query = query.select->list_of_selects->children.at(0);

    auto inner_query_tmp = inner_query->clone();
    select_table_id = extractDependentTable(inner_query_tmp, global_context, table_id_.table_name, inner_subquery);

    global_context.addDependency(select_table_id, table_id_);

    if (query.live_view_timeout)
    {
        is_temporary = true;
        temporary_live_view_timeout = *query.live_view_timeout;
    }

    blocks_ptr = std::make_shared<BlocksPtr>();
    blocks_metadata_ptr = std::make_shared<BlocksMetadataPtr>();
    active_ptr = std::make_shared<bool>(true);
}

NameAndTypePair StorageLiveView::getColumn(const String & column_name) const
{
    if (column_name == "_version")
        return NameAndTypePair("_version", std::make_shared<DataTypeUInt64>());

    return IStorage::getColumn(column_name);
}

bool StorageLiveView::hasColumn(const String & column_name) const
{
    if (column_name == "_version")
        return true;

    return IStorage::hasColumn(column_name);
}

Block StorageLiveView::getHeader() const
{
    std::lock_guard lock(sample_block_lock);

    if (!sample_block)
    {
        sample_block = InterpreterSelectQuery(inner_query->clone(), *live_view_context, SelectQueryOptions(QueryProcessingStage::Complete)).getSampleBlock();
        sample_block.insert({DataTypeUInt64().createColumnConst(
            sample_block.rows(), 0)->convertToFullColumnIfConst(),
            std::make_shared<DataTypeUInt64>(),
            "_version"});
        /// convert all columns to full columns
        /// in case some of them are constant
        for (size_t i = 0; i < sample_block.columns(); ++i)
        {
            sample_block.safeGetByPosition(i).column = sample_block.safeGetByPosition(i).column->convertToFullColumnIfConst();
        }
    }
    return sample_block;
}

ASTPtr StorageLiveView::getInnerBlocksQuery()
{
    std::lock_guard lock(sample_block_lock);
    if (!inner_blocks_query)
    {
        inner_blocks_query = inner_query->clone();
        /// Rewrite inner query with right aliases for JOIN.
        /// It cannot be done in constructor or startup() because InterpreterSelectQuery may access table,
        /// which is not loaded yet during server startup, so we do it lazily
        InterpreterSelectQuery(inner_blocks_query, *live_view_context, SelectQueryOptions().modify().analyze());
        auto table_id = getStorageID();
        extractDependentTable(inner_blocks_query, global_context, table_id.table_name, inner_subquery);
    }
    return inner_blocks_query->clone();
}

bool StorageLiveView::getNewBlocks()
{
    SipHash hash;
    UInt128 key;
    BlocksPtr new_blocks = std::make_shared<Blocks>();
    BlocksMetadataPtr new_blocks_metadata = std::make_shared<BlocksMetadata>();

    mergeable_blocks = collectMergeableBlocks(*live_view_context);
    BlockInputStreams from = blocksToInputStreams(mergeable_blocks->blocks, mergeable_blocks->sample_block);
    BlockInputStreamPtr data = completeQuery({from});

    while (Block block = data->read())
    {
        /// calculate hash before virtual column is added
        block.updateHash(hash);
        /// add result version meta column
        block.insert({DataTypeUInt64().createColumnConst(
            block.rows(), getBlocksVersion() + 1)->convertToFullColumnIfConst(),
            std::make_shared<DataTypeUInt64>(),
            "_version"});
        new_blocks->push_back(block);
    }

    hash.get128(key.low, key.high);

    /// Update blocks only if hash keys do not match
    /// NOTE: hash could be different for the same result
    ///       if blocks are not in the same order
    bool updated = false;
    {
        if (getBlocksHashKey() != key.toHexString())
        {
            if (new_blocks->empty())
            {
                new_blocks->push_back(getHeader());
            }
            new_blocks_metadata->hash = key.toHexString();
            new_blocks_metadata->version = getBlocksVersion() + 1;
            (*blocks_ptr) = new_blocks;
            (*blocks_metadata_ptr) = new_blocks_metadata;
            updated = true;
        }
    }
    return updated;
}

void StorageLiveView::checkTableCanBeDropped() const
{
    auto table_id = getStorageID();
    Dependencies dependencies = global_context.getDependencies(table_id);
    if (!dependencies.empty())
    {
        StorageID dependent_table_id = dependencies.front();
        throw Exception("Table has dependency " + dependent_table_id.getNameForLogs(), ErrorCodes::TABLE_WAS_NOT_DROPPED);
    }
}

void StorageLiveView::noUsersThread(std::shared_ptr<StorageLiveView> storage, const UInt64 & timeout)
{
    bool drop_table = false;

    if (storage->shutdown_called)
        return;

    auto table_id = storage->getStorageID();
    {
        while (1)
        {
            std::unique_lock lock(storage->no_users_thread_wakeup_mutex);
            if (!storage->no_users_thread_condition.wait_for(lock, std::chrono::seconds(timeout), [&] { return storage->no_users_thread_wakeup; }))
            {
                storage->no_users_thread_wakeup = false;
                if (storage->shutdown_called)
                    return;
                if (storage->hasUsers())
                    return;
                if (!storage->global_context.getDependencies(table_id).empty())
                    continue;
                drop_table = true;
            }
            break;
        }
    }

    if (drop_table)
    {
        if (storage->global_context.tryGetTable(table_id))
        {
            try
            {
                /// We create and execute `drop` query for this table
                auto drop_query = std::make_shared<ASTDropQuery>();
                drop_query->database = table_id.database_name;
                drop_query->table = table_id.table_name;
                drop_query->kind = ASTDropQuery::Kind::Drop;
                ASTPtr ast_drop_query = drop_query;
                InterpreterDropQuery drop_interpreter(ast_drop_query, storage->global_context);
                drop_interpreter.execute();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
}

void StorageLiveView::startNoUsersThread(const UInt64 & timeout)
{
    bool expected = false;
    if (!start_no_users_thread_called.compare_exchange_strong(expected, true))
        return;

    if (is_temporary)
    {
        std::lock_guard no_users_thread_lock(no_users_thread_mutex);

        if (shutdown_called)
            return;

        if (no_users_thread.joinable())
        {
            {
                std::lock_guard lock(no_users_thread_wakeup_mutex);
                no_users_thread_wakeup = true;
                no_users_thread_condition.notify_one();
            }
            no_users_thread.join();
        }
        {
            std::lock_guard lock(no_users_thread_wakeup_mutex);
            no_users_thread_wakeup = false;
        }
        if (!is_dropped)
            no_users_thread = std::thread(&StorageLiveView::noUsersThread,
                std::static_pointer_cast<StorageLiveView>(shared_from_this()), timeout);
    }

    start_no_users_thread_called = false;
}

void StorageLiveView::startup()
{
    startNoUsersThread(temporary_live_view_timeout);
}

void StorageLiveView::shutdown()
{
    global_context.removeDependency(select_table_id, getStorageID());
    bool expected = false;
    if (!shutdown_called.compare_exchange_strong(expected, true))
        return;

    {
        std::lock_guard no_users_thread_lock(no_users_thread_mutex);
        if (no_users_thread.joinable())
        {
            {
                std::lock_guard lock(no_users_thread_wakeup_mutex);
                no_users_thread_wakeup = true;
                no_users_thread_condition.notify_one();
            }
        }
    }
}

StorageLiveView::~StorageLiveView()
{
    shutdown();

    {
        std::lock_guard lock(no_users_thread_mutex);
        if (no_users_thread.joinable())
            no_users_thread.detach();
    }
}

void StorageLiveView::drop(TableStructureWriteLockHolder &)
{
    auto table_id = getStorageID();
    global_context.removeDependency(select_table_id, table_id);

    std::lock_guard lock(mutex);
    is_dropped = true;
    condition.notify_all();
}

void StorageLiveView::refresh(const Context & context)
{
    auto alter_lock = lockAlterIntention(context.getCurrentQueryId());
    {
        std::lock_guard lock(mutex);
        if (getNewBlocks())
            condition.notify_all();
    }
}

BlockInputStreams StorageLiveView::read(
    const Names & /*column_names*/,
    const SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    std::shared_ptr<BlocksBlockInputStream> stream;
    {
        std::lock_guard lock(mutex);
        if (!(*blocks_ptr))
        {
            if (getNewBlocks())
                condition.notify_all();
        }
        stream = std::make_shared<BlocksBlockInputStream>(blocks_ptr, getHeader());
    }
    return { stream };
}

BlockInputStreams StorageLiveView::watch(
    const Names & /*column_names*/,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    ASTWatchQuery & query = typeid_cast<ASTWatchQuery &>(*query_info.query);

    bool has_limit = false;
    UInt64 limit = 0;

    if (query.limit_length)
    {
        has_limit = true;
        limit = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*query.limit_length).value);
    }

    if (query.is_watch_events)
    {
        auto reader = std::make_shared<LiveViewEventsBlockInputStream>(
            std::static_pointer_cast<StorageLiveView>(shared_from_this()),
            blocks_ptr, blocks_metadata_ptr, active_ptr, has_limit, limit,
            context.getSettingsRef().live_view_heartbeat_interval.totalSeconds(),
            temporary_live_view_timeout);

        {
            std::lock_guard no_users_thread_lock(no_users_thread_mutex);
            if (no_users_thread.joinable())
            {
                std::lock_guard lock(no_users_thread_wakeup_mutex);
                no_users_thread_wakeup = true;
                no_users_thread_condition.notify_one();
            }
        }

        {
            std::lock_guard lock(mutex);
            if (!(*blocks_ptr))
            {
                if (getNewBlocks())
                    condition.notify_all();
            }
        }

        processed_stage = QueryProcessingStage::Complete;

        return { reader };
    }
    else
    {
        auto reader = std::make_shared<LiveViewBlockInputStream>(
            std::static_pointer_cast<StorageLiveView>(shared_from_this()),
            blocks_ptr, blocks_metadata_ptr, active_ptr, has_limit, limit,
            context.getSettingsRef().live_view_heartbeat_interval.totalSeconds(),
            temporary_live_view_timeout);

        {
            std::lock_guard no_users_thread_lock(no_users_thread_mutex);
            if (no_users_thread.joinable())
            {
                std::lock_guard lock(no_users_thread_wakeup_mutex);
                no_users_thread_wakeup = true;
                no_users_thread_condition.notify_one();
            }
        }

        {
            std::lock_guard lock(mutex);
            if (!(*blocks_ptr))
            {
                if (getNewBlocks())
                    condition.notify_all();
            }
        }

        processed_stage = QueryProcessingStage::Complete;

        return { reader };
    }
}

void registerStorageLiveView(StorageFactory & factory)
{
    factory.registerStorage("LiveView", [](const StorageFactory::Arguments & args)
    {
        if (!args.attach && !args.local_context.getSettingsRef().allow_experimental_live_view)
            throw Exception("Experimental LIVE VIEW feature is not enabled (the setting 'allow_experimental_live_view')", ErrorCodes::SUPPORT_IS_DISABLED);

        return StorageLiveView::create(args.table_id, args.local_context, args.query, args.columns);
    });
}

}
