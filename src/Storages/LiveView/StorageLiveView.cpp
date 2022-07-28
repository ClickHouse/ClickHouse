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
#include <Parsers/ASTLiteral.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/BlocksSource.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Common/SipHash.h>
#include <Common/hex.h>

#include <Storages/LiveView/StorageLiveView.h>
#include <Storages/LiveView/LiveViewBlockInputStream.h>
#include <Storages/LiveView/LiveViewBlockOutputStream.h>
#include <Storages/LiveView/LiveViewEventsBlockInputStream.h>
#include <Storages/LiveView/StorageBlocks.h>
#include <Storages/LiveView/TemporaryLiveViewCleaner.h>

#include <Storages/StorageFactory.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/queryToString.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Access/AccessFlags.h>
#include <Processors/Sources/SourceFromSingleChunk.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int QUERY_IS_NOT_SUPPORTED_IN_LIVE_VIEW;
    extern const int SUPPORT_IS_DISABLED;
}


static StorageID extractDependentTable(ASTPtr & query, ContextPtr context, const String & table_name, ASTPtr & inner_subquery)
{
    ASTSelectQuery & select_query = typeid_cast<ASTSelectQuery &>(*query);

    if (auto db_and_table = getDatabaseAndTable(select_query, 0))
    {
        String select_database_name = context->getCurrentDatabase();
        String select_table_name = db_and_table->table;

        if (db_and_table->database.empty())
        {
            db_and_table->database = select_database_name;
            AddDefaultDatabaseVisitor visitor(context, select_database_name);
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
            throw Exception("LIVE VIEWs are only supported for queries from tables, but there is no table name in select query.",
                            DB::ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_LIVE_VIEW);
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

MergeableBlocksPtr StorageLiveView::collectMergeableBlocks(ContextPtr local_context)
{
    ASTPtr mergeable_query = inner_query;

    if (inner_subquery)
        mergeable_query = inner_subquery;

    MergeableBlocksPtr new_mergeable_blocks = std::make_shared<MergeableBlocks>();
    BlocksPtrs new_blocks = std::make_shared<std::vector<BlocksPtr>>();
    BlocksPtr base_blocks = std::make_shared<Blocks>();

    InterpreterSelectQuery interpreter(mergeable_query->clone(), local_context, SelectQueryOptions(QueryProcessingStage::WithMergeableState), Names());

    auto view_mergeable_stream = std::make_shared<MaterializingBlockInputStream>(interpreter.execute().getInputStream());

    while (Block this_block = view_mergeable_stream->read())
        base_blocks->push_back(this_block);

    new_blocks->push_back(base_blocks);

    new_mergeable_blocks->blocks = new_blocks;
    new_mergeable_blocks->sample_block = view_mergeable_stream->getHeader();

    return new_mergeable_blocks;
}

Pipes StorageLiveView::blocksToPipes(BlocksPtrs blocks, Block & sample_block)
{
    Pipes pipes;
    for (auto & blocks_for_source : *blocks)
        pipes.emplace_back(std::make_shared<BlocksSource>(std::make_shared<BlocksPtr>(blocks_for_source), sample_block));

    return pipes;
}

/// Complete query using input streams from mergeable blocks
BlockInputStreamPtr StorageLiveView::completeQuery(Pipes pipes)
{
    //FIXME it's dangerous to create Context on stack
    auto block_context = Context::createCopy(getContext());
    block_context->makeQueryContext();

    auto creator = [&](const StorageID & blocks_id_global)
    {
        auto parent_table_metadata = getParentStorage()->getInMemoryMetadataPtr();
        return StorageBlocks::createStorage(
            blocks_id_global, parent_table_metadata->getColumns(),
            std::move(pipes), QueryProcessingStage::WithMergeableState);
    };
    block_context->addExternalTable(getBlocksTableName(), TemporaryTableHolder(getContext(), creator));

    InterpreterSelectQuery select(getInnerBlocksQuery(), block_context, StoragePtr(), nullptr, SelectQueryOptions(QueryProcessingStage::Complete));
    BlockInputStreamPtr data = std::make_shared<MaterializingBlockInputStream>(select.execute().getInputStream());

    /// Squashing is needed here because the view query can generate a lot of blocks
    /// even when only one block is inserted into the parent table (e.g. if the query is a GROUP BY
    /// and two-level aggregation is triggered).
    data = std::make_shared<SquashingBlockInputStream>(
        data, getContext()->getSettingsRef().min_insert_block_size_rows,
        getContext()->getSettingsRef().min_insert_block_size_bytes);

    return data;
}

void StorageLiveView::writeIntoLiveView(
    StorageLiveView & live_view,
    const Block & block,
    ContextPtr local_context)
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
    Pipes from;
    MergeableBlocksPtr mergeable_blocks;
    BlocksPtr new_mergeable_blocks = std::make_shared<Blocks>();

    {
        std::lock_guard lock(live_view.mutex);

        mergeable_blocks = live_view.getMergeableBlocks();
        if (!mergeable_blocks || mergeable_blocks->blocks->size() >= local_context->getGlobalContext()->getSettingsRef().max_live_view_insert_blocks_before_refresh)
        {
            mergeable_blocks = live_view.collectMergeableBlocks(local_context);
            live_view.setMergeableBlocks(mergeable_blocks);
            from = live_view.blocksToPipes(mergeable_blocks->blocks, mergeable_blocks->sample_block);
            is_block_processed = true;
        }
    }

    if (!is_block_processed)
    {
        ASTPtr mergeable_query = live_view.getInnerQuery();

        if (live_view.getInnerSubQuery())
            mergeable_query = live_view.getInnerSubQuery();

        Pipes pipes;
        pipes.emplace_back(std::make_shared<SourceFromSingleChunk>(block.cloneEmpty(), Chunk(block.getColumns(), block.rows())));

        auto creator = [&](const StorageID & blocks_id_global)
        {
            auto parent_metadata = live_view.getParentStorage()->getInMemoryMetadataPtr();
            return StorageBlocks::createStorage(
                blocks_id_global, parent_metadata->getColumns(),
                std::move(pipes), QueryProcessingStage::FetchColumns);
        };
        TemporaryTableHolder blocks_storage(local_context, creator);

        InterpreterSelectQuery select_block(mergeable_query, local_context, blocks_storage.getTable(), blocks_storage.getTable()->getInMemoryMetadataPtr(),
            QueryProcessingStage::WithMergeableState);

        auto data_mergeable_stream = std::make_shared<MaterializingBlockInputStream>(
            select_block.execute().getInputStream());

        while (Block this_block = data_mergeable_stream->read())
            new_mergeable_blocks->push_back(this_block);

        if (new_mergeable_blocks->empty())
            return;

        {
            std::lock_guard lock(live_view.mutex);

            mergeable_blocks = live_view.getMergeableBlocks();
            mergeable_blocks->blocks->push_back(new_mergeable_blocks);
            from = live_view.blocksToPipes(mergeable_blocks->blocks, mergeable_blocks->sample_block);
        }
    }

    BlockInputStreamPtr data = live_view.completeQuery(std::move(from));
    copyData(*data, *output);
}


StorageLiveView::StorageLiveView(
    const StorageID & table_id_,
    ContextPtr context_,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
{
    live_view_context = Context::createCopy(getContext());
    live_view_context->makeQueryContext();

    log = &Poco::Logger::get("StorageLiveView (" + table_id_.database_name + "." + table_id_.table_name + ")");

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);

    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    /// Default value, if only table name exist in the query
    if (query.select->list_of_selects->children.size() != 1)
        throw Exception("UNION is not supported for LIVE VIEW", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_LIVE_VIEW);

    inner_query = query.select->list_of_selects->children.at(0);

    auto inner_query_tmp = inner_query->clone();
    select_table_id = extractDependentTable(inner_query_tmp, getContext(), table_id_.table_name, inner_subquery);

    DatabaseCatalog::instance().addDependency(select_table_id, table_id_);

    if (query.live_view_timeout)
    {
        is_temporary = true;
        temporary_live_view_timeout = Seconds {*query.live_view_timeout};
    }

    if (query.live_view_periodic_refresh)
    {
        is_periodically_refreshed = true;
        periodic_live_view_refresh = Seconds {*query.live_view_periodic_refresh};
    }

    blocks_ptr = std::make_shared<BlocksPtr>();
    blocks_metadata_ptr = std::make_shared<BlocksMetadataPtr>();
    active_ptr = std::make_shared<bool>(true);

    periodic_refresh_task = getContext()->getSchedulePool().createTask("LieViewPeriodicRefreshTask", [this]{ periodicRefreshTaskFunc(); });
    periodic_refresh_task->deactivate();
}

Block StorageLiveView::getHeader() const
{
    std::lock_guard lock(sample_block_lock);

    if (!sample_block)
    {
        sample_block = InterpreterSelectQuery(inner_query->clone(), live_view_context, SelectQueryOptions(QueryProcessingStage::Complete)).getSampleBlock();
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

StoragePtr StorageLiveView::getParentStorage() const
{
    return DatabaseCatalog::instance().getTable(select_table_id, getContext());
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
        InterpreterSelectQuery(inner_blocks_query, live_view_context, SelectQueryOptions().modify().analyze()); // NOLINT
        auto table_id = getStorageID();
        extractDependentTable(inner_blocks_query, getContext(), table_id.table_name, inner_subquery);
    }
    return inner_blocks_query->clone();
}

bool StorageLiveView::getNewBlocks()
{
    SipHash hash;
    UInt128 key;
    BlocksPtr new_blocks = std::make_shared<Blocks>();
    BlocksMetadataPtr new_blocks_metadata = std::make_shared<BlocksMetadata>();

    /// can't set mergeable_blocks here or anywhere else outside the writeIntoLiveView function
    /// as there could be a race codition when the new block has been inserted into
    /// the source table by the PushingToViewsBlockOutputStream and this method
    /// called before writeIntoLiveView function is called which can lead to
    /// the same block added twice to the mergeable_blocks leading to
    /// inserted data to be duplicated
    auto new_mergeable_blocks = collectMergeableBlocks(live_view_context);
    Pipes from = blocksToPipes(new_mergeable_blocks->blocks, new_mergeable_blocks->sample_block);
    BlockInputStreamPtr data = completeQuery(std::move(from));

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

    hash.get128(key);

    /// Update blocks only if hash keys do not match
    /// NOTE: hash could be different for the same result
    ///       if blocks are not in the same order
    bool updated = false;
    {
        if (getBlocksHashKey() != getHexUIntLowercase(key))
        {
            if (new_blocks->empty())
            {
                new_blocks->push_back(getHeader());
            }
            new_blocks_metadata->hash = getHexUIntLowercase(key);
            new_blocks_metadata->version = getBlocksVersion() + 1;
            new_blocks_metadata->time = std::chrono::system_clock::now();

            (*blocks_ptr) = new_blocks;
            (*blocks_metadata_ptr) = new_blocks_metadata;

            updated = true;
        }
        else
        {
            new_blocks_metadata->hash = getBlocksHashKey();
            new_blocks_metadata->version = getBlocksVersion();
            new_blocks_metadata->time = std::chrono::system_clock::now();

            (*blocks_metadata_ptr) = new_blocks_metadata;
        }
    }
    return updated;
}

void StorageLiveView::checkTableCanBeDropped() const
{
    auto table_id = getStorageID();
    Dependencies dependencies = DatabaseCatalog::instance().getDependencies(table_id);
    if (!dependencies.empty())
    {
        StorageID dependent_table_id = dependencies.front();
        throw Exception("Table has dependency " + dependent_table_id.getNameForLogs(), ErrorCodes::TABLE_WAS_NOT_DROPPED);
    }
}

void StorageLiveView::startup()
{
    if (is_temporary)
        TemporaryLiveViewCleaner::instance().addView(std::static_pointer_cast<StorageLiveView>(shared_from_this()));

    if (is_periodically_refreshed)
        periodic_refresh_task->activate();
}

void StorageLiveView::shutdown()
{
    shutdown_called = true;

    if (is_periodically_refreshed)
        periodic_refresh_task->deactivate();

    DatabaseCatalog::instance().removeDependency(select_table_id, getStorageID());
}

StorageLiveView::~StorageLiveView()
{
    shutdown();
}

void StorageLiveView::drop()
{
    auto table_id = getStorageID();
    DatabaseCatalog::instance().removeDependency(select_table_id, table_id);

    std::lock_guard lock(mutex);
    is_dropped = true;
    condition.notify_all();
}

void StorageLiveView::scheduleNextPeriodicRefresh()
{
    Seconds current_time = std::chrono::duration_cast<Seconds> (std::chrono::system_clock::now().time_since_epoch());
    Seconds blocks_time = std::chrono::duration_cast<Seconds> (getBlocksTime().time_since_epoch());

    if ((current_time - periodic_live_view_refresh) >= blocks_time)
    {
        refresh(false);
        blocks_time = std::chrono::duration_cast<Seconds> (getBlocksTime().time_since_epoch());
    }
    current_time = std::chrono::duration_cast<Seconds> (std::chrono::system_clock::now().time_since_epoch());

    auto next_refresh_time = blocks_time + periodic_live_view_refresh;

    if (current_time >= next_refresh_time)
        periodic_refresh_task->scheduleAfter(0);
    else
    {
        auto schedule_time = std::chrono::duration_cast<MilliSeconds> (next_refresh_time - current_time);
        periodic_refresh_task->scheduleAfter(static_cast<size_t>(schedule_time.count()));
    }
}

void StorageLiveView::periodicRefreshTaskFunc()
{
    LOG_TRACE(log, "periodic refresh task");

    std::lock_guard lock(mutex);

    if (hasActiveUsers())
        scheduleNextPeriodicRefresh();
}

void StorageLiveView::refresh(bool grab_lock)
{
    // Lock is already acquired exclusively from InterperterAlterQuery.cpp InterpreterAlterQuery::execute() method.
    // So, reacquiring lock is not needed and will result in an exception.

    if (grab_lock)
    {
        std::lock_guard lock(mutex);
        if (getNewBlocks())
            condition.notify_all();
    }
    else
    {
        if (getNewBlocks())
            condition.notify_all();
    }
}

Pipe StorageLiveView::read(
    const Names & /*column_names*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    std::lock_guard lock(mutex);

    if (!(*blocks_ptr))
        refresh(false);

    else if (is_periodically_refreshed)
    {
        Seconds current_time = std::chrono::duration_cast<Seconds>(std::chrono::system_clock::now().time_since_epoch());
        Seconds blocks_time = std::chrono::duration_cast<Seconds>(getBlocksTime().time_since_epoch());

        if ((current_time - periodic_live_view_refresh) >= blocks_time)
            refresh(false);
    }

    return Pipe(std::make_shared<BlocksSource>(blocks_ptr, getHeader()));
}

BlockInputStreams StorageLiveView::watch(
    const Names & /*column_names*/,
    const SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum & processed_stage,
    size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    ASTWatchQuery & query = typeid_cast<ASTWatchQuery &>(*query_info.query);

    bool has_limit = false;
    UInt64 limit = 0;
    BlockInputStreamPtr reader;

    if (query.limit_length)
    {
        has_limit = true;
        limit = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*query.limit_length).value);
    }

    if (query.is_watch_events)
        reader = std::make_shared<LiveViewEventsBlockInputStream>(
            std::static_pointer_cast<StorageLiveView>(shared_from_this()),
            blocks_ptr, blocks_metadata_ptr, active_ptr, has_limit, limit,
            local_context->getSettingsRef().live_view_heartbeat_interval.totalSeconds());
    else
        reader = std::make_shared<LiveViewBlockInputStream>(
            std::static_pointer_cast<StorageLiveView>(shared_from_this()),
            blocks_ptr, blocks_metadata_ptr, active_ptr, has_limit, limit,
            local_context->getSettingsRef().live_view_heartbeat_interval.totalSeconds());

    {
        std::lock_guard lock(mutex);

        if (!(*blocks_ptr))
            refresh(false);

        if (is_periodically_refreshed)
            scheduleNextPeriodicRefresh();
    }

    processed_stage = QueryProcessingStage::Complete;
    return { reader };
}

NamesAndTypesList StorageLiveView::getVirtuals() const
{
    return NamesAndTypesList{
        NameAndTypePair("_version", std::make_shared<DataTypeUInt64>())
    };
}

void registerStorageLiveView(StorageFactory & factory)
{
    factory.registerStorage("LiveView", [](const StorageFactory::Arguments & args)
    {
        if (!args.attach && !args.getLocalContext()->getSettingsRef().allow_experimental_live_view)
            throw Exception(
                "Experimental LIVE VIEW feature is not enabled (the setting 'allow_experimental_live_view')",
                ErrorCodes::SUPPORT_IS_DISABLED);

        return StorageLiveView::create(args.table_id, args.getLocalContext(), args.query, args.columns);
    });
}

}
