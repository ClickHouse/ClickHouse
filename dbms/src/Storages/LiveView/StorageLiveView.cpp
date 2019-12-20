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
#include <Parsers/ASTIdentifier.h>
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
#include <Storages/LiveView/ProxyStorage.h>

#include <Storages/StorageFactory.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>

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

static void extractDependentTable(ASTSelectQuery & query, String & select_database_name, String & select_table_name)
{
    auto db_and_table = getDatabaseAndTable(query, 0);
    ASTPtr subquery = extractTableExpression(query, 0);

    if (!db_and_table && !subquery)
        return;

    if (db_and_table)
    {
        select_table_name = db_and_table->table;

        if (db_and_table->database.empty())
        {
            db_and_table->database = select_database_name;
            AddDefaultDatabaseVisitor visitor(select_database_name);
            visitor.visit(query);
        }
        else
            select_database_name = db_and_table->database;
    }
    else if (auto * ast_select = subquery->as<ASTSelectWithUnionQuery>())
    {
        if (ast_select->list_of_selects->children.size() != 1)
            throw Exception("UNION is not supported for LIVE VIEW", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_LIVE_VIEW);

        auto & inner_query = ast_select->list_of_selects->children.at(0);

        extractDependentTable(inner_query->as<ASTSelectQuery &>(), select_database_name, select_table_name);
    }
    else
        throw Exception("Logical error while creating StorageLiveView."
            " Could not retrieve table name from select query.",
            DB::ErrorCodes::LOGICAL_ERROR);
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
    BlocksPtrs mergeable_blocks;
    BlocksPtr new_mergeable_blocks = std::make_shared<Blocks>();

    {
        std::lock_guard lock(live_view.mutex);

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
            std::lock_guard lock(live_view.mutex);

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


StorageLiveView::StorageLiveView(
    const String & table_name_,
    const String & database_name_,
    Context & local_context,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_)
    : table_name(table_name_),
    database_name(database_name_), global_context(local_context.getGlobalContext())
{
    setColumns(columns_);

    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    /// Default value, if only table name exist in the query
    select_database_name = local_context.getCurrentDatabase();
    if (query.select->list_of_selects->children.size() != 1)
        throw Exception("UNION is not supported for LIVE VIEW", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_LIVE_VIEW);

    inner_query = query.select->list_of_selects->children.at(0);

    ASTSelectQuery & select_query = typeid_cast<ASTSelectQuery &>(*inner_query);
    extractDependentTable(select_query, select_database_name, select_table_name);

    /// If the table is not specified - use the table `system.one`
    if (select_table_name.empty())
    {
        select_database_name = "system";
        select_table_name = "one";
    }

    global_context.addDependency(
        DatabaseAndTableName(select_database_name, select_table_name),
        DatabaseAndTableName(database_name, table_name));

    is_temporary = query.temporary;
    temporary_live_view_timeout = local_context.getSettingsRef().temporary_live_view_timeout.totalSeconds();

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
        auto storage = global_context.getTable(select_database_name, select_table_name);
        sample_block = InterpreterSelectQuery(inner_query, global_context, storage,
            SelectQueryOptions(QueryProcessingStage::Complete)).getSampleBlock();
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

bool StorageLiveView::getNewBlocks()
{
    SipHash hash;
    UInt128 key;
    BlocksPtr new_blocks = std::make_shared<Blocks>();
    BlocksMetadataPtr new_blocks_metadata = std::make_shared<BlocksMetadata>();
    BlocksPtr new_mergeable_blocks = std::make_shared<Blocks>();

    InterpreterSelectQuery interpreter(inner_query->clone(), global_context, SelectQueryOptions(QueryProcessingStage::WithMergeableState), Names());
    auto mergeable_stream = std::make_shared<MaterializingBlockInputStream>(interpreter.execute().in);

    while (Block block = mergeable_stream->read())
        new_mergeable_blocks->push_back(block);

    mergeable_blocks = std::make_shared<std::vector<BlocksPtr>>();
    mergeable_blocks->push_back(new_mergeable_blocks);
    BlockInputStreamPtr from = std::make_shared<BlocksBlockInputStream>(std::make_shared<BlocksPtr>(new_mergeable_blocks), mergeable_stream->getHeader());
    auto proxy_storage = ProxyStorage::createProxyStorage(global_context.getTable(select_database_name, select_table_name), {from}, QueryProcessingStage::WithMergeableState);
    InterpreterSelectQuery select(inner_query->clone(), global_context, proxy_storage, SelectQueryOptions(QueryProcessingStage::Complete));
    BlockInputStreamPtr data = std::make_shared<MaterializingBlockInputStream>(select.execute().in);

    /// Squashing is needed here because the view query can generate a lot of blocks
    /// even when only one block is inserted into the parent table (e.g. if the query is a GROUP BY
    /// and two-level aggregation is triggered).
    data = std::make_shared<SquashingBlockInputStream>(
        data, global_context.getSettingsRef().min_insert_block_size_rows, global_context.getSettingsRef().min_insert_block_size_bytes);

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
    Dependencies dependencies = global_context.getDependencies(database_name, table_name);
    if (!dependencies.empty())
    {
        DatabaseAndTableName database_and_table_name = dependencies.front();
        throw Exception("Table has dependency " + database_and_table_name.first + "." + database_and_table_name.second, ErrorCodes::TABLE_WAS_NOT_DROPPED);
    }
}

void StorageLiveView::noUsersThread(std::shared_ptr<StorageLiveView> storage, const UInt64 & timeout)
{
    bool drop_table = false;

    if (storage->shutdown_called)
        return;

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
                if (!storage->global_context.getDependencies(storage->database_name, storage->table_name).empty())
                    continue;
                drop_table = true;
            }
            break;
        }
    }

    if (drop_table)
    {
        if (storage->global_context.tryGetTable(storage->database_name, storage->table_name))
        {
            try
            {
                /// We create and execute `drop` query for this table
                auto drop_query = std::make_shared<ASTDropQuery>();
                drop_query->database = storage->database_name;
                drop_query->table = storage->table_name;
                drop_query->kind = ASTDropQuery::Kind::Drop;
                ASTPtr ast_drop_query = drop_query;
                InterpreterDropQuery drop_interpreter(ast_drop_query, storage->global_context);
                drop_interpreter.execute();
            }
            catch (...)
            {
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
    global_context.removeDependency(
        DatabaseAndTableName(select_database_name, select_table_name),
        DatabaseAndTableName(database_name, table_name));

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
            context.getSettingsRef().temporary_live_view_timeout.totalSeconds());

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
            context.getSettingsRef().temporary_live_view_timeout.totalSeconds());

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

        return StorageLiveView::create(args.table_name, args.database_name, args.local_context, args.query, args.columns);
    });
}

}
