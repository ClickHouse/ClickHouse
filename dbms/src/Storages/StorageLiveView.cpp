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
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/LiveViewBlockInputStream.h>
#include <DataStreams/LiveViewEventsBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <Common/typeid_cast.h>

#include <Storages/StorageLiveView.h>
#include <Storages/StorageFactory.h>
#include <Storages/ProxyStorage.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int QUERY_IS_NOT_SUPPORTED_IN_LIVE_VIEW;
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

static void checkAllowedQueries(const ASTSelectQuery & query)
{
    if (query.prewhere() || query.final() || query.sample_size())
        throw Exception("LIVE VIEW cannot have PREWHERE, SAMPLE or FINAL.", DB::ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_LIVE_VIEW);

    ASTPtr subquery = extractTableExpression(query, 0);
    if (!subquery)
        return;

    if (const auto * ast_select = subquery->as<ASTSelectWithUnionQuery>())
    {
        if (ast_select->list_of_selects->children.size() != 1)
            throw Exception("UNION is not supported for LIVE VIEW", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_LIVE_VIEW);

        const auto & inner_query = ast_select->list_of_selects->children.at(0);

        checkAllowedQueries(inner_query->as<ASTSelectQuery &>());
    }
}

StorageLiveView::StorageLiveView(
    const String & table_name_,
    const String & database_name_,
    Context & local_context,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns)
    : IStorage(columns), table_name(table_name_),
    database_name(database_name_), global_context(local_context.getGlobalContext())
{
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
    if (!sample_block)
    {
        auto storage = global_context.getTable(select_database_name, select_table_name);
        sample_block = InterpreterSelectQuery(inner_query, global_context, storage).getSampleBlock();
        sample_block.insert({DataTypeUInt64().createColumnConst(
            sample_block.rows(), 0)->convertToFullColumnIfConst(),
            std::make_shared<DataTypeUInt64>(),
            "_version"});
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

void StorageLiveView::noUsersThread(const UInt64 & timeout)
{
    if (shutdown_called)
        return;

    bool drop_table = false;

    {
        while (1)
        {
            Poco::FastMutex::ScopedLock lock(noUsersThreadMutex);
            if (!noUsersThreadWakeUp && !noUsersThreadCondition.tryWait(noUsersThreadMutex,
                timeout * 1000))
            {
                noUsersThreadWakeUp = false;
                if (shutdown_called)
                    return;
                if (hasUsers())
                    return;
                if (!global_context.getDependencies(database_name, table_name).empty())
                    continue;
                drop_table = true;
            }
            break;
        }
    }

    if (drop_table)
    {
        if (global_context.tryGetTable(database_name, table_name))
        {
            try
            {
                /// We create and execute `drop` query for this table
                auto drop_query = std::make_shared<ASTDropQuery>();
                drop_query->database = database_name;
                drop_query->table = table_name;
                drop_query->kind = ASTDropQuery::Kind::Drop;
                ASTPtr ast_drop_query = drop_query;
                InterpreterDropQuery drop_interpreter(ast_drop_query, global_context);
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
    if (!startnousersthread_called.compare_exchange_strong(expected, true))
        return;

    if (is_dropped)
        return;

    if (is_temporary)
    {
        if (no_users_thread.joinable())
        {
            {
                Poco::FastMutex::ScopedLock lock(noUsersThreadMutex);
                noUsersThreadWakeUp = true;
                noUsersThreadCondition.signal();
            }
            no_users_thread.join();
        }
        {
            Poco::FastMutex::ScopedLock lock(noUsersThreadMutex);
            noUsersThreadWakeUp = false;
        }
        if (!is_dropped)
            no_users_thread = std::thread(&StorageLiveView::noUsersThread, this, timeout);
    }
    startnousersthread_called = false;
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

    if (no_users_thread.joinable())
    {
        Poco::FastMutex::ScopedLock lock(noUsersThreadMutex);
        noUsersThreadWakeUp = true;
        noUsersThreadCondition.signal();
        /// Must detach the no users thread
        /// as we can't join it as it will result
        /// in a deadlock
        no_users_thread.detach();
    }
}

StorageLiveView::~StorageLiveView()
{
    shutdown();
}

void StorageLiveView::drop()
{
    global_context.removeDependency(
        DatabaseAndTableName(select_database_name, select_table_name),
        DatabaseAndTableName(database_name, table_name));
    Poco::FastMutex::ScopedLock lock(mutex);
    is_dropped = true;
    condition.broadcast();
}

void StorageLiveView::refresh(const Context & context)
{
    auto alter_lock = lockAlterIntention(context.getCurrentQueryId());
    {
        Poco::FastMutex::ScopedLock lock(mutex);
        if (getNewBlocks())
            condition.broadcast();
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
    /// add user to the blocks_ptr
    std::shared_ptr<BlocksPtr> stream_blocks_ptr = blocks_ptr;
    {
        Poco::FastMutex::ScopedLock lock(mutex);
        if (!(*blocks_ptr))
        {
            if (getNewBlocks())
                condition.broadcast();
        }
    }
    return { std::make_shared<BlocksBlockInputStream>(stream_blocks_ptr, getHeader()) };
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

    /// By default infinite stream of updates
    int64_t length = -2;

    if (query.limit_length)
        length = static_cast<int64_t>(safeGet<UInt64>(typeid_cast<ASTLiteral &>(*query.limit_length).value));

    if (query.is_watch_events)
    {
        auto reader = std::make_shared<LiveViewEventsBlockInputStream>(*this, blocks_ptr, blocks_metadata_ptr, active_ptr, condition, mutex, length, context.getSettingsRef().live_view_heartbeat_interval.totalSeconds(),
        context.getSettingsRef().temporary_live_view_timeout.totalSeconds());

        if (no_users_thread.joinable())
        {
            Poco::FastMutex::ScopedLock lock(noUsersThreadMutex);
            noUsersThreadWakeUp = true;
            noUsersThreadCondition.signal();
        }

        {
            Poco::FastMutex::ScopedLock lock(mutex);
            if (!(*blocks_ptr))
            {
                if (getNewBlocks())
                    condition.broadcast();
            }
        }

        processed_stage = QueryProcessingStage::Complete;

        return { reader };
    }
    else
    {
        auto reader = std::make_shared<LiveViewBlockInputStream>(*this, blocks_ptr, blocks_metadata_ptr, active_ptr, condition, mutex, length, context.getSettingsRef().live_view_heartbeat_interval.totalSeconds(),
        context.getSettingsRef().temporary_live_view_timeout.totalSeconds());

        if (no_users_thread.joinable())
        {
            Poco::FastMutex::ScopedLock lock(noUsersThreadMutex);
            noUsersThreadWakeUp = true;
            noUsersThreadCondition.signal();
        }

        {
            Poco::FastMutex::ScopedLock lock(mutex);
            if (!(*blocks_ptr))
            {
                if (getNewBlocks())
                    condition.broadcast();
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
        return StorageLiveView::create(args.table_name, args.database_name, args.local_context, args.query, args.columns);
    });
}

}
