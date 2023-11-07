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
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Processors/Sources/BlocksSource.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Transforms/SquashingChunksTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Common/SipHash.h>
#include <base/hex.h>

#include <Storages/LiveView/StorageLiveView.h>
#include <Storages/LiveView/LiveViewSource.h>
#include <Storages/LiveView/LiveViewSink.h>
#include <Storages/LiveView/LiveViewEventsSource.h>
#include <Storages/LiveView/StorageBlocks.h>

#include <Storages/StorageFactory.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Access/Common/AccessFlags.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int QUERY_IS_NOT_SUPPORTED_IN_LIVE_VIEW;
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNSUPPORTED_METHOD;
}

namespace
{

Pipes blocksToPipes(BlocksPtrs blocks, Block & sample_block)
{
    Pipes pipes;
    for (auto & blocks_for_source : *blocks)
        pipes.emplace_back(std::make_shared<BlocksSource>(blocks_for_source, sample_block));

    return pipes;
}

SelectQueryDescription buildSelectQueryDescription(const ASTPtr & select_query, const ContextPtr & context, std::string replace_table_name = {})
{
    ASTPtr inner_query = select_query;
    std::optional<StorageID> dependent_table_storage_id;

    while (true)
    {
        auto * inner_select_with_union_query = inner_query->as<ASTSelectWithUnionQuery>();

        if (inner_select_with_union_query)
        {
            if (inner_select_with_union_query->list_of_selects->children.size() != 1)
                throw Exception(ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_LIVE_VIEW, "UNION is not supported for LIVE VIEW");

            inner_query = inner_select_with_union_query->list_of_selects->children[0];
        }

        auto * inner_select_query = inner_query->as<ASTSelectQuery>();
        if (!inner_select_query)
            throw Exception(DB::ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_LIVE_VIEW,
                "LIVE VIEWs are only supported for queries from tables, "
                "but there is no table name in select query.");

        if (auto db_and_table = getDatabaseAndTable(*inner_select_query, 0))
        {
            String select_database_name = db_and_table->database;
            String select_table_name = db_and_table->table;

            if (select_database_name.empty())
            {
                select_database_name = context->getCurrentDatabase();
                db_and_table->database = select_database_name;
                AddDefaultDatabaseVisitor visitor(context, select_database_name);
                visitor.visit(*inner_select_query);
            }

            if (replace_table_name.empty())
            {
                dependent_table_storage_id = StorageID(select_database_name, select_table_name);
            }
            else
            {
                inner_select_query->replaceDatabaseAndTable("", replace_table_name);
                dependent_table_storage_id = StorageID("", replace_table_name);
            }

            break;
        }
        else if (auto subquery = extractTableExpression(*inner_select_query, 0))
        {
            inner_query = subquery;
        }
        else
        {
            break;
        }
    }

    if (!dependent_table_storage_id)
    {
        /// If the table is not specified - use the table `system.one`
        dependent_table_storage_id = StorageID("system", "one");
    }

    SelectQueryDescription result;
    result.select_table_id = *dependent_table_storage_id;
    result.select_query = select_query;
    result.inner_query = std::move(inner_query);

    return result;
}

struct SelectQueryTreeDescription
{
    QueryTreeNodePtr select_query_node;
    QueryTreeNodePtr inner_query_node;
    QueryTreeNodePtr dependent_table_node;
};

SelectQueryTreeDescription buildSelectQueryTreeDescription(const ASTPtr & select_query, const ContextPtr & context)
{
    auto select_query_node = buildQueryTree(select_query, context);

    QueryTreePassManager query_tree_pass_manager(context);
    addQueryTreePasses(query_tree_pass_manager);
    query_tree_pass_manager.run(select_query_node);

    QueryTreeNodePtr inner_query_node = select_query_node;
    QueryTreeNodePtr dependent_table_node;

    while (true)
    {
        auto & query_node = inner_query_node->as<QueryNode &>();

        auto left_table_expression = extractLeftTableExpression(query_node.getJoinTree());
        auto left_table_expression_node_type = left_table_expression->getNodeType();

        if (left_table_expression_node_type == QueryTreeNodeType::QUERY)
        {
            inner_query_node = left_table_expression;
        }
        else if (left_table_expression_node_type == QueryTreeNodeType::TABLE)
        {
            dependent_table_node = left_table_expression;
            break;
        }
        else if (left_table_expression_node_type == QueryTreeNodeType::TABLE_FUNCTION)
        {
            break;
        }
        else
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "LiveView does not support UNION {} subquery",
                left_table_expression->formatASTForErrorMessage());
        }
    }

    return {std::move(select_query_node), std::move(inner_query_node), std::move(dependent_table_node)};
}

}

StorageLiveView::StorageLiveView(
    const StorageID & table_id_,
    ContextPtr context_,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_,
    const String & comment)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
{
    live_view_context = Context::createCopy(getContext());
    live_view_context->makeQueryContext();

    log = &Poco::Logger::get("StorageLiveView (" + table_id_.database_name + "." + table_id_.table_name + ")");

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    if (!comment.empty())
        storage_metadata.setComment(comment);

    setInMemoryMetadata(storage_metadata);

    if (!query.select)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "SELECT query is not specified for {}", getName());

    auto select_query_clone = query.select->clone();
    select_query_description = buildSelectQueryDescription(select_query_clone, getContext());

    DatabaseCatalog::instance().addViewDependency(select_query_description.select_table_id, table_id_);

    if (query.live_view_periodic_refresh)
    {
        is_periodically_refreshed = true;
        periodic_live_view_refresh = Seconds {*query.live_view_periodic_refresh};
    }

    blocks_ptr = std::make_shared<BlocksPtr>();
    blocks_metadata_ptr = std::make_shared<BlocksMetadataPtr>();
    active_ptr = std::make_shared<bool>(true);

    periodic_refresh_task = getContext()->getSchedulePool().createTask("LiveViewPeriodicRefreshTask",
        [this]
        {
            try
            {
                periodicRefreshTaskFunc();
            }
            catch (...)
            {
                tryLogCurrentException(log, "Exception in LiveView periodic refresh task in BackgroundSchedulePool");
            }
        });
    periodic_refresh_task->deactivate();
}

StorageLiveView::~StorageLiveView()
{
    shutdown();
}

NamesAndTypesList StorageLiveView::getVirtuals() const
{
    return NamesAndTypesList{
        NameAndTypePair("_version", std::make_shared<DataTypeUInt64>())
    };
}

void StorageLiveView::checkTableCanBeDropped([[ maybe_unused ]] ContextPtr query_context) const
{
    auto table_id = getStorageID();
    auto view_ids = DatabaseCatalog::instance().getDependentViews(table_id);
    if (!view_ids.empty())
    {
        StorageID view_id = *view_ids.begin();
        throw Exception(ErrorCodes::TABLE_WAS_NOT_DROPPED, "Table has dependency {}", view_id);
    }
}

void StorageLiveView::drop()
{
    auto table_id = getStorageID();
    DatabaseCatalog::instance().removeViewDependency(select_query_description.select_table_id, table_id);

    std::lock_guard lock(mutex);
    condition.notify_all();
}

void StorageLiveView::startup()
{
    if (is_periodically_refreshed)
        periodic_refresh_task->activate();
}

void StorageLiveView::shutdown()
{
    shutdown_called = true;

    if (is_periodically_refreshed)
        periodic_refresh_task->deactivate();

    DatabaseCatalog::instance().removeViewDependency(select_query_description.select_table_id, getStorageID());
}

Pipe StorageLiveView::read(
    const Names & /*column_names*/,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const size_t /*num_streams*/)
{
    std::lock_guard lock(mutex);

    if (!(*blocks_ptr))
    {
        refreshImpl(lock);
    }
    else if (is_periodically_refreshed)
    {
        Seconds current_time = std::chrono::duration_cast<Seconds>(std::chrono::system_clock::now().time_since_epoch());
        Seconds blocks_time = std::chrono::duration_cast<Seconds>(getBlocksTime(lock).time_since_epoch());

        if ((current_time - periodic_live_view_refresh) >= blocks_time)
            refreshImpl(lock);
    }

    return Pipe(std::make_shared<BlocksSource>(*blocks_ptr, getHeader()));
}

Pipe StorageLiveView::watch(
    const Names & /*column_names*/,
    const SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum & processed_stage,
    size_t /*max_block_size*/,
    const size_t /*num_streams*/)
{
    ASTWatchQuery & query = typeid_cast<ASTWatchQuery &>(*query_info.query);

    bool has_limit = false;
    UInt64 limit = 0;
    Pipe reader;

    if (query.limit_length)
    {
        has_limit = true;
        limit = typeid_cast<ASTLiteral &>(*query.limit_length).value.safeGet<UInt64>();
    }

    if (query.is_watch_events)
        reader = Pipe(std::make_shared<LiveViewEventsSource>(
            std::static_pointer_cast<StorageLiveView>(shared_from_this()),
            blocks_ptr, blocks_metadata_ptr, active_ptr, has_limit, limit,
            local_context->getSettingsRef().live_view_heartbeat_interval.totalSeconds()));
    else
        reader = Pipe(std::make_shared<LiveViewSource>(
            std::static_pointer_cast<StorageLiveView>(shared_from_this()),
            blocks_ptr, blocks_metadata_ptr, active_ptr, has_limit, limit,
            local_context->getSettingsRef().live_view_heartbeat_interval.totalSeconds()));

    {
        std::lock_guard lock(mutex);

        if (!(*blocks_ptr))
            refreshImpl(lock);

        if (is_periodically_refreshed)
            scheduleNextPeriodicRefresh(lock);
    }

    processed_stage = QueryProcessingStage::Complete;
    return reader;
}

void StorageLiveView::writeBlock(const Block & block, ContextPtr local_context)
{
    auto output = std::make_shared<LiveViewSink>(*this);

    /// Check if live view has any readers if not
    /// just reset blocks to empty and do nothing else
    /// When first reader comes the blocks will be read.
    {
        std::lock_guard lock(mutex);
        if (!hasActiveUsers(lock))
        {
            reset(lock);
            return;
        }
    }

    bool is_block_processed = false;
    Pipes from;
    BlocksPtr new_mergeable_blocks = std::make_shared<Blocks>();

    {
        std::lock_guard lock(mutex);
        if (!mergeable_blocks || mergeable_blocks->blocks->size() >= local_context->getGlobalContext()->getSettingsRef().max_live_view_insert_blocks_before_refresh)
        {
            mergeable_blocks = collectMergeableBlocks(local_context, lock);
            from = blocksToPipes(mergeable_blocks->blocks, mergeable_blocks->sample_block);
            is_block_processed = true;
        }
    }

    if (!is_block_processed)
    {
        Pipes pipes;
        pipes.emplace_back(std::make_shared<SourceFromSingleChunk>(block));

        auto creator = [&](const StorageID & blocks_id_global)
        {
            auto parent_metadata = getDependentTableStorage()->getInMemoryMetadataPtr();
            return StorageBlocks::createStorage(
                blocks_id_global, parent_metadata->getColumns(),
                std::move(pipes), QueryProcessingStage::FetchColumns);
        };
        TemporaryTableHolder blocks_storage(local_context, creator);

        QueryPipelineBuilder builder;

        if (local_context->getSettingsRef().allow_experimental_analyzer)
        {
            auto select_description = buildSelectQueryTreeDescription(select_query_description.inner_query, local_context);
            if (select_description.dependent_table_node)
            {
                auto storage = blocks_storage.getTable();
                auto storage_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr(), local_context);
                auto replacement_table_expression = std::make_shared<TableNode>(std::move(storage),
                    TableLockHolder{},
                    std::move(storage_snapshot));

                select_description.inner_query_node = select_description.inner_query_node->cloneAndReplace(
                    select_description.dependent_table_node,
                    std::move(replacement_table_expression));
            }

            InterpreterSelectQueryAnalyzer interpreter(select_description.inner_query_node,
                local_context,
                SelectQueryOptions(QueryProcessingStage::WithMergeableState));
            builder = interpreter.buildQueryPipeline();
        }
        else
        {
            InterpreterSelectQuery interpreter(select_query_description.inner_query,
                local_context,
                blocks_storage.getTable(),
                blocks_storage.getTable()->getInMemoryMetadataPtr(),
                QueryProcessingStage::WithMergeableState);
            builder = interpreter.buildQueryPipeline();
        }

        builder.addSimpleTransform([&](const Block & cur_header)
        {
            return std::make_shared<MaterializingTransform>(cur_header);
        });

        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
        PullingAsyncPipelineExecutor executor(pipeline);
        Block this_block;

        while (executor.pull(this_block))
            new_mergeable_blocks->push_back(this_block);

        if (new_mergeable_blocks->empty())
            return;

        {
            std::lock_guard lock(mutex);
            mergeable_blocks->blocks->push_back(new_mergeable_blocks);
            from = blocksToPipes(mergeable_blocks->blocks, mergeable_blocks->sample_block);
        }
    }

    auto pipeline = completeQuery(std::move(from));
    pipeline.addChain(Chain(std::move(output)));
    pipeline.setSinks([&](const Block & cur_header, Pipe::StreamType)
    {
        return std::make_shared<EmptySink>(cur_header);
    });

    auto executor = pipeline.execute();
    executor->execute(pipeline.getNumThreads(), local_context->getSettingsRef().use_concurrency_control);
}

void StorageLiveView::refresh()
{
    std::lock_guard lock(mutex);
    refreshImpl(lock);
}

void StorageLiveView::refreshImpl(const std::lock_guard<std::mutex> & lock)
{
    if (getNewBlocks(lock))
        condition.notify_all();
}

Block StorageLiveView::getHeader() const
{
    std::lock_guard lock(sample_block_lock);

    if (!sample_block)
    {
        if (live_view_context->getSettingsRef().allow_experimental_analyzer)
        {
            sample_block = InterpreterSelectQueryAnalyzer::getSampleBlock(select_query_description.select_query,
                live_view_context,
                SelectQueryOptions(QueryProcessingStage::Complete));
        }
        else
        {
            auto & select_with_union_query = select_query_description.select_query->as<ASTSelectWithUnionQuery &>();
            auto select_query = select_with_union_query.list_of_selects->children.at(0)->clone();
            sample_block = InterpreterSelectQuery(select_query,
                live_view_context,
                SelectQueryOptions(QueryProcessingStage::Complete)).getSampleBlock();
        }

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

StoragePtr StorageLiveView::getDependentTableStorage() const
{
    return DatabaseCatalog::instance().getTable(select_query_description.select_table_id, getContext());
}

ASTPtr StorageLiveView::getInnerBlocksQuery()
{
    std::lock_guard lock(sample_block_lock);
    if (!inner_blocks_query)
    {
        auto & select_with_union_query = select_query_description.select_query->as<ASTSelectWithUnionQuery &>();
        auto blocks_query = select_with_union_query.list_of_selects->children.at(0)->clone();

        if (!live_view_context->getSettingsRef().allow_experimental_analyzer)
        {
            /// Rewrite inner query with right aliases for JOIN.
            /// It cannot be done in constructor or startup() because InterpreterSelectQuery may access table,
            /// which is not loaded yet during server startup, so we do it lazily
            InterpreterSelectQuery(blocks_query, live_view_context, SelectQueryOptions().modify().analyze()); // NOLINT
        }

        auto table_id = getStorageID();
        std::string replace_table_name = table_id.table_name + "_blocks";
        inner_blocks_query = buildSelectQueryDescription(blocks_query, getContext(), replace_table_name).select_query;
    }

    return inner_blocks_query->clone();
}

MergeableBlocksPtr StorageLiveView::collectMergeableBlocks(ContextPtr local_context, const std::lock_guard<std::mutex> &) const
{
    MergeableBlocksPtr new_mergeable_blocks = std::make_shared<MergeableBlocks>();
    BlocksPtrs new_blocks = std::make_shared<std::vector<BlocksPtr>>();
    BlocksPtr base_blocks = std::make_shared<Blocks>();

    QueryPipelineBuilder builder;

    if (local_context->getSettingsRef().allow_experimental_analyzer)
    {
        InterpreterSelectQueryAnalyzer interpreter(select_query_description.inner_query,
            local_context,
            SelectQueryOptions(QueryProcessingStage::WithMergeableState));
        builder = interpreter.buildQueryPipeline();
    }
    else
    {
        InterpreterSelectQuery interpreter(select_query_description.inner_query->clone(),
            local_context,
            SelectQueryOptions(QueryProcessingStage::WithMergeableState), Names());
        builder = interpreter.buildQueryPipeline();
    }

    builder.addSimpleTransform([&](const Block & cur_header)
    {
        return std::make_shared<MaterializingTransform>(cur_header);
    });

    new_mergeable_blocks->sample_block = builder.getHeader();

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    PullingAsyncPipelineExecutor executor(pipeline);
    Block this_block;

    while (executor.pull(this_block))
        base_blocks->push_back(this_block);

    new_blocks->push_back(base_blocks);
    new_mergeable_blocks->blocks = new_blocks;

    return new_mergeable_blocks;
}

/// Complete query using input streams from mergeable blocks
QueryPipelineBuilder StorageLiveView::completeQuery(Pipes pipes)
{
    auto block_context = Context::createCopy(getContext());
    block_context->makeQueryContext();

    QueryPlanResourceHolder resource_holder;
    resource_holder.interpreter_context.push_back(block_context);

    auto creator = [&](const StorageID & blocks_id_global)
    {
        auto parent_table_metadata = getDependentTableStorage()->getInMemoryMetadataPtr();
        return StorageBlocks::createStorage(
            blocks_id_global, parent_table_metadata->getColumns(),
            std::move(pipes), QueryProcessingStage::WithMergeableState);
    };

    TemporaryTableHolder blocks_storage_table_holder(getContext(), creator);

    QueryPipelineBuilder builder;

    if (block_context->getSettingsRef().allow_experimental_analyzer)
    {
        auto select_description = buildSelectQueryTreeDescription(select_query_description.select_query, block_context);

        if (select_description.dependent_table_node)
        {
            auto storage = blocks_storage_table_holder.getTable();
            auto storage_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr(), block_context);
            auto replacement_table_expression = std::make_shared<TableNode>(std::move(storage),
                TableLockHolder{},
                std::move(storage_snapshot));

            select_description.select_query_node = select_description.select_query_node->cloneAndReplace(
                select_description.dependent_table_node,
                std::move(replacement_table_expression));
        }

        InterpreterSelectQueryAnalyzer interpreter(select_description.select_query_node,
            block_context,
            SelectQueryOptions(QueryProcessingStage::Complete));
        builder = interpreter.buildQueryPipeline();
    }
    else
    {
        auto inner_blocks_query_ = getInnerBlocksQuery();
        block_context->addExternalTable(getBlocksTableName(), std::move(blocks_storage_table_holder));
        InterpreterSelectQuery interpreter(inner_blocks_query_,
            block_context,
            StoragePtr(),
            nullptr,
            SelectQueryOptions(QueryProcessingStage::Complete));
        builder = interpreter.buildQueryPipeline();
    }

    builder.addSimpleTransform([&](const Block & cur_header)
    {
        return std::make_shared<MaterializingTransform>(cur_header);
    });

    /// Squashing is needed here because the view query can generate a lot of blocks
    /// even when only one block is inserted into the parent table (e.g. if the query is a GROUP BY
    /// and two-level aggregation is triggered).
    builder.addSimpleTransform([&](const Block & cur_header)
    {
        return std::make_shared<SquashingChunksTransform>(
            cur_header,
            getContext()->getSettingsRef().min_insert_block_size_rows,
            getContext()->getSettingsRef().min_insert_block_size_bytes);
    });

    builder.addResources(std::move(resource_holder));

    return builder;
}

bool StorageLiveView::getNewBlocks(const std::lock_guard<std::mutex> & lock)
{
    SipHash hash;
    BlocksPtr new_blocks = std::make_shared<Blocks>();
    BlocksMetadataPtr new_blocks_metadata = std::make_shared<BlocksMetadata>();

    /// can't set mergeable_blocks here or anywhere else outside the writeIntoLiveView function
    /// as there could be a race condition when the new block has been inserted into
    /// the source table by the PushingToViews chain and this method
    /// called before writeIntoLiveView function is called which can lead to
    /// the same block added twice to the mergeable_blocks leading to
    /// inserted data to be duplicated
    auto new_mergeable_blocks = collectMergeableBlocks(live_view_context, lock);
    Pipes from = blocksToPipes(new_mergeable_blocks->blocks, new_mergeable_blocks->sample_block);
    auto builder = completeQuery(std::move(from));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));

    PullingAsyncPipelineExecutor executor(pipeline);
    Block block;
    while (executor.pull(block))
    {
        if (block.rows() == 0)
            continue;

        /// calculate hash before virtual column is added
        block.updateHash(hash);
        /// add result version meta column
        block.insert({DataTypeUInt64().createColumnConst(
            block.rows(), getBlocksVersion(lock) + 1)->convertToFullColumnIfConst(),
            std::make_shared<DataTypeUInt64>(),
            "_version"});
        new_blocks->push_back(block);
    }

    const auto key = hash.get128();

    /// Update blocks only if hash keys do not match
    /// NOTE: hash could be different for the same result
    ///       if blocks are not in the same order
    bool updated = false;
    {
        if (getBlocksHashKey(lock) != getHexUIntLowercase(key))
        {
            if (new_blocks->empty())
            {
                new_blocks->push_back(getHeader());
            }
            new_blocks_metadata->hash = getHexUIntLowercase(key);
            new_blocks_metadata->version = getBlocksVersion(lock) + 1;
            new_blocks_metadata->time = std::chrono::system_clock::now();

            (*blocks_ptr) = new_blocks;
            (*blocks_metadata_ptr) = new_blocks_metadata;

            updated = true;
        }
        else
        {
            new_blocks_metadata->hash = getBlocksHashKey(lock);
            new_blocks_metadata->version = getBlocksVersion(lock);
            new_blocks_metadata->time = std::chrono::system_clock::now();

            (*blocks_metadata_ptr) = new_blocks_metadata;
        }
    }
    return updated;
}

void StorageLiveView::periodicRefreshTaskFunc()
{
    LOG_TRACE(log, "periodic refresh task");

    std::lock_guard lock(mutex);

    if (hasActiveUsers(lock))
        scheduleNextPeriodicRefresh(lock);
}

void StorageLiveView::scheduleNextPeriodicRefresh(const std::lock_guard<std::mutex> & lock)
{
    Seconds current_time = std::chrono::duration_cast<Seconds>(std::chrono::system_clock::now().time_since_epoch());
    Seconds blocks_time = std::chrono::duration_cast<Seconds>(getBlocksTime(lock).time_since_epoch());

    if ((current_time - periodic_live_view_refresh) >= blocks_time)
    {
        refreshImpl(lock);
        blocks_time = std::chrono::duration_cast<Seconds>(getBlocksTime(lock).time_since_epoch());
    }
    current_time = std::chrono::duration_cast<Seconds>(std::chrono::system_clock::now().time_since_epoch());

    auto next_refresh_time = blocks_time + periodic_live_view_refresh;

    if (current_time >= next_refresh_time)
        periodic_refresh_task->scheduleAfter(0);
    else
    {
        auto schedule_time = std::chrono::duration_cast<MilliSeconds> (next_refresh_time - current_time);
        periodic_refresh_task->scheduleAfter(static_cast<size_t>(schedule_time.count()));
    }
}

void registerStorageLiveView(StorageFactory & factory)
{
    factory.registerStorage("LiveView", [](const StorageFactory::Arguments & args)
    {
        if (!args.attach && !args.getLocalContext()->getSettingsRef().allow_experimental_live_view)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                            "Experimental LIVE VIEW feature is not enabled (the setting 'allow_experimental_live_view')");

        return std::make_shared<StorageLiveView>(args.table_id, args.getLocalContext(), args.query, args.columns, args.comment);
    });
}

}
