#include <Storages/StorageMaterializedView.h>

#include <Storages/MaterializedView/RefreshTask.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTCreateQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Access/Common/AccessFlags.h>

#include <Storages/AlterCommands.h>
#include <Storages/StorageFactory.h>
#include <Storages/ReadInOrderOptimizer.h>
#include <Storages/SelectQueryDescription.h>

#include <Common/typeid_cast.h>
#include <Common/checkStackSize.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Sinks/SinkToStorage.h>

#include <Backups/BackupEntriesCollector.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_QUERY;
}

namespace ActionLocks
{
    extern const StorageActionBlockType ViewRefresh;
}

static inline String generateInnerTableName(const StorageID & view_id)
{
    if (view_id.hasUUID())
        return ".inner_id." + toString(view_id.uuid);
    return ".inner." + view_id.getTableName();
}

/// Remove columns from target_header that does not exists in src_header
static void removeNonCommonColumns(const Block & src_header, Block & target_header)
{
    std::set<size_t> target_only_positions;
    for (const auto & column : target_header)
    {
        if (!src_header.has(column.name))
            target_only_positions.insert(target_header.getPositionByName(column.name));
    }
    target_header.erase(target_only_positions);
}

StorageMaterializedView::StorageMaterializedView(
    const StorageID & table_id_,
    ContextPtr local_context,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_,
    bool attach_,
    const String & comment)
    : IStorage(table_id_), WithMutableContext(local_context->getGlobalContext())
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);

    if (!query.select)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "SELECT query is not specified for {}", getName());

    /// If the destination table is not set, use inner table
    has_inner_table = query.to_table_id.empty();
    if (has_inner_table && !query.storage)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
                        "You must specify where to save results of a MaterializedView query: "
                        "either ENGINE or an existing table in a TO clause");

    auto select = SelectQueryDescription::getSelectQueryFromASTForMatView(query.select->clone(), query.refresh_strategy != nullptr, local_context);
    storage_metadata.setSelectQuery(select);
    if (!comment.empty())
        storage_metadata.setComment(comment);
    if (query.refresh_strategy)
        storage_metadata.setRefresh(query.refresh_strategy->clone());

    setInMemoryMetadata(storage_metadata);

    bool point_to_itself_by_uuid = has_inner_table && query.to_inner_uuid != UUIDHelpers::Nil
                                                   && query.to_inner_uuid == table_id_.uuid;
    bool point_to_itself_by_name = !has_inner_table && query.to_table_id.database_name == table_id_.database_name
                                                    && query.to_table_id.table_name == table_id_.table_name;
    if (point_to_itself_by_uuid || point_to_itself_by_name)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Materialized view {} cannot point to itself", table_id_.getFullTableName());

    if (!has_inner_table)
    {
        target_table_id = query.to_table_id;
    }
    else if (attach_)
    {
        /// If there is an ATTACH request, then the internal table must already be created.
        target_table_id = StorageID(getStorageID().database_name, generateInnerTableName(getStorageID()), query.to_inner_uuid);
    }
    else
    {
        /// We will create a query to create an internal table.
        auto create_context = Context::createCopy(local_context);
        auto manual_create_query = std::make_shared<ASTCreateQuery>();
        manual_create_query->setDatabase(getStorageID().database_name);
        manual_create_query->setTable(generateInnerTableName(getStorageID()));
        manual_create_query->uuid = query.to_inner_uuid;

        auto new_columns_list = std::make_shared<ASTColumns>();
        new_columns_list->set(new_columns_list->columns, query.columns_list->columns->ptr());

        manual_create_query->set(manual_create_query->columns_list, new_columns_list);
        manual_create_query->set(manual_create_query->storage, query.storage->ptr());

        InterpreterCreateQuery create_interpreter(manual_create_query, create_context);
        create_interpreter.setInternal(true);
        create_interpreter.execute();

        target_table_id = DatabaseCatalog::instance().getTable({manual_create_query->getDatabase(), manual_create_query->getTable()}, getContext())->getStorageID();
    }

    if (query.refresh_strategy)
    {
        refresher = RefreshTask::create(
            *this,
            getContext(),
            *query.refresh_strategy);
        refresh_on_start = !attach_ && !query.is_create_empty;
    }
}

QueryProcessingStage::Enum StorageMaterializedView::getQueryProcessingStage(
    ContextPtr local_context,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr &,
    SelectQueryInfo & query_info) const
{
    const auto & target_metadata = getTargetTable()->getInMemoryMetadataPtr();
    return getTargetTable()->getQueryProcessingStage(local_context, to_stage, getTargetTable()->getStorageSnapshot(target_metadata, local_context), query_info);
}

void StorageMaterializedView::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const size_t num_streams)
{
    auto storage = getTargetTable();
    auto lock = storage->lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);
    auto target_metadata_snapshot = storage->getInMemoryMetadataPtr();
    auto target_storage_snapshot = storage->getStorageSnapshot(target_metadata_snapshot, local_context);

    if (query_info.order_optimizer)
        query_info.input_order_info = query_info.order_optimizer->getInputOrder(target_metadata_snapshot, local_context);

    storage->read(query_plan, column_names, target_storage_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);

    if (query_plan.isInitialized())
    {
        auto mv_header = getHeaderForProcessingStage(column_names, storage_snapshot, query_info, local_context, processed_stage);
        auto target_header = query_plan.getCurrentDataStream().header;

        /// No need to convert columns that does not exists in MV
        removeNonCommonColumns(mv_header, target_header);

        /// No need to convert columns that does not exists in the result header.
        ///
        /// Distributed storage may process query up to the specific stage, and
        /// so the result header may not include all the columns from the
        /// materialized view.
        removeNonCommonColumns(target_header, mv_header);

        if (!blocksHaveEqualStructure(mv_header, target_header))
        {
            auto converting_actions = ActionsDAG::makeConvertingActions(target_header.getColumnsWithTypeAndName(),
                                                                        mv_header.getColumnsWithTypeAndName(),
                                                                        ActionsDAG::MatchColumnsMode::Name);
            /* Leave columns outside from materialized view structure as is.
             * They may be added in case of distributed query with JOIN.
             * In that case underlying table returns joined columns as well.
             */
            converting_actions->projectInput(false);
            auto converting_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), converting_actions);
            converting_step->setStepDescription("Convert target table structure to MaterializedView structure");
            query_plan.addStep(std::move(converting_step));
        }

        query_plan.addStorageHolder(storage);
        query_plan.addTableLock(std::move(lock));
    }
}

SinkToStoragePtr StorageMaterializedView::write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr local_context, bool async_insert)
{
    auto storage = getTargetTable();
    auto lock = storage->lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);

    auto metadata_snapshot = storage->getInMemoryMetadataPtr();
    auto sink = storage->write(query, metadata_snapshot, local_context, async_insert);

    sink->addTableLock(lock);
    return sink;
}


void StorageMaterializedView::drop()
{
    auto table_id = getStorageID();
    const auto & select_query = getInMemoryMetadataPtr()->getSelectQuery();
    if (!select_query.select_table_id.empty())
        DatabaseCatalog::instance().removeViewDependency(select_query.select_table_id, table_id);

    /// Sync flag and the setting make sense for Atomic databases only.
    /// However, with Atomic databases, IStorage::drop() can be called only from a background task in DatabaseCatalog.
    /// Running synchronous DROP from that task leads to deadlock.
    /// Usually dropInnerTableIfAny is no-op, because the inner table is dropped before enqueueing a drop task for the MV itself.
    /// But there's a race condition with SYSTEM RESTART REPLICA: the inner table might be detached due to RESTART.
    /// In this case, dropInnerTableIfAny will not find the inner table and will not drop it during executions of DROP query for the MV itself.
    /// DDLGuard does not protect from that, because RESTART REPLICA acquires DDLGuard for the inner table name,
    /// but DROP acquires DDLGuard for the name of MV. And we cannot acquire second DDLGuard for the inner name in DROP,
    /// because it may lead to lock-order-inversion (DDLGuards must be acquired in lexicographical order).
    dropInnerTableIfAny(/* sync */ false, getContext());
}

void StorageMaterializedView::dropInnerTableIfAny(bool sync, ContextPtr local_context)
{
    /// We will use `sync` argument wneh this function is called from a DROP query
    /// and will ignore database_atomic_wait_for_drop_and_detach_synchronously when it's called from drop task.
    /// See the comment in StorageMaterializedView::drop.
    /// DDL queries with StorageMaterializedView are fundamentally broken.
    /// Best-effort to make them work: the inner table name is almost always less than the MV name (so it's safe to lock DDLGuard)
    auto inner_table_id = getTargetTableId();
    bool may_lock_ddl_guard = getStorageID().getQualifiedName() < inner_table_id.getQualifiedName();
    if (has_inner_table && tryGetTargetTable())
        InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, getContext(), local_context, inner_table_id,
                                               sync, /* ignore_sync_setting */ true, may_lock_ddl_guard);
}

void StorageMaterializedView::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr local_context, TableExclusiveLockHolder &)
{
    if (has_inner_table)
        InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Truncate, getContext(), local_context, getTargetTableId(), true);
}

void StorageMaterializedView::checkStatementCanBeForwarded() const
{
    if (!has_inner_table)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "MATERIALIZED VIEW targets existing table {}. "
            "Execute the statement directly on it.", getTargetTableId().getNameForLogs());
}

bool StorageMaterializedView::optimize(
    const ASTPtr & query,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup,
    ContextPtr local_context)
{
    checkStatementCanBeForwarded();
    auto storage_ptr = getTargetTable();
    auto metadata_snapshot = storage_ptr->getInMemoryMetadataPtr();
    return storage_ptr->optimize(query, metadata_snapshot, partition, final, deduplicate, deduplicate_by_columns, cleanup, local_context);
}

std::tuple<ContextMutablePtr, std::shared_ptr<ASTInsertQuery>> StorageMaterializedView::prepareRefresh() const
{
    auto refresh_context = Context::createCopy(getContext());
    /// Generate a random query id.
    refresh_context->setCurrentQueryId("");

    CurrentThread::QueryScope query_scope(refresh_context);

    auto inner_table_id = getTargetTableId();
    auto new_table_name = ".tmp" + generateInnerTableName(getStorageID());

    auto db = DatabaseCatalog::instance().getDatabase(inner_table_id.database_name);

    auto create_table_query = db->getCreateTableQuery(inner_table_id.table_name, getContext());
    auto & create_query = create_table_query->as<ASTCreateQuery &>();
    create_query.setTable(new_table_name);
    create_query.setDatabase(db->getDatabaseName());
    create_query.create_or_replace = true;
    create_query.replace_table = true;
    create_query.uuid = UUIDHelpers::Nil;

    InterpreterCreateQuery create_interpreter(create_table_query, refresh_context);
    create_interpreter.setInternal(true);
    create_interpreter.execute();

    StorageID fresh_table = DatabaseCatalog::instance().getTable({create_query.getDatabase(), create_query.getTable()}, getContext())->getStorageID();

    auto insert_query = std::make_shared<ASTInsertQuery>();
    insert_query->select = getInMemoryMetadataPtr()->getSelectQuery().select_query;
    insert_query->setTable(fresh_table.table_name);
    insert_query->setDatabase(fresh_table.database_name);
    insert_query->table_id = fresh_table;

    return {refresh_context, insert_query};
}

StorageID StorageMaterializedView::exchangeTargetTable(StorageID fresh_table, ContextPtr refresh_context)
{
    auto stale_table_id = getTargetTableId();

    auto db = DatabaseCatalog::instance().getDatabase(stale_table_id.database_name);
    auto target_db = DatabaseCatalog::instance().getDatabase(fresh_table.database_name);

    CurrentThread::QueryScope query_scope(refresh_context);

    target_db->renameTable(
        refresh_context, fresh_table.table_name, *db, stale_table_id.table_name, /*exchange=*/true, /*dictionary=*/false);

    std::swap(stale_table_id.database_name, fresh_table.database_name);
    std::swap(stale_table_id.table_name, fresh_table.table_name);
    setTargetTableId(std::move(fresh_table));
    return stale_table_id;
}

void StorageMaterializedView::alter(
    const AlterCommands & params,
    ContextPtr local_context,
    AlterLockHolder &)
{
    auto table_id = getStorageID();
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    StorageInMemoryMetadata old_metadata = getInMemoryMetadata();
    params.apply(new_metadata, local_context);

    /// start modify query
    const auto & new_select = new_metadata.select;
    const auto & old_select = old_metadata.getSelectQuery();

    DatabaseCatalog::instance().updateViewDependency(old_select.select_table_id, table_id, new_select.select_table_id, table_id);
    /// end modify query

    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(local_context, table_id, new_metadata);
    setInMemoryMetadata(new_metadata);

    if (refresher)
        refresher->alterRefreshParams(new_metadata.refresh->as<const ASTRefreshStrategy &>());
}


void StorageMaterializedView::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /*local_context*/) const
{
    for (const auto & command : commands)
    {
        if (command.isCommentAlter())
            continue;
        if (command.type == AlterCommand::MODIFY_QUERY)
            continue;
        if (command.type == AlterCommand::MODIFY_REFRESH && refresher)
            continue;
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}",
            command.type, getName());
    }
}

void StorageMaterializedView::checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const
{
    checkStatementCanBeForwarded();
    getTargetTable()->checkMutationIsPossible(commands, settings);
}

Pipe StorageMaterializedView::alterPartition(
    const StorageMetadataPtr & metadata_snapshot, const PartitionCommands & commands, ContextPtr local_context)
{
    checkStatementCanBeForwarded();
    return getTargetTable()->alterPartition(metadata_snapshot, commands, local_context);
}

void StorageMaterializedView::checkAlterPartitionIsPossible(
    const PartitionCommands & commands, const StorageMetadataPtr & metadata_snapshot,
    const Settings & settings, ContextPtr local_context) const
{
    checkStatementCanBeForwarded();
    getTargetTable()->checkAlterPartitionIsPossible(commands, metadata_snapshot, settings, local_context);
}

void StorageMaterializedView::mutate(const MutationCommands & commands, ContextPtr local_context)
{
    checkStatementCanBeForwarded();
    getTargetTable()->mutate(commands, local_context);
}

void StorageMaterializedView::renameInMemory(const StorageID & new_table_id)
{
    auto old_table_id = getStorageID();
    auto inner_table_id = getTargetTableId();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    bool from_atomic_to_atomic_database = old_table_id.hasUUID() && new_table_id.hasUUID();

    if (!from_atomic_to_atomic_database && has_inner_table && tryGetTargetTable())
    {
        auto new_target_table_name = generateInnerTableName(new_table_id);
        auto rename = std::make_shared<ASTRenameQuery>();

        assert(inner_table_id.database_name == old_table_id.database_name);

        ASTRenameQuery::Element elem
        {
            ASTRenameQuery::Table
            {
                inner_table_id.database_name.empty() ? nullptr : std::make_shared<ASTIdentifier>(inner_table_id.database_name),
                std::make_shared<ASTIdentifier>(inner_table_id.table_name)
            },
            ASTRenameQuery::Table
            {
                new_table_id.database_name.empty() ? nullptr : std::make_shared<ASTIdentifier>(new_table_id.database_name),
                std::make_shared<ASTIdentifier>(new_target_table_name)
            }
        };
        rename->elements.emplace_back(std::move(elem));

        InterpreterRenameQuery(rename, getContext()).execute();
        updateTargetTableId(new_table_id.database_name, new_target_table_name);
    }

    IStorage::renameInMemory(new_table_id);
    if (from_atomic_to_atomic_database && has_inner_table)
    {
        assert(inner_table_id.database_name == old_table_id.database_name);
        updateTargetTableId(new_table_id.database_name, std::nullopt);
    }
    const auto & select_query = metadata_snapshot->getSelectQuery();
    // TODO Actually we don't need to update dependency if MV has UUID, but then db and table name will be outdated
    DatabaseCatalog::instance().updateViewDependency(select_query.select_table_id, old_table_id, select_query.select_table_id, getStorageID());

    if (refresher)
        refresher->rename(new_table_id);
}

void StorageMaterializedView::startup()
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto & select_query = metadata_snapshot->getSelectQuery();
    if (!select_query.select_table_id.empty())
        DatabaseCatalog::instance().addViewDependency(select_query.select_table_id, getStorageID());

    if (refresher)
    {
        refresher->initializeAndStart(std::static_pointer_cast<StorageMaterializedView>(shared_from_this()));

        if (refresh_on_start)
            refresher->run();
    }
}

void StorageMaterializedView::shutdown(bool)
{
    if (refresher)
        refresher->shutdown();

    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto & select_query = metadata_snapshot->getSelectQuery();
    /// Make sure the dependency is removed after DETACH TABLE
    if (!select_query.select_table_id.empty())
        DatabaseCatalog::instance().removeViewDependency(select_query.select_table_id, getStorageID());
}

StoragePtr StorageMaterializedView::getTargetTable() const
{
    checkStackSize();
    return DatabaseCatalog::instance().getTable(getTargetTableId(), getContext());
}

StoragePtr StorageMaterializedView::tryGetTargetTable() const
{
    checkStackSize();
    return DatabaseCatalog::instance().tryGetTable(getTargetTableId(), getContext());
}

NamesAndTypesList StorageMaterializedView::getVirtuals() const
{
    return getTargetTable()->getVirtuals();
}

Strings StorageMaterializedView::getDataPaths() const
{
    if (auto table = tryGetTargetTable())
        return table->getDataPaths();
    return {};
}

void StorageMaterializedView::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions)
{
    /// We backup the target table's data only if it's inner.
    if (hasInnerTable())
    {
        if (auto table = tryGetTargetTable())
            table->backupData(backup_entries_collector, data_path_in_backup, partitions);
        else
            LOG_WARNING(&Poco::Logger::get("StorageMaterializedView"),
                        "Inner table does not exist, will not backup any data");
    }
}

void StorageMaterializedView::restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions)
{
    if (hasInnerTable())
        return getTargetTable()->restoreDataFromBackup(restorer, data_path_in_backup, partitions);
}

bool StorageMaterializedView::supportsBackupPartition() const
{
    if (hasInnerTable())
        return getTargetTable()->supportsBackupPartition();
    return false;
}

std::optional<UInt64> StorageMaterializedView::totalRows(const Settings & settings) const
{
    if (hasInnerTable())
    {
        if (auto table = tryGetTargetTable())
            return table->totalRows(settings);
    }
    return {};
}

std::optional<UInt64> StorageMaterializedView::totalBytes(const Settings & settings) const
{
    if (hasInnerTable())
    {
        if (auto table = tryGetTargetTable())
            return table->totalBytes(settings);
    }
    return {};
}

std::optional<UInt64> StorageMaterializedView::totalBytesUncompressed(const Settings & settings) const
{
    if (hasInnerTable())
    {
        if (auto table = tryGetTargetTable())
            return table->totalBytesUncompressed(settings);
    }
    return {};
}

ActionLock StorageMaterializedView::getActionLock(StorageActionBlockType type)
{
    if (type == ActionLocks::ViewRefresh && refresher)
        refresher->stop();
    if (has_inner_table)
    {
        if (auto target_table = tryGetTargetTable())
            return target_table->getActionLock(type);
    }
    return ActionLock{};
}

bool StorageMaterializedView::isRemote() const
{
    if (auto table = tryGetTargetTable())
        return table->isRemote();
    return false;
}

void StorageMaterializedView::onActionLockRemove(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::ViewRefresh && refresher)
        refresher->start();
}

DB::StorageID StorageMaterializedView::getTargetTableId() const
{
    std::lock_guard guard(target_table_id_mutex);
    return target_table_id;
}

void StorageMaterializedView::setTargetTableId(DB::StorageID id)
{
    std::lock_guard guard(target_table_id_mutex);
    target_table_id = std::move(id);
}

void StorageMaterializedView::updateTargetTableId(std::optional<String> database_name, std::optional<String> table_name)
{
    std::lock_guard guard(target_table_id_mutex);
    if (database_name)
        target_table_id.database_name = *std::move(database_name);
    if (table_name)
        target_table_id.table_name = *std::move(table_name);
}

void registerStorageMaterializedView(StorageFactory & factory)
{
    factory.registerStorage("MaterializedView", [](const StorageFactory::Arguments & args)
    {
        /// Pass local_context here to convey setting for inner table
        return std::make_shared<StorageMaterializedView>(
            args.table_id, args.getLocalContext(), args.query,
            args.columns, args.attach, args.comment);
    });
}

}
