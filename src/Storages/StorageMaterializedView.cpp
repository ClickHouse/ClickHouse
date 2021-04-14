#include <Storages/StorageMaterializedView.h>

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Access/AccessFlags.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Storages/AlterCommands.h>
#include <Storages/StorageFactory.h>
#include <Storages/ReadInOrderOptimizer.h>
#include <Storages/SelectQueryDescription.h>

#include <Common/typeid_cast.h>
#include <Common/checkStackSize.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/QueryPlan/SettingQuotaAndLimitsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_QUERY;
    extern const int QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW;
}

static inline String generateInnerTableName(const StorageID & view_id)
{
    if (view_id.hasUUID())
        return ".inner_id." + toString(view_id.uuid);
    return ".inner." + view_id.getTableName();
}


StorageMaterializedView::StorageMaterializedView(
    const StorageID & table_id_,
    ContextPtr local_context,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_,
    bool attach_)
    : IStorage(table_id_), WithContext(local_context->getGlobalContext())
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);

    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    /// If the destination table is not set, use inner table
    has_inner_table = query.to_table_id.empty();
    if (has_inner_table && !query.storage)
        throw Exception(
            "You must specify where to save results of a MaterializedView query: either ENGINE or an existing table in a TO clause",
            ErrorCodes::INCORRECT_QUERY);

    if (query.select->list_of_selects->children.size() != 1)
        throw Exception("UNION is not supported for MATERIALIZED VIEW", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW);

    auto select = SelectQueryDescription::getSelectQueryFromASTForMatView(query.select->clone(), local_context);
    storage_metadata.setSelectQuery(select);
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
        manual_create_query->database = getStorageID().database_name;
        manual_create_query->table = generateInnerTableName(getStorageID());
        manual_create_query->uuid = query.to_inner_uuid;

        auto new_columns_list = std::make_shared<ASTColumns>();
        new_columns_list->set(new_columns_list->columns, query.columns_list->columns->ptr());

        manual_create_query->set(manual_create_query->columns_list, new_columns_list);
        manual_create_query->set(manual_create_query->storage, query.storage->ptr());

        InterpreterCreateQuery create_interpreter(manual_create_query, create_context);
        create_interpreter.setInternal(true);
        create_interpreter.execute();

        target_table_id = DatabaseCatalog::instance().getTable({manual_create_query->database, manual_create_query->table}, getContext())->getStorageID();
    }

    if (!select.select_table_id.empty())
        DatabaseCatalog::instance().addDependency(select.select_table_id, getStorageID());
}

QueryProcessingStage::Enum StorageMaterializedView::getQueryProcessingStage(
    ContextPtr local_context, QueryProcessingStage::Enum to_stage, SelectQueryInfo & query_info) const
{
    return getTargetTable()->getQueryProcessingStage(local_context, to_stage, query_info);
}

Pipe StorageMaterializedView::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, metadata_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe(
        QueryPlanOptimizationSettings::fromContext(local_context),
        BuildQueryPipelineSettings::fromContext(local_context));
}

void StorageMaterializedView::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    auto storage = getTargetTable();
    auto lock = storage->lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);
    auto target_metadata_snapshot = storage->getInMemoryMetadataPtr();

    if (query_info.order_optimizer)
        query_info.input_order_info = query_info.order_optimizer->getInputOrder(target_metadata_snapshot, local_context);

    storage->read(query_plan, column_names, target_metadata_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);

    if (query_plan.isInitialized())
    {
        auto mv_header = getHeaderForProcessingStage(*this, column_names, metadata_snapshot, query_info, local_context, processed_stage);
        auto target_header = query_plan.getCurrentDataStream().header;
        if (!blocksHaveEqualStructure(mv_header, target_header))
        {
            auto converting_actions = ActionsDAG::makeConvertingActions(target_header.getColumnsWithTypeAndName(),
                                                                        mv_header.getColumnsWithTypeAndName(),
                                                                        ActionsDAG::MatchColumnsMode::Name);
            auto converting_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), converting_actions);
            converting_step->setStepDescription("Convert target table structure to MaterializedView structure");
            query_plan.addStep(std::move(converting_step));
        }

        StreamLocalLimits limits;
        SizeLimits leaf_limits;

        /// Add table lock for destination table.
        auto adding_limits_and_quota = std::make_unique<SettingQuotaAndLimitsStep>(
                query_plan.getCurrentDataStream(),
                storage,
                std::move(lock),
                limits,
                leaf_limits,
                nullptr,
                nullptr);

        adding_limits_and_quota->setStepDescription("Lock destination table for MaterializedView");
        query_plan.addStep(std::move(adding_limits_and_quota));
    }
}

BlockOutputStreamPtr StorageMaterializedView::write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr local_context)
{
    auto storage = getTargetTable();
    auto lock = storage->lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);

    auto metadata_snapshot = storage->getInMemoryMetadataPtr();
    auto stream = storage->write(query, metadata_snapshot, local_context);

    stream->addTableLock(lock);
    return stream;
}


static void executeDropQuery(ASTDropQuery::Kind kind, ContextPtr global_context, ContextPtr current_context, const StorageID & target_table_id, bool no_delay)
{
    if (DatabaseCatalog::instance().tryGetTable(target_table_id, current_context))
    {
        /// We create and execute `drop` query for internal table.
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = target_table_id.database_name;
        drop_query->table = target_table_id.table_name;
        drop_query->kind = kind;
        drop_query->no_delay = no_delay;
        drop_query->if_exists = true;
        ASTPtr ast_drop_query = drop_query;
        /// FIXME We have to use global context to execute DROP query for inner table
        /// to avoid "Not enough privileges" error if current user has only DROP VIEW ON mat_view_name privilege
        /// and not allowed to drop inner table explicitly. Allowing to drop inner table without explicit grant
        /// looks like expected behaviour and we have tests for it.
        auto drop_context = Context::createCopy(global_context);
        drop_context->getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        if (auto txn = current_context->getZooKeeperMetadataTransaction())
        {
            /// For Replicated database
            drop_context->setQueryContext(current_context);
            drop_context->initZooKeeperMetadataTransaction(txn, true);
        }
        InterpreterDropQuery drop_interpreter(ast_drop_query, drop_context);
        drop_interpreter.execute();
    }
}


void StorageMaterializedView::drop()
{
    auto table_id = getStorageID();
    const auto & select_query = getInMemoryMetadataPtr()->getSelectQuery();
    if (!select_query.select_table_id.empty())
        DatabaseCatalog::instance().removeDependency(select_query.select_table_id, table_id);

    dropInnerTable(true, getContext());
}

void StorageMaterializedView::dropInnerTable(bool no_delay, ContextPtr local_context)
{
    if (has_inner_table && tryGetTargetTable())
        executeDropQuery(ASTDropQuery::Kind::Drop, getContext(), local_context, target_table_id, no_delay);
}

void StorageMaterializedView::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr local_context, TableExclusiveLockHolder &)
{
    if (has_inner_table)
        executeDropQuery(ASTDropQuery::Kind::Truncate, getContext(), local_context, target_table_id, true);
}

void StorageMaterializedView::checkStatementCanBeForwarded() const
{
    if (!has_inner_table)
        throw Exception(
            "MATERIALIZED VIEW targets existing table " + target_table_id.getNameForLogs() + ". "
            + "Execute the statement directly on it.", ErrorCodes::INCORRECT_QUERY);
}

bool StorageMaterializedView::optimize(
    const ASTPtr & query,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    ContextPtr local_context)
{
    checkStatementCanBeForwarded();
    auto storage_ptr = getTargetTable();
    auto metadata_snapshot = storage_ptr->getInMemoryMetadataPtr();
    return getTargetTable()->optimize(query, metadata_snapshot, partition, final, deduplicate, deduplicate_by_columns, local_context);
}

void StorageMaterializedView::alter(
    const AlterCommands & params,
    ContextPtr local_context,
    TableLockHolder &)
{
    auto table_id = getStorageID();
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    StorageInMemoryMetadata old_metadata = getInMemoryMetadata();
    params.apply(new_metadata, local_context);

    /// start modify query
    if (local_context->getSettingsRef().allow_experimental_alter_materialized_view_structure)
    {
        const auto & new_select = new_metadata.select;
        const auto & old_select = old_metadata.getSelectQuery();

        DatabaseCatalog::instance().updateDependency(old_select.select_table_id, table_id, new_select.select_table_id, table_id);

        new_metadata.setSelectQuery(new_select);
    }
    /// end modify query

    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(local_context, table_id, new_metadata);
    setInMemoryMetadata(new_metadata);
}


void StorageMaterializedView::checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const
{
    const auto & settings = local_context->getSettingsRef();
    if (settings.allow_experimental_alter_materialized_view_structure)
    {
        for (const auto & command : commands)
        {
            if (!command.isCommentAlter() && command.type != AlterCommand::MODIFY_QUERY)
                throw Exception(
                    "Alter of type '" + alterTypeToString(command.type) + "' is not supported by storage " + getName(),
                    ErrorCodes::NOT_IMPLEMENTED);
        }
    }
    else
    {
        for (const auto & command : commands)
        {
            if (!command.isCommentAlter())
                throw Exception(
                    "Alter of type '" + alterTypeToString(command.type) + "' is not supported by storage " + getName(),
                    ErrorCodes::NOT_IMPLEMENTED);
        }
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
    const PartitionCommands & commands, const StorageMetadataPtr & metadata_snapshot, const Settings & settings) const
{
    checkStatementCanBeForwarded();
    getTargetTable()->checkAlterPartitionIsPossible(commands, metadata_snapshot, settings);
}

void StorageMaterializedView::mutate(const MutationCommands & commands, ContextPtr local_context)
{
    checkStatementCanBeForwarded();
    getTargetTable()->mutate(commands, local_context);
}

void StorageMaterializedView::renameInMemory(const StorageID & new_table_id)
{
    auto old_table_id = getStorageID();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    bool from_atomic_to_atomic_database = old_table_id.hasUUID() && new_table_id.hasUUID();

    if (has_inner_table && tryGetTargetTable() && !from_atomic_to_atomic_database)
    {
        auto new_target_table_name = generateInnerTableName(new_table_id);
        auto rename = std::make_shared<ASTRenameQuery>();

        ASTRenameQuery::Table from;
        from.database = target_table_id.database_name;
        from.table = target_table_id.table_name;

        ASTRenameQuery::Table to;
        to.database = target_table_id.database_name;
        to.table = new_target_table_name;

        ASTRenameQuery::Element elem;
        elem.from = from;
        elem.to = to;
        rename->elements.emplace_back(elem);

        InterpreterRenameQuery(rename, getContext()).execute();
        target_table_id.table_name = new_target_table_name;
    }

    IStorage::renameInMemory(new_table_id);
    const auto & select_query = metadata_snapshot->getSelectQuery();
    // TODO Actually we don't need to update dependency if MV has UUID, but then db and table name will be outdated
    DatabaseCatalog::instance().updateDependency(select_query.select_table_id, old_table_id, select_query.select_table_id, getStorageID());
}

void StorageMaterializedView::shutdown()
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto & select_query = metadata_snapshot->getSelectQuery();
    /// Make sure the dependency is removed after DETACH TABLE
    if (!select_query.select_table_id.empty())
        DatabaseCatalog::instance().removeDependency(select_query.select_table_id, getStorageID());
}

StoragePtr StorageMaterializedView::getTargetTable() const
{
    checkStackSize();
    return DatabaseCatalog::instance().getTable(target_table_id, getContext());
}

StoragePtr StorageMaterializedView::tryGetTargetTable() const
{
    checkStackSize();
    return DatabaseCatalog::instance().tryGetTable(target_table_id, getContext());
}

Strings StorageMaterializedView::getDataPaths() const
{
    if (auto table = tryGetTargetTable())
        return table->getDataPaths();
    return {};
}

ActionLock StorageMaterializedView::getActionLock(StorageActionBlockType type)
{
    return has_inner_table ? getTargetTable()->getActionLock(type) : ActionLock{};
}

void registerStorageMaterializedView(StorageFactory & factory)
{
    factory.registerStorage("MaterializedView", [](const StorageFactory::Arguments & args)
    {
        /// Pass local_context here to convey setting for inner table
        return StorageMaterializedView::create(
            args.table_id, args.getLocalContext(), args.query,
            args.columns, args.attach);
    });
}

}
