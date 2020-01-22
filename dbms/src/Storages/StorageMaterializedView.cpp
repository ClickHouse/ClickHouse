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
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Storages/StorageFactory.h>
#include <Storages/ReadInOrderOptimizer.h>

#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW;
    extern const int QUERY_IS_NOT_SUPPORTED_IN_LIVE_VIEW;
}

static inline String generateInnerTableName(const String & table_name)
{
    return ".inner." + table_name;
}

static StorageID extractDependentTableFromSelectQuery(ASTSelectQuery & query, Context & context, bool add_default_db = true)
{
    if (add_default_db)
    {
        AddDefaultDatabaseVisitor visitor(context.getCurrentDatabase(), nullptr);
        visitor.visit(query);
    }

    if (auto db_and_table = getDatabaseAndTable(query, 0))
    {
        return StorageID(db_and_table->database, db_and_table->table/*, db_and_table->uuid*/);
    }
    else if (auto subquery = extractTableExpression(query, 0))
    {
        auto * ast_select = subquery->as<ASTSelectWithUnionQuery>();
        if (!ast_select)
            throw Exception("Logical error while creating StorageMaterializedView. "
                            "Could not retrieve table name from select query.",
                            DB::ErrorCodes::LOGICAL_ERROR);
        if (ast_select->list_of_selects->children.size() != 1)
            throw Exception("UNION is not supported for MATERIALIZED VIEW",
                  ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW);

        auto & inner_query = ast_select->list_of_selects->children.at(0);

        return extractDependentTableFromSelectQuery(inner_query->as<ASTSelectQuery &>(), context, false);
    }
    else
        return StorageID::createEmpty();
}


static void checkAllowedQueries(const ASTSelectQuery & query)
{
    if (query.prewhere() || query.final() || query.sample_size())
        throw Exception("MATERIALIZED VIEW cannot have PREWHERE, SAMPLE or FINAL.", DB::ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW);

    ASTPtr subquery = extractTableExpression(query, 0);
    if (!subquery)
        return;

    if (const auto * ast_select = subquery->as<ASTSelectWithUnionQuery>())
    {
        if (ast_select->list_of_selects->children.size() != 1)
            throw Exception("UNION is not supported for MATERIALIZED VIEW", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW);

        const auto & inner_query = ast_select->list_of_selects->children.at(0);

        checkAllowedQueries(inner_query->as<ASTSelectQuery &>());
    }
}


StorageMaterializedView::StorageMaterializedView(
    const StorageID & table_id_,
    Context & local_context,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_,
    bool attach_)
    : IStorage(table_id_), global_context(local_context.getGlobalContext())
{
    setColumns(columns_);

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

    inner_query = query.select->list_of_selects->children.at(0);

    auto & select_query = inner_query->as<ASTSelectQuery &>();
    select_table_id = extractDependentTableFromSelectQuery(select_query, local_context);
    checkAllowedQueries(select_query);

    if (!has_inner_table)
        target_table_id = query.to_table_id;
    else if (attach_)
    {
        /// If there is an ATTACH request, then the internal table must already be created.
        target_table_id = StorageID(table_id_.database_name, generateInnerTableName(table_id_.table_name));
    }
    else
    {
        /// We will create a query to create an internal table.
        auto manual_create_query = std::make_shared<ASTCreateQuery>();
        manual_create_query->database = table_id_.database_name;
        manual_create_query->table = generateInnerTableName(table_id_.table_name);

        auto new_columns_list = std::make_shared<ASTColumns>();
        new_columns_list->set(new_columns_list->columns, query.columns_list->columns->ptr());

        manual_create_query->set(manual_create_query->columns_list, new_columns_list);
        manual_create_query->set(manual_create_query->storage, query.storage->ptr());

        InterpreterCreateQuery create_interpreter(manual_create_query, local_context);
        create_interpreter.setInternal(true);
        create_interpreter.execute();

        target_table_id = global_context.getTable(manual_create_query->database, manual_create_query->table)->getStorageID();
    }

    if (!select_table_id.empty())
        global_context.addDependency(select_table_id, table_id_);
}

NameAndTypePair StorageMaterializedView::getColumn(const String & column_name) const
{
    return getTargetTable()->getColumn(column_name);
}

bool StorageMaterializedView::hasColumn(const String & column_name) const
{
    return getTargetTable()->hasColumn(column_name);
}

QueryProcessingStage::Enum StorageMaterializedView::getQueryProcessingStage(const Context & context) const
{
    return getTargetTable()->getQueryProcessingStage(context);
}

BlockInputStreams StorageMaterializedView::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    auto storage = getTargetTable();
    auto lock = storage->lockStructureForShare(false, context.getCurrentQueryId());
    if (query_info.order_by_optimizer)
        query_info.input_sorting_info = query_info.order_by_optimizer->getInputOrder(storage);

    auto streams = storage->read(column_names, query_info, context, processed_stage, max_block_size, num_streams);
    for (auto & stream : streams)
        stream->addTableLock(lock);
    return streams;
}

BlockOutputStreamPtr StorageMaterializedView::write(const ASTPtr & query, const Context & context)
{
    auto storage = getTargetTable();
    auto lock = storage->lockStructureForShare(true, context.getCurrentQueryId());
    auto stream = storage->write(query, context);
    stream->addTableLock(lock);
    return stream;
}


static void executeDropQuery(ASTDropQuery::Kind kind, Context & global_context, const StorageID & target_table_id)
{
    if (global_context.tryGetTable(target_table_id))
    {
        /// We create and execute `drop` query for internal table.
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = target_table_id.database_name;
        drop_query->table = target_table_id.table_name;
        drop_query->kind = kind;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, global_context);
        drop_interpreter.execute();
    }
}


void StorageMaterializedView::drop()
{
    auto table_id = getStorageID();
    if (!select_table_id.empty())
        global_context.removeDependency(select_table_id, table_id);

    if (has_inner_table && tryGetTargetTable())
        executeDropQuery(ASTDropQuery::Kind::Drop, global_context, target_table_id);
}

void StorageMaterializedView::truncate(const ASTPtr &, const Context &, TableStructureWriteLockHolder &)
{
    if (has_inner_table)
        executeDropQuery(ASTDropQuery::Kind::Truncate, global_context, target_table_id);
}

void StorageMaterializedView::checkStatementCanBeForwarded() const
{
    if (!has_inner_table)
        throw Exception(
            "MATERIALIZED VIEW targets existing table " + target_table_id.getNameForLogs() + ". "
            + "Execute the statement directly on it.", ErrorCodes::INCORRECT_QUERY);
}

bool StorageMaterializedView::optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context)
{
    checkStatementCanBeForwarded();
    return getTargetTable()->optimize(query, partition, final, deduplicate, context);
}

void StorageMaterializedView::alterPartition(const ASTPtr & query, const PartitionCommands &commands, const Context &context)
{
    checkStatementCanBeForwarded();
    getTargetTable()->alterPartition(query, commands, context);
}

void StorageMaterializedView::mutate(const MutationCommands & commands, const Context & context)
{
    checkStatementCanBeForwarded();
    getTargetTable()->mutate(commands, context);
}

void StorageMaterializedView::renameInMemory(const String & new_database_name, const String & new_table_name)
{
    if (has_inner_table && tryGetTargetTable())
    {
        auto new_target_table_name = generateInnerTableName(new_table_name);
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

        InterpreterRenameQuery(rename, global_context).execute();
        target_table_id.table_name = new_target_table_name;
    }

    /// Global lock on server context, so there will not be point between removing and adding dependency
    // TODO Actually we don't need to update dependency if MV has UUID, but then db and table name will be outdated
    auto lock = global_context.getLock();

    if (!select_table_id.empty())
        global_context.removeDependencyUnsafe(select_table_id, getStorageID());
    IStorage::renameInMemory(new_database_name, new_table_name);
    if (!select_table_id.empty())
        global_context.addDependencyUnsafe(select_table_id, getStorageID());
}

void StorageMaterializedView::shutdown()
{
    /// Make sure the dependency is removed after DETACH TABLE
    if (!select_table_id.empty())
        global_context.removeDependency(select_table_id, getStorageID());
}

StoragePtr StorageMaterializedView::getTargetTable() const
{
    return global_context.getTable(target_table_id);
}

StoragePtr StorageMaterializedView::tryGetTargetTable() const
{
    return global_context.tryGetTable(target_table_id);
}

Strings StorageMaterializedView::getDataPaths() const
{
    if (auto table = tryGetTargetTable())
        return table->getDataPaths();
    return {};
}

void StorageMaterializedView::checkTableCanBeDropped() const
{
    /// Don't drop the target table if it was created manually via 'TO inner_table' statement
    if (!has_inner_table)
        return;

    auto target_table = tryGetTargetTable();
    if (!target_table)
        return;

    target_table->checkTableCanBeDropped();
}

void StorageMaterializedView::checkPartitionCanBeDropped(const ASTPtr & partition)
{
    /// Don't drop the partition in target table if it was created manually via 'TO inner_table' statement
    if (!has_inner_table)
        return;

    auto target_table = tryGetTargetTable();
    if (!target_table)
        return;

    target_table->checkPartitionCanBeDropped(partition);
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
            args.table_id, args.local_context, args.query,
            args.columns, args.attach);
    });
}

}
