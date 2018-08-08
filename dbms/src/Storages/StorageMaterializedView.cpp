#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTIdentifier.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>

#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageFactory.h>

#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW;
}


static void extractDependentTable(ASTSelectQuery & query, String & select_database_name, String & select_table_name)
{
    auto query_table = query.table();

    if (!query_table)
        return;

    if (auto ast_id = typeid_cast<const ASTIdentifier *>(query_table.get()))
    {
        auto query_database = query.database();

        if (!query_database)
            query.setDatabaseIfNeeded(select_database_name);

        select_table_name = ast_id->name;
        select_database_name = query_database ? typeid_cast<const ASTIdentifier &>(*query_database).name : select_database_name;

    }
    else if (auto ast_select = typeid_cast<ASTSelectWithUnionQuery *>(query_table.get()))
    {
        if (ast_select->list_of_selects->children.size() != 1)
            throw Exception("UNION is not supported for MATERIALIZED VIEW", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW);

        auto & inner_query = ast_select->list_of_selects->children.at(0);

        extractDependentTable(typeid_cast<ASTSelectQuery &>(*inner_query), select_database_name, select_table_name);
    }
    else
        throw Exception("Logical error while creating StorageMaterializedView."
            " Could not retrieve table name from select query.",
            DB::ErrorCodes::LOGICAL_ERROR);
}


static void checkAllowedQueries(const ASTSelectQuery & query)
{
    if (query.prewhere_expression || query.final() || query.sample_size())
        throw Exception("MATERIALIZED VIEW cannot have PREWHERE, SAMPLE or FINAL.", DB::ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW);

    auto query_table = query.table();

    if (!query_table)
        return;

    if (auto ast_select = typeid_cast<const ASTSelectWithUnionQuery *>(query_table.get()))
    {
        if (ast_select->list_of_selects->children.size() != 1)
            throw Exception("UNION is not supported for MATERIALIZED VIEW", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW);

        const auto & inner_query = ast_select->list_of_selects->children.at(0);

        checkAllowedQueries(typeid_cast<const ASTSelectQuery &>(*inner_query));
    }
}


StorageMaterializedView::StorageMaterializedView(
    const String & table_name_,
    const String & database_name_,
    Context & local_context,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_,
    bool attach_)
    : IStorage{columns_}, table_name(table_name_),
    database_name(database_name_), global_context(local_context.getGlobalContext())
{
    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    if (!query.storage && query.to_table.empty())
        throw Exception(
            "You must specify where to save results of a MaterializedView query: either ENGINE or an existing table in a TO clause",
            ErrorCodes::INCORRECT_QUERY);

    /// Default value, if only table name exist in the query
    select_database_name = local_context.getCurrentDatabase();
    if (query.select->list_of_selects->children.size() != 1)
        throw Exception("UNION is not supported for MATERIALIZED VIEW", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW);

    inner_query = query.select->list_of_selects->children.at(0);

    ASTSelectQuery & select_query = typeid_cast<ASTSelectQuery &>(*inner_query);
    extractDependentTable(select_query, select_database_name, select_table_name);
    checkAllowedQueries(select_query);

    if (!select_table_name.empty())
        global_context.addDependency(
            DatabaseAndTableName(select_database_name, select_table_name),
            DatabaseAndTableName(database_name, table_name));

    // If the destination table is not set, use inner table
    if (!query.to_table.empty())
    {
        target_database_name = query.to_database;
        target_table_name = query.to_table;
    }
    else
    {
        target_database_name = database_name;
        target_table_name = ".inner." + table_name;
        has_inner_table = true;
    }

    /// If there is an ATTACH request, then the internal table must already be connected.
    if (!attach_ && has_inner_table)
    {
        /// We will create a query to create an internal table.
        auto manual_create_query = std::make_shared<ASTCreateQuery>();
        manual_create_query->database = target_database_name;
        manual_create_query->table = target_table_name;
        manual_create_query->set(manual_create_query->columns, query.columns->ptr());
        manual_create_query->set(manual_create_query->storage, query.storage->ptr());

        /// Execute the query.
        try
        {
            InterpreterCreateQuery create_interpreter(manual_create_query, local_context);
            create_interpreter.setInternal(true);
            create_interpreter.execute();
        }
        catch (...)
        {
            /// In case of any error we should remove dependency to the view.
            if (!select_table_name.empty())
                global_context.removeDependency(
                    DatabaseAndTableName(select_database_name, select_table_name),
                    DatabaseAndTableName(database_name, table_name));

            throw;
        }
    }
}

NameAndTypePair StorageMaterializedView::getColumn(const String & column_name) const
{
    return getTargetTable()->getColumn(column_name);
}

bool StorageMaterializedView::hasColumn(const String & column_name) const
{
    return getTargetTable()->hasColumn(column_name);
}

BlockInputStreams StorageMaterializedView::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    return getTargetTable()->read(column_names, query_info, context, processed_stage, max_block_size, num_streams);
}

BlockOutputStreamPtr StorageMaterializedView::write(const ASTPtr & query, const Settings & settings)
{
    return getTargetTable()->write(query, settings);
}


static void executeDropQuery(ASTDropQuery::Kind kind, Context & global_context, const String & target_database_name, const String & target_table_name)
{
    if (global_context.tryGetTable(target_database_name, target_table_name))
    {
        /// We create and execute `drop` query for internal table.
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = target_database_name;
        drop_query->table = target_table_name;
        drop_query->kind = kind;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, global_context);
        drop_interpreter.execute();
    }
}


void StorageMaterializedView::drop()
{
    global_context.removeDependency(
        DatabaseAndTableName(select_database_name, select_table_name),
        DatabaseAndTableName(database_name, table_name));

    if (has_inner_table && tryGetTargetTable())
        executeDropQuery(ASTDropQuery::Kind::Drop, global_context, target_database_name, target_table_name);
}

void StorageMaterializedView::truncate(const ASTPtr &)
{
    if (has_inner_table)
        executeDropQuery(ASTDropQuery::Kind::Truncate, global_context, target_database_name, target_table_name);
}

void StorageMaterializedView::checkStatementCanBeForwarded() const
{
    if (!has_inner_table)
        throw Exception(
            "MATERIALIZED VIEW targets existing table " + target_database_name + "." + target_table_name + ". "
            + "Execute the statement directly on it.", ErrorCodes::INCORRECT_QUERY);
}

bool StorageMaterializedView::optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context)
{
    checkStatementCanBeForwarded();
    return getTargetTable()->optimize(query, partition, final, deduplicate, context);
}

void StorageMaterializedView::dropPartition(const ASTPtr & query, const ASTPtr & partition, bool detach, const Context & context)
{
    checkStatementCanBeForwarded();
    getTargetTable()->dropPartition(query, partition, detach, context);
}

void StorageMaterializedView::clearColumnInPartition(const ASTPtr & partition, const Field & column_name, const Context & context)
{
    checkStatementCanBeForwarded();
    getTargetTable()->clearColumnInPartition(partition, column_name, context);
}

void StorageMaterializedView::attachPartition(const ASTPtr & partition, bool part, const Context & context)
{
    checkStatementCanBeForwarded();
    getTargetTable()->attachPartition(partition, part, context);
}

void StorageMaterializedView::freezePartition(const ASTPtr & partition, const String & with_name, const Context & context)
{
    checkStatementCanBeForwarded();
    getTargetTable()->freezePartition(partition, with_name, context);
}

void StorageMaterializedView::shutdown()
{
    /// Make sure the dependency is removed after DETACH TABLE
    global_context.removeDependency(
        DatabaseAndTableName(select_database_name, select_table_name),
        DatabaseAndTableName(database_name, table_name));
}

StoragePtr StorageMaterializedView::getTargetTable() const
{
    return global_context.getTable(target_database_name, target_table_name);
}

StoragePtr StorageMaterializedView::tryGetTargetTable() const
{
    return global_context.tryGetTable(target_database_name, target_table_name);
}

String StorageMaterializedView::getDataPath() const
{
    if (auto table = tryGetTargetTable())
        return table->getDataPath();
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

void registerStorageMaterializedView(StorageFactory & factory)
{
    factory.registerStorage("MaterializedView", [](const StorageFactory::Arguments & args)
    {
        /// Pass local_context here to convey setting for inner table
        return StorageMaterializedView::create(
            args.table_name, args.database_name, args.local_context, args.query,
            args.columns, args.attach);
    });
}

}
