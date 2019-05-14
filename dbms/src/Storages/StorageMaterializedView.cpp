#include <Storages/StorageMaterializedView.h>

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>

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

static inline String generateInnerTableName(const String & table_name)
{
    return ".inner." + table_name;
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
            throw Exception("UNION is not supported for MATERIALIZED VIEW", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW);

        auto & inner_query = ast_select->list_of_selects->children.at(0);

        extractDependentTable(inner_query->as<ASTSelectQuery &>(), select_database_name, select_table_name);
    }
    else
        throw Exception("Logical error while creating StorageMaterializedView."
            " Could not retrieve table name from select query.",
            DB::ErrorCodes::LOGICAL_ERROR);
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

    auto & select_query = inner_query->as<ASTSelectQuery &>();
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
        target_table_name = generateInnerTableName(table_name);
        has_inner_table = true;
    }

    /// If there is an ATTACH request, then the internal table must already be connected.
    if (!attach_ && has_inner_table)
    {
        /// We will create a query to create an internal table.
        auto manual_create_query = std::make_shared<ASTCreateQuery>();
        manual_create_query->database = target_database_name;
        manual_create_query->table = target_table_name;

        auto new_columns_list = std::make_shared<ASTColumns>();
        new_columns_list->set(new_columns_list->columns, query.columns_list->columns->ptr());

        manual_create_query->set(manual_create_query->columns_list, new_columns_list);
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

void StorageMaterializedView::truncate(const ASTPtr &, const Context &)
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

static void executeRenameQuery(Context & global_context, const String & database_name, const String & table_original_name, const String & new_table_name)
{
    if (global_context.tryGetTable(database_name, table_original_name))
    {
            auto rename = std::make_shared<ASTRenameQuery>();

            ASTRenameQuery::Table from;
            from.database = database_name;
            from.table = table_original_name;

            ASTRenameQuery::Table to;
            to.database = database_name;
            to.table = new_table_name;

            ASTRenameQuery::Element elem;
            elem.from = from;
            elem.to = to;

            rename->elements.emplace_back(elem);

            InterpreterRenameQuery(rename, global_context).execute();
    }
}


void StorageMaterializedView::rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & new_table_name)
{
    if (has_inner_table && tryGetTargetTable())
    {
        String new_target_table_name = generateInnerTableName(new_table_name);
        executeRenameQuery(global_context, target_database_name, target_table_name, new_target_table_name);
        target_table_name = new_target_table_name;
    }

    auto lock = global_context.getLock();

    global_context.removeDependencyUnsafe(
            DatabaseAndTableName(select_database_name, select_table_name),
            DatabaseAndTableName(database_name, table_name));

    table_name = new_table_name;

    global_context.addDependencyUnsafe(
            DatabaseAndTableName(select_database_name, select_table_name),
            DatabaseAndTableName(database_name, table_name));
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
