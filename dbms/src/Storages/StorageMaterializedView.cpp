#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTIdentifier.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>

#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageFactory.h>

#include <Storages/VirtualColumnFactory.h>

#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}


static void extractDependentTable(const ASTSelectQuery & query, String & select_database_name, String & select_table_name)
{
    auto query_table = query.table();

    if (!query_table)
        return;

    if (auto ast_id = typeid_cast<const ASTIdentifier *>(query_table.get()))
    {
        auto query_database = query.database();

        if (!query_database)
            throw Exception("Logical error while creating StorageMaterializedView."
                " Could not retrieve database name from select query.",
                DB::ErrorCodes::LOGICAL_ERROR);

        select_database_name = typeid_cast<const ASTIdentifier &>(*query_database).name;
        select_table_name = ast_id->name;
    }
    else if (auto ast_select = typeid_cast<const ASTSelectQuery *>(query_table.get()))
    {
        extractDependentTable(*ast_select, select_database_name, select_table_name);
    }
    else
        throw Exception("Logical error while creating StorageMaterializedView."
            " Could not retrieve table name from select query.",
            DB::ErrorCodes::LOGICAL_ERROR);
}


StorageMaterializedView::StorageMaterializedView(
    const String & table_name_,
    const String & database_name_,
    Context & local_context,
    const ASTCreateQuery & query,
    const NamesAndTypesList & columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    bool attach_)
    : IStorage{columns_, materialized_columns_, alias_columns_, column_defaults_}, table_name(table_name_),
    database_name(database_name_), global_context(local_context.getGlobalContext())
{
    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    if (!query.storage && query.to_table.empty())
        throw Exception(
            "You must specify where to save results of a MaterializedView query: either ENGINE or an existing table in a TO clause",
            ErrorCodes::INCORRECT_QUERY);

    extractDependentTable(*query.select, select_database_name, select_table_name);

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

    inner_query = query.select->ptr();

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

void StorageMaterializedView::drop()
{
    global_context.removeDependency(
        DatabaseAndTableName(select_database_name, select_table_name),
        DatabaseAndTableName(database_name, table_name));

    if (has_inner_table && global_context.tryGetTable(target_database_name, target_table_name))
    {
        /// We create and execute `drop` query for internal table.
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = target_database_name;
        drop_query->table = target_table_name;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, global_context);
        drop_interpreter.execute();
    }
}

bool StorageMaterializedView::optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context)
{
    return getTargetTable()->optimize(query, partition, final, deduplicate, context);
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

bool StorageMaterializedView::checkTableCanBeDropped() const
{
    /// Don't drop the target table if it was created manually via 'TO inner_table' statement
    return has_inner_table ? getTargetTable()->checkTableCanBeDropped() : true;
}


void registerStorageMaterializedView(StorageFactory & factory)
{
    factory.registerStorage("MaterializedView", [](const StorageFactory::Arguments & args)
    {
        /// Pass local_context here to convey setting for inner table
        return StorageMaterializedView::create(
            args.table_name, args.database_name, args.local_context, args.query,
            args.columns, args.materialized_columns, args.alias_columns, args.column_defaults,
            args.attach);
    });
}

}
