#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTIdentifier.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>

#include <Storages/StorageMaterializedView.h>
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
    Context & context_,
    ASTPtr & query_,
    NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    bool attach_)
    : IStorage{materialized_columns_, alias_columns_, column_defaults_}, table_name(table_name_),
    database_name(database_name_), context(context_), columns(columns_)
{
    ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*query_);

    if (!create.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    if (!create.inner_storage)
        throw Exception("ENGINE of MaterializedView should be specified explicitly", ErrorCodes::INCORRECT_QUERY);

    ASTSelectQuery & select = typeid_cast<ASTSelectQuery &>(*create.select);

    /// If the internal query does not specify a database, retrieve it from the context and write it to the query.
    select.setDatabaseIfNeeded(database_name);

    extractDependentTable(select, select_database_name, select_table_name);

    if (!select_table_name.empty())
        context.getGlobalContext().addDependency(
            DatabaseAndTableName(select_database_name, select_table_name),
            DatabaseAndTableName(database_name, table_name));

    String inner_table_name = getInnerTableName();
    inner_query = create.select;

    /// If there is an ATTACH request, then the internal table must already be connected.
    if (!attach_)
    {
        /// We will create a query to create an internal table.
        auto manual_create_query = std::make_shared<ASTCreateQuery>();
        manual_create_query->database = database_name;
        manual_create_query->table = inner_table_name;
        manual_create_query->columns = create.columns;
        manual_create_query->children.push_back(manual_create_query->columns);
        manual_create_query->storage = create.inner_storage;
        manual_create_query->children.push_back(manual_create_query->storage);

        /// Execute the query.
        try
        {
            InterpreterCreateQuery create_interpreter(manual_create_query, context);
            create_interpreter.execute();
        }
        catch (...)
        {
            /// In case of any error we should remove dependency to the view.
            if (!select_table_name.empty())
                context.getGlobalContext().removeDependency(
                    DatabaseAndTableName(select_database_name, select_table_name),
                    DatabaseAndTableName(database_name, table_name));

            throw;
        }
    }
}

NameAndTypePair StorageMaterializedView::getColumn(const String & column_name) const
{
    return getInnerTable()->getColumn(column_name);
}

bool StorageMaterializedView::hasColumn(const String & column_name) const
{
    return getInnerTable()->hasColumn(column_name);
}

BlockInputStreams StorageMaterializedView::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    return getInnerTable()->read(column_names, query_info, context, processed_stage, max_block_size, num_streams);
}

BlockOutputStreamPtr StorageMaterializedView::write(const ASTPtr & query, const Settings & settings)
{
    return getInnerTable()->write(query, settings);
}

void StorageMaterializedView::drop()
{
    context.getGlobalContext().removeDependency(
        DatabaseAndTableName(select_database_name, select_table_name),
        DatabaseAndTableName(database_name, table_name));

    auto inner_table_name = getInnerTableName();

    if (context.tryGetTable(database_name, inner_table_name))
    {
        /// We create and execute `drop` query for internal table.
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = database_name;
        drop_query->table = inner_table_name;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, context);
        drop_interpreter.execute();
    }
}

bool StorageMaterializedView::optimize(const ASTPtr & query, const String & partition, bool final, bool deduplicate, const Settings & settings)
{
    return getInnerTable()->optimize(query, partition, final, deduplicate, settings);
}

StoragePtr StorageMaterializedView::getInnerTable() const
{
    return context.getTable(database_name, getInnerTableName());
}


}
