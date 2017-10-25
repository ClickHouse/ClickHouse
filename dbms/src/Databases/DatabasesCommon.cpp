#include <sstream>

#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Storages/StorageFactory.h>
#include <Databases/DatabasesCommon.h>


namespace DB
{


String getTableDefinitionFromCreateQuery(const ASTPtr & query)
{
    ASTPtr query_clone = query->clone();
    ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*query_clone.get());

    /// We remove everything that is not needed for ATTACH from the query.
    create.attach = true;
    create.database.clear();
    create.if_not_exists = false;
    create.is_populate = false;

    String engine = create.storage->engine->name;

    /// For engine VIEW it is necessary to save the SELECT query itself, for the rest - on the contrary
    if (engine != "View" && engine != "MaterializedView")
        create.select = nullptr;

    /// For "MATERIALIZED VIEW x TO y" it's necessary to save destination table
    if (engine != "MaterializedView" || create.inner_storage)
    {
        create.as_database.clear();
        create.as_table.clear();
    }

    std::ostringstream statement_stream;
    formatAST(create, statement_stream, 0, false);
    statement_stream << '\n';
    return statement_stream.str();
}


std::pair<String, StoragePtr> createTableFromDefinition(
    const String & definition,
    const String & database_name,
    const String & database_data_path,
    Context & context,
    bool has_force_restore_data_flag,
    const String & description_for_error_message)
{
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, definition.data(), definition.data() + definition.size(), description_for_error_message);

    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
    ast_create_query.attach = true;
    ast_create_query.database = database_name;

    /// We do not directly use `InterpreterCreateQuery::execute`, because
    /// - the database has not been created yet;
    /// - the code is simpler, since the query is already brought to a suitable form.
    if (!ast_create_query.columns)
        throw Exception("Missing definition of columns.", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

    InterpreterCreateQuery::ColumnsInfo columns_info = InterpreterCreateQuery::getColumnsInfo(*ast_create_query.columns, context);

    String storage_name;

    if (ast_create_query.is_view)
        storage_name = "View";
    else if (ast_create_query.is_materialized_view)
        storage_name = "MaterializedView";
    else
    {
        if (!ast_create_query.storage)
            throw Exception("Missing ENGINE definition.", ErrorCodes::ENGINE_REQUIRED);
        storage_name = ast_create_query.storage->engine->name;
    }

    return
    {
        ast_create_query.table,
        StorageFactory::instance().get(
            storage_name, database_data_path, ast_create_query.table, database_name, context,
            context.getGlobalContext(), ast_create_query, columns_info.columns,
            columns_info.materialized_columns, columns_info.alias_columns, columns_info.column_defaults,
            true, has_force_restore_data_flag)
    };
}

}
