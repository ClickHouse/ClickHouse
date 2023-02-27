#include <Databases/DatabaseOnDisk.h>
#include <Storages/System/attachInformationSchemaTables.h>
#include <Storages/System/attachSystemTablesImpl.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/getResource.h>

namespace DB
{

/// View structures are taken from http://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt

static void createInformationSchemaView(ContextMutablePtr context, IDatabase & database, const String & view_name)
{
    try
    {
        assert(database.getDatabaseName() == DatabaseCatalog::INFORMATION_SCHEMA ||
               database.getDatabaseName() == DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE);
        if (database.getEngineName() != "Memory")
            return;
        bool is_uppercase = database.getDatabaseName() == DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE;

        String metadata_resource_name = view_name + ".sql";
        auto attach_query = getResource(metadata_resource_name);
        if (attach_query.empty())
            return;

        ParserCreateQuery parser;
        ASTPtr ast = parseQuery(parser, attach_query.data(), attach_query.data() + attach_query.size(),
                                "Attach query from embedded resource " + metadata_resource_name,
                                DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH);

        auto & ast_create = ast->as<ASTCreateQuery &>();
        assert(view_name == ast_create.getTable());
        ast_create.attach = false;
        ast_create.setDatabase(database.getDatabaseName());
        if (is_uppercase)
            ast_create.setTable(Poco::toUpper(view_name));

        StoragePtr view = createTableFromAST(ast_create, database.getDatabaseName(),
                                             database.getTableDataPath(ast_create), context, true).second;

        database.createTable(context, ast_create.getTable(), view, ast);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void attachInformationSchema(ContextMutablePtr context, IDatabase & information_schema_database)
{
    createInformationSchemaView(context, information_schema_database, "schemata");
    createInformationSchemaView(context, information_schema_database, "tables");
    createInformationSchemaView(context, information_schema_database, "views");
    createInformationSchemaView(context, information_schema_database, "columns");
}

}
