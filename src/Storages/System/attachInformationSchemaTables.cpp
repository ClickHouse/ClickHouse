#include <Databases/DatabaseOnDisk.h>
#include <Storages/System/attachInformationSchemaTables.h>
#include <Storages/System/attachSystemTablesImpl.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <incbin.h>

#include "config.h"

/// Embedded SQL definitions
INCBIN(resource_schemata_sql, SOURCE_DIR "/src/Storages/System/InformationSchema/schemata.sql");
INCBIN(resource_tables_sql, SOURCE_DIR "/src/Storages/System/InformationSchema/tables.sql");
INCBIN(resource_views_sql, SOURCE_DIR "/src/Storages/System/InformationSchema/views.sql");
INCBIN(resource_columns_sql, SOURCE_DIR "/src/Storages/System/InformationSchema/columns.sql");


namespace DB
{

/// View structures are taken from http://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt

static void createInformationSchemaView(ContextMutablePtr context, IDatabase & database, const String & view_name, std::string_view query)
{
    try
    {
        assert(database.getDatabaseName() == DatabaseCatalog::INFORMATION_SCHEMA ||
               database.getDatabaseName() == DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE);
        if (database.getEngineName() != "Memory")
            return;

        String metadata_resource_name = view_name + ".sql";
        if (query.empty())
            return;

        ParserCreateQuery parser;
        ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(),
                                "Attach query from embedded resource " + metadata_resource_name,
                                DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH);

        auto & ast_create = ast->as<ASTCreateQuery &>();
        assert(view_name == ast_create.getTable());
        ast_create.attach = false;
        ast_create.setDatabase(database.getDatabaseName());

        StoragePtr view = createTableFromAST(ast_create, database.getDatabaseName(),
                                             database.getTableDataPath(ast_create), context, true).second;
        database.createTable(context, ast_create.getTable(), view, ast);
        ASTPtr ast_upper = ast_create.clone();
        auto & ast_create_upper = ast_upper->as<ASTCreateQuery &>();
        ast_create_upper.setTable(Poco::toUpper(view_name));
        StoragePtr view_upper = createTableFromAST(ast_create_upper, database.getDatabaseName(),
                                             database.getTableDataPath(ast_create_upper), context, true).second;

        database.createTable(context, ast_create_upper.getTable(), view_upper, ast_upper);

    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void attachInformationSchema(ContextMutablePtr context, IDatabase & information_schema_database)
{
    createInformationSchemaView(context, information_schema_database, "schemata", std::string_view(reinterpret_cast<const char *>(gresource_schemata_sqlData), gresource_schemata_sqlSize));
    createInformationSchemaView(context, information_schema_database, "tables", std::string_view(reinterpret_cast<const char *>(gresource_tables_sqlData), gresource_tables_sqlSize));
    createInformationSchemaView(context, information_schema_database, "views", std::string_view(reinterpret_cast<const char *>(gresource_views_sqlData), gresource_views_sqlSize));
    createInformationSchemaView(context, information_schema_database, "columns", std::string_view(reinterpret_cast<const char *>(gresource_columns_sqlData), gresource_columns_sqlSize));
}

}
