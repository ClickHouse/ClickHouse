#include <Databases/DatabaseOnDisk.h>
#include <Storages/System/attachInformationSchemaTables.h>
#include <Storages/System/attachSystemTablesImpl.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <common/getResource.h>


namespace DB
{

/// View structures are taken from http://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt

static void attachInformationSchemaView(ContextMutablePtr context, IDatabase & database, const String & view_name)
{
    String metadata_resource_name = view_name + ".sql";
    auto attach_query = getResource(metadata_resource_name);
    if (attach_query.empty())
        return;

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, attach_query.data(), attach_query.data() + attach_query.size(),
                            "Attach query from embedded resource " + metadata_resource_name,
                            DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    auto & ast_create = ast->as<ASTCreateQuery &>();
    StoragePtr view = createTableFromAST(ast_create, database.getDatabaseName(),
                                         database.getTableDataPath(ast_create), context, true).second;

    database.attachTable(view_name, view);
}

void attachInformationSchema(ContextMutablePtr context, IDatabase & information_schema_database)
{
    attachInformationSchemaView(context, information_schema_database, "schemata");
    attachInformationSchemaView(context, information_schema_database, "tables");
    attachInformationSchemaView(context, information_schema_database, "views");
    attachInformationSchemaView(context, information_schema_database, "columns");
}

}
