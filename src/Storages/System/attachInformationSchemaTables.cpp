#include <Databases/DatabaseOnDisk.h>
#include <Storages/System/attachInformationSchemaTables.h>
#include <Storages/System/attachSystemTablesImpl.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <span>


namespace DB
{

/// Include views in information_schema.

constexpr unsigned char schemata[] = {
#embed "information_schema/schemata.sql"
};

constexpr unsigned char tables[] = {
#embed "information_schema/tables.sql"
};

constexpr unsigned char views[] = {
#embed "information_schema/views.sql"
};

constexpr unsigned char columns[] = {
#embed "information_schema/columns.sql"
};

constexpr unsigned char key_column_usage[] = {
#embed "information_schema/key_column_usage.sql"
};

constexpr unsigned char referential_constraints[] = {
#embed "information_schema/referential_constraints.sql"
};

constexpr unsigned char statistics[] = {
#embed "information_schema/statistics.sql"
};

constexpr unsigned char engines[] = {
#embed "information_schema/engines.sql" /// MySQL-specific
};

constexpr unsigned char character_sets[] = {
#embed "information_schema/character_sets.sql"
};

constexpr unsigned char collations[] = {
#embed "information_schema/collations.sql"
};

/// View structures are taken from http://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt

static void createInformationSchemaView(ContextMutablePtr context, IDatabase & database, const String & view_name, std::span<const unsigned char> query_)
{
    try
    {
        assert(database.getDatabaseName() == DatabaseCatalog::INFORMATION_SCHEMA ||
               database.getDatabaseName() == DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE);
        if (database.getEngineName() != "Memory")
            return;

        std::string_view query(reinterpret_cast<const char *>(query_.data()), query_.size());

        String metadata_resource_name = view_name + ".sql";
        if (query.empty())
            return;

        ParserCreateQuery parser;
        ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(),
                                "Attach query from embedded resource " + metadata_resource_name,
                                DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

        auto & ast_create = ast->as<ASTCreateQuery &>();
        assert(view_name == ast_create.getTable());
        ast_create.attach = false;
        ast_create.setDatabase(database.getDatabaseName());

        StoragePtr view = createTableFromAST(ast_create, database.getDatabaseName(),
                                             database.getTableDataPath(ast_create), context, LoadingStrictnessLevel::FORCE_RESTORE).second;
        database.createTable(context, ast_create.getTable(), view, ast);
        ASTPtr ast_upper = ast_create.clone();
        auto & ast_create_upper = ast_upper->as<ASTCreateQuery &>();
        ast_create_upper.setTable(Poco::toUpper(view_name));
        StoragePtr view_upper = createTableFromAST(ast_create_upper, database.getDatabaseName(),
                                             database.getTableDataPath(ast_create_upper), context, LoadingStrictnessLevel::FORCE_RESTORE).second;

        database.createTable(context, ast_create_upper.getTable(), view_upper, ast_upper);

    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void attachInformationSchema(ContextMutablePtr context, IDatabase & information_schema_database)
{
    createInformationSchemaView(context, information_schema_database, "schemata", schemata);
    createInformationSchemaView(context, information_schema_database, "tables", tables);
    createInformationSchemaView(context, information_schema_database, "views", views);
    createInformationSchemaView(context, information_schema_database, "columns", columns);
    createInformationSchemaView(context, information_schema_database, "key_column_usage", key_column_usage);
    createInformationSchemaView(context, information_schema_database, "referential_constraints", referential_constraints);
    createInformationSchemaView(context, information_schema_database, "statistics", statistics);
    createInformationSchemaView(context, information_schema_database, "engines", engines);
    createInformationSchemaView(context, information_schema_database, "character_sets", character_sets);
    createInformationSchemaView(context, information_schema_database, "collations", collations);
}

}
