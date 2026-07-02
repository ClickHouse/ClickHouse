#include <Parsers/ASTUseQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Databases/DataLake/DataLakeConstants.h>
#include <Databases/IDatabase.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Access/Common/AccessFlags.h>
#include <Common/typeid_cast.h>


namespace DB
{

BlockIO InterpreterUseQuery::execute()
{
    const String & new_database = query_ptr->as<ASTUseQuery &>().getDatabase();
    auto & catalog = DatabaseCatalog::instance();

    String database_name = new_database;
    String table_prefix;

    /// `USE catalog.namespace` for a DataLakeCatalog database: the part before the first dot
    /// is the database, the rest becomes a prefix applied to unqualified table names.
    /// An existing database with the full (dotted) name takes priority.
    if (!catalog.isDatabaseExist(new_database))
    {
        if (auto dot_pos = new_database.find('.'); dot_pos != String::npos)
        {
            auto database = catalog.tryGetDatabase(new_database.substr(0, dot_pos));
            /// Namespaces are specific to data lake catalogs, so do not treat the suffix
            /// as a prefix for other database engines.
            if (database && database->getEngineName() == DataLake::DATABASE_ENGINE_NAME)
            {
                database_name = database->getDatabaseName();
                table_prefix = new_database.substr(dot_pos + 1);
            }
        }
    }

    getContext()->checkAccess(AccessType::SHOW_DATABASES, database_name);
    getContext()->getSessionContext()->setCurrentDatabase(database_name, table_prefix);

    return {};
}

void registerInterpreterUseQuery(InterpreterFactory & factory);
void registerInterpreterUseQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterUseQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterUseQuery", create_fn);
}

}
