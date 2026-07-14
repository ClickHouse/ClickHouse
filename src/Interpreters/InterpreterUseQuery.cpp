#include <Parsers/ASTUseQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Databases/IDatabase.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Access/Common/AccessFlags.h>
#include <Common/typeid_cast.h>
#include <base/find_symbols.h>


namespace DB
{

BlockIO InterpreterUseQuery::execute()
{
    const String & new_database = query_ptr->as<ASTUseQuery &>().getDatabase();
    const auto [database_name, table_prefix] = DatabaseCatalog::instance().splitTablePrefixFromDatabaseName(new_database);
    getContext()->checkAccess(AccessType::SHOW_DATABASES, database_name);

    /// setCurrentDatabase validates that the namespace exists
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
