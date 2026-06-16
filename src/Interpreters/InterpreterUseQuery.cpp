#include <Parsers/ASTUseQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Access/Common/AccessFlags.h>
#include <Common/typeid_cast.h>


namespace DB
{

BlockIO InterpreterUseQuery::execute()
{
    const String & new_database = query_ptr->as<ASTUseQuery &>().getDatabase();
    getContext()->checkAccess(AccessType::SHOW_DATABASES, new_database);
    auto session_context = getContext()->getSessionContext();
    session_context->setCurrentDatabase(new_database);
    /// Keep the `database` setting in sync with the session's current database. `database` is a real
    /// setting that `executeQuery` applies as the documented equivalent of `USE` on every statement;
    /// without this, an earlier `SET database = ...` would be re-applied on the next query and
    /// silently override the database just selected by this `USE`. A query's own
    /// `SETTINGS database = ...` is applied later and still takes precedence.
    session_context->setSetting("database", new_database);
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
