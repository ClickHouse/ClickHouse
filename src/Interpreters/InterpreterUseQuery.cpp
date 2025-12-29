#include <Parsers/ASTUseQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Access/Common/AccessFlags.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_DATABASE;
}

BlockIO InterpreterUseQuery::execute()
{
    const String & new_target = query_ptr->as<ASTUseQuery &>().getDatabase();
    auto session_context = getContext()->getSessionContext();
    auto & catalog = DatabaseCatalog::instance();

    /// Try to use as a full database name first
    if (catalog.isDatabaseExist(new_target))
    {
        getContext()->checkAccess(AccessType::SHOW_DATABASES, new_target);
        session_context->setCurrentDatabase(new_target);
        session_context->clearCurrentTablePrefix();
        return {};
    }

    /// If not an exact database, try to interpret as "database.table_prefix" for DataLakeCatalog
    size_t dot_pos = new_target.find('.');
    if (dot_pos != String::npos)
    {
        String db_part = new_target.substr(0, dot_pos);
        String prefix_part = new_target.substr(dot_pos + 1);

        if (catalog.isDatabaseExist(db_part) && catalog.isDatalakeCatalog(db_part))
        {
            getContext()->checkAccess(AccessType::SHOW_DATABASES, db_part);
            session_context->setCurrentDatabase(db_part);
            session_context->setCurrentTablePrefix(prefix_part);
            return {};
        }
    }

    /// Neither exact database nor db.prefix interpretation worked
    throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Unknown database '{}'", new_target);
}

void registerInterpreterUseQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterUseQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterUseQuery", create_fn);
}

}
