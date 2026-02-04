#include <Parsers/ASTUseQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
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
    auto session_context = getContext()->getSessionContext();

    /// Try to use as a full database name first
    String db_part = new_database;
    String prefix_part;
    /// If there is no original db, try to find it in the format of db.namespace
    if (!catalog.isDatabaseExist(new_database))
    {
        size_t dot_pos = new_database.find('.');
        if (dot_pos != String::npos)
        {
            const String possible_db_part = new_database.substr(0, dot_pos);
            if (catalog.isDatabaseExist(possible_db_part) && catalog.isDatalakeCatalog(possible_db_part))
            {
                db_part = possible_db_part;
                prefix_part = new_database.substr(dot_pos + 1);
            }
        }
    }

    getContext()->checkAccess(AccessType::SHOW_DATABASES, db_part); // will throw exception if db not found
    session_context->setCurrentDatabase(db_part, prefix_part);

    return {};
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
