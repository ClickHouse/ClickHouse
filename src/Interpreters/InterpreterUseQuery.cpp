#include <Parsers/ASTUseQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Access/AccessFlags.h>
#include <Common/typeid_cast.h>


namespace DB
{

BlockIO InterpreterUseQuery::execute()
{
    const String & new_database = query_ptr->as<ASTUseQuery &>().database;
    context.checkAccess(AccessType::SHOW_DATABASES, new_database);
    context.getSessionContext().setCurrentDatabase(new_database);
    return {};
}

}
