#include <Parsers/ASTUseQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Common/typeid_cast.h>


namespace DB
{

BlockIO InterpreterUseQuery::execute()
{
    const String & new_database = typeid_cast<const ASTUseQuery &>(*query_ptr).database;
    context.getSessionContext().setCurrentDatabase(new_database);
    return {};
}

}
