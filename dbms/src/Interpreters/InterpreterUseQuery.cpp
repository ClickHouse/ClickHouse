#include <Parsers/ASTUseQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Common/typeid_cast.h>


namespace DB
{

BlockIO InterpreterUseQuery::execute()
{
    if (const auto & use_query = query_ptr->as<ASTUseQuery>())
    {
        const auto & database_identifier = use_query->named.get(ASTUseQuery::DATABASE);

        const String & new_database = getIdentifierName(database_identifier);
        context.getSessionContext().setCurrentDatabase(new_database);
        return {};
    }
}

}
