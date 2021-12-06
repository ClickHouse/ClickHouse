#include <Interpreters/Access/InterpreterShowPrivilegesQuery.h>
#include <Interpreters/executeQuery.h>


namespace DB
{
InterpreterShowPrivilegesQuery::InterpreterShowPrivilegesQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : query_ptr(query_ptr_), context(context_)
{
}


BlockIO InterpreterShowPrivilegesQuery::execute()
{
    return executeQuery("SELECT * FROM system.privileges", context, true);
}

}
