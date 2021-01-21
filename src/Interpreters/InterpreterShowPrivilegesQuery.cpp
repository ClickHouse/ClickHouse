#include <Interpreters/InterpreterShowPrivilegesQuery.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>


namespace DB
{
InterpreterShowPrivilegesQuery::InterpreterShowPrivilegesQuery(const ASTPtr & query_ptr_, Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
}


BlockIO InterpreterShowPrivilegesQuery::execute()
{
    context.applySettingChange({"is_reinterpreted_execution", true});
    return executeQuery("SELECT * FROM system.privileges", context, true);
}

}
