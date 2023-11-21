#include <Interpreters/InterpreterShowSettingQuery.h>

#include <Common/escapeString.h>
#include <Parsers/ASTShowSettingQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>


namespace DB
{


InterpreterShowSettingQuery::InterpreterShowSettingQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_)
    , query_ptr(query_ptr_)
{
}


String InterpreterShowSettingQuery::getRewrittenQuery()
{
    const auto & query = query_ptr->as<ASTShowSettingQuery &>();
    return fmt::format(R"(SELECT value FROM system.settings WHERE name = '{0}')", escapeString(query.getSettingName()));
}


BlockIO InterpreterShowSettingQuery::execute()
{
    return executeQuery(getRewrittenQuery(), getContext(), QueryFlags{ .internal = true }).second;
}


}
