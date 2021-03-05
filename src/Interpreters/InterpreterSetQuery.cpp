#include <Parsers/ASTSetQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>

namespace DB
{


BlockIO InterpreterSetQuery::execute()
{
    const auto & ast = query_ptr->as<ASTSetQuery &>();

    getContext()->checkSettingsConstraints(ast.changes);
    // Here settings are pushed to the session context and are not visible in the query context
    getContext()->getSessionContext()->applySettingsChanges(ast.changes);
    // Make setting changes also available to the query context.
    getContext()->applySettingsChanges(ast.changes);

    return {};
}


void InterpreterSetQuery::executeForCurrentContext()
{
    const auto & ast = query_ptr->as<ASTSetQuery &>();
    getContext()->checkSettingsConstraints(ast.changes);
    getContext()->applySettingsChanges(ast.changes);
}

}
