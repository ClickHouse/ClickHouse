#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{


BlockIO InterpreterSetQuery::execute()
{
    const auto & ast = query_ptr->as<ASTSetQuery &>();
    getContext()->checkSettingsConstraints(ast.changes);
    auto session_context = getContext()->getSessionContext();
    session_context->applySettingsChanges(ast.changes);
    session_context->addQueryParameters(ast.query_parameters);
    return {};
}


void InterpreterSetQuery::executeForCurrentContext()
{
    const auto & ast = query_ptr->as<ASTSetQuery &>();
    getContext()->checkSettingsConstraints(ast.changes);
    getContext()->applySettingsChanges(ast.changes);
}

}
