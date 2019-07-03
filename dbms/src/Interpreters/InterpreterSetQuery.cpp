#include <Parsers/ASTSetQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>

namespace DB
{


BlockIO InterpreterSetQuery::execute()
{
    const auto & ast = query_ptr->as<ASTSetQuery &>();
    context.checkSettingsConstraints(ast.changes);
    context.getSessionContext().applySettingsChanges(ast.changes);
    return {};
}


void InterpreterSetQuery::executeForCurrentContext()
{
    const auto & ast = query_ptr->as<ASTSetQuery &>();
    context.checkSettingsConstraints(ast.changes);
    context.applySettingsChanges(ast.changes);
}

}
