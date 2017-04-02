#include <Parsers/ASTSetQuery.h>
#include <Interpreters/InterpreterSetQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int READONLY;
}


BlockIO InterpreterSetQuery::execute()
{
    ASTSetQuery & ast = typeid_cast<ASTSetQuery &>(*query_ptr);
    Context & target = ast.global ? context.getGlobalContext() : context.getSessionContext();
    executeImpl(ast, target);
    return {};
}


void InterpreterSetQuery::executeForCurrentContext()
{
    ASTSetQuery & ast = typeid_cast<ASTSetQuery &>(*query_ptr);
    executeImpl(ast, context);
}


void InterpreterSetQuery::executeImpl(ASTSetQuery & ast, Context & target)
{
    /** The `readonly` value is understood as follows:
      * 0 - everything allowed.
      * 1 - only read queries can be made; you can not change the settings.
      * 2 - You can only do read queries and you can change the settings, except for the `readonly` setting.
      */

    if (context.getSettingsRef().limits.readonly == 1)
        throw Exception("Cannot execute SET query in readonly mode", ErrorCodes::READONLY);

    if (context.getSettingsRef().limits.readonly > 1)
        for (ASTSetQuery::Changes::const_iterator it = ast.changes.begin(); it != ast.changes.end(); ++it)
            if (it->name == "readonly")
                throw Exception("Cannot modify 'readonly' setting in readonly mode", ErrorCodes::READONLY);

    for (ASTSetQuery::Changes::const_iterator it = ast.changes.begin(); it != ast.changes.end(); ++it)
        target.setSetting(it->name, it->value);
}


}
