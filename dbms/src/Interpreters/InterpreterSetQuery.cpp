#include <Parsers/ASTSetQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Common/typeid_cast.h>
#include <Core/FieldVisitors.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int READONLY;
}


BlockIO InterpreterSetQuery::execute()
{
    const ASTSetQuery & ast = typeid_cast<const ASTSetQuery &>(*query_ptr);

    checkAccess(ast);

    Context & target = context.getSessionContext();
    for (const auto & change : ast.changes)
        target.setSetting(change.name, change.value);

    return {};
}

void InterpreterSetQuery::checkAccess(const ASTSetQuery & ast)
{
    /** The `readonly` value is understood as follows:
      * 0 - everything allowed.
      * 1 - only read queries can be made; you can not change the settings.
      * 2 - You can only do read queries and you can change the settings, except for the `readonly` setting.
      */

    const Settings & settings = context.getSettingsRef();
    auto readonly = settings.limits.readonly;

    for (const auto & change : ast.changes)
    {
        String value;
        /// Setting isn't checked if value wasn't changed.
        if (!settings.tryGet(change.name, value) || applyVisitor(FieldVisitorToString(), change.value) != value)
        {
            if (readonly == 1)
                throw Exception("Cannot execute SET query in readonly mode", ErrorCodes::READONLY);

            if (readonly > 1 && change.name == "readonly")
                throw Exception("Cannot modify 'readonly' setting in readonly mode", ErrorCodes::READONLY);
        }
    }
}


void InterpreterSetQuery::executeForCurrentContext()
{
    const ASTSetQuery & ast = typeid_cast<const ASTSetQuery &>(*query_ptr);

    checkAccess(ast);

    for (const auto & change : ast.changes)
        context.setSetting(change.name, change.value);
}


}
