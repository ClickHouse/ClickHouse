#include <Interpreters/checkFunctionAccess.h>

#include <Access/AccessControl.h>
#include <Access/Common/AccessType.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>


namespace DB
{

void checkFunctionAccess(const ContextPtr & context, const String & function_name)
{
    if (!AccessControl::hasFunctionsRequiringGrant() || !context)
        return;

    const String registered_name = FunctionFactory::instance().resolveNameOrAlias(function_name);
    if (AccessControl::functionRequiresGrant(registered_name))
        context->checkAccess(AccessType::FUNCTION, registered_name);
}

}
