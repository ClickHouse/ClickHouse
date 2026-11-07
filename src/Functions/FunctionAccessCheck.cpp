#include <Functions/FunctionAccessCheck.h>
#include <Interpreters/Context.h>


namespace DB
{

FunctionAccessCheck::FunctionAccessCheck(const ContextPtr & context)
    : access(ContextAccess::fromContext(context))
    , privileges_info(context->getQueryPrivilegesInfoPtr())
{
}

}
