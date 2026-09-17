#include <Access/UDTUsageAccess.h>

#include <Interpreters/Context.h>

namespace DB::UDT
{

void checkUsageAccess(const ContextPtr & context, std::span<const AccessTarget> targets)
{
    context->checkAccess(makeUsageAccessElements(targets));
}

}
