#include <Access/CachedAccessChecking.h>
#include <Access/ContextAccess.h>


namespace DB
{
CachedAccessChecking::CachedAccessChecking(const std::shared_ptr<const ContextAccess> & access_, AccessFlags access_flags_)
    : CachedAccessChecking(access_, AccessRightsElement{access_flags_})
{
}

CachedAccessChecking::CachedAccessChecking(const std::shared_ptr<const ContextAccess> & access_, const AccessRightsElement & element_)
    : access(access_), element(element_)
{
}

CachedAccessChecking::~CachedAccessChecking() = default;

bool CachedAccessChecking::checkAccess(bool throw_if_denied)
{
    if (checked)
        return result;
    if (throw_if_denied)
    {
        try
        {
            access->checkAccess(element);
            result = true;
        }
        catch (...)
        {
            result = false;
            throw;
        }
    }
    else
    {
        result = access->isGranted(element);
    }
    checked = true;
    return result;
}

}
