#pragma once

#include <Access/Common/AccessRightsElement.h>
#include <memory>


namespace DB
{
class ContextAccess;

/// Checks if the current user has a specified access type granted,
/// and if it's checked another time later, it will just return the first result.
class CachedAccessChecking
{
public:
    CachedAccessChecking(const std::shared_ptr<const ContextAccess> & access_, AccessFlags access_flags_);
    CachedAccessChecking(const std::shared_ptr<const ContextAccess> & access_, const AccessRightsElement & element_);
    ~CachedAccessChecking();

    bool checkAccess(bool throw_if_denied = true);

private:
    const std::shared_ptr<const ContextAccess> access;
    const AccessRightsElement element;
    bool checked = false;
    bool result = false;
};

}
