#include <Access/DefaultGrantsForNewUser.h>
#include <Access/RBACVersion.h>
#include <Interpreters/DatabaseCatalog.h>


namespace DB
{

const AccessRightsElements & getDefaultGrantsForNewUser(UInt64 rbac_version)
{
    checkRBACVersion(rbac_version);
    if (rbac_version < RBACVersion::NO_FULL_SYSTEM_DB_GRANTS)
    {
        static const AccessRightsElements full_system_db_access{{AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE}};
        return full_system_db_access;
    }
    else
    {
        static const AccessRightsElements empty;
        return empty;
    }
}

}
