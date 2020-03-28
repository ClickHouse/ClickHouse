LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
)

SRCS(
    AccessControlManager.cpp
    AccessRightsContext.cpp
    AccessRightsContextFactory.cpp
    AccessRights.cpp
    AccessRightsElement.cpp
    AllowedClientHosts.cpp
    Authentication.cpp
    CurrentRolesInfo.cpp
    DiskAccessStorage.cpp
    GenericRoleSet.cpp
    IAccessEntity.cpp
    IAccessStorage.cpp
    MemoryAccessStorage.cpp
    MultipleAccessStorage.cpp
    QuotaContext.cpp
    QuotaContextFactory.cpp
    Quota.cpp
    RoleContext.cpp
    RoleContextFactory.cpp
    Role.cpp
    RowPolicyContext.cpp
    RowPolicyContextFactory.cpp
    RowPolicy.cpp
    SettingsConstraints.cpp
    User.cpp
    UsersConfigAccessStorage.cpp
)

END()
