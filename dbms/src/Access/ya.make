LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
)

SRCS(
    AllowedClientHosts.cpp
    AccessControlManager.cpp
    AccessRights.cpp
    AccessRightsContext.cpp
    AccessRightsContextFactory.cpp
    AccessRightsElement.cpp
    Authentication.cpp
    CurrentRolesInfo.cpp
    DiskAccessStorage.cpp
    GenericRoleSet.cpp
    IAccessEntity.cpp
    IAccessStorage.cpp
    MemoryAccessStorage.cpp
    MultipleAccessStorage.cpp
    Quota.cpp
    QuotaContext.cpp
    QuotaContextFactory.cpp
    Role.cpp
    RoleContext.cpp
    RoleContextFactory.cpp
    RowPolicy.cpp
    RowPolicyContext.cpp
    RowPolicyContextFactory.cpp
    User.cpp
    UsersConfigAccessStorage.cpp
)

END()
