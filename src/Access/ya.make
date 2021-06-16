# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
LIBRARY()

PEERDIR(
    clickhouse/src/Common
)

SRCS(
    AccessControlManager.cpp
    AccessRights.cpp
    AccessRightsElement.cpp
    AllowedClientHosts.cpp
    Authentication.cpp
    ContextAccess.cpp
    DiskAccessStorage.cpp
    EnabledQuota.cpp
    EnabledRoles.cpp
    EnabledRolesInfo.cpp
    EnabledRowPolicies.cpp
    EnabledSettings.cpp
    ExternalAuthenticators.cpp
    GrantedRoles.cpp
    IAccessEntity.cpp
    IAccessStorage.cpp
    LDAPClient.cpp
    MemoryAccessStorage.cpp
    MultipleAccessStorage.cpp
    QuotaCache.cpp
    Quota.cpp
    QuotaUsage.cpp
    RoleCache.cpp
    Role.cpp
    RolesOrUsersSet.cpp
    RowPolicyCache.cpp
    RowPolicy.cpp
    SettingsConstraints.cpp
    SettingsProfile.cpp
    SettingsProfileElement.cpp
    SettingsProfilesCache.cpp
    User.cpp
    UsersConfigAccessStorage.cpp

)

END()
