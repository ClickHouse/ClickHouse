LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
)

SRCS(
    AccessControlManager.cpp
    AccessRights.cpp
    AccessRightsElement.cpp
    AllowedClientHosts.cpp
    Authentication.cpp
    DiskAccessStorage.cpp
    IAccessEntity.cpp
    IAccessStorage.cpp
    MemoryAccessStorage.cpp
    MultipleAccessStorage.cpp
    Quota.cpp
    Role.cpp
    RowPolicy.cpp
    SettingsConstraints.cpp
    User.cpp
    UsersConfigAccessStorage.cpp
)

END()
