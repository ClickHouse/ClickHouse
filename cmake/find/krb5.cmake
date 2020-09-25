OPTION(ENABLE_KRB5 "Enable krb5" ${ENABLE_LIBRARIES})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/krb5/README")
    message (WARNING "submodule contrib/krb5 is missing. to fix try run: \n git submodule update --init --recursive")
    set (ENABLE_KRB5 0)
endif ()

if (NOT CMAKE_SYSTEM_NAME MATCHES "Linux" AND NOT (CMAKE_SYSTEM_NAME MATCHES "Darwin" AND NOT CMAKE_CROSSCOMPILING))
    message (WARNING "krb5 disabled in non-Linux and non-native-Darwin environments")
    set (ENABLE_KRB5 0)
endif ()

if (ENABLE_KRB5)

    set (USE_KRB5 1)
    set (KRB5_LIBRARY krb5)

    set (KRB5_INCLUDE_DIR
        "${ClickHouse_SOURCE_DIR}/contrib/krb5/src/include"
        "${ClickHouse_BINARY_DIR}/contrib/krb5-cmake/include"
    )

endif ()

message (STATUS "Using krb5=${USE_KRB5}: ${KRB5_INCLUDE_DIR} : ${KRB5_LIBRARY}")
