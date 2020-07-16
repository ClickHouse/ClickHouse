OPTION(ENABLE_KRB5 "Enable krb5" ${ENABLE_LIBRARIES})
if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/krb5/README")
    message (WARNING "submodule contrib/krb5 is missing. to fix try run: \n git submodule update --init --recursive")
    set (ENABLE_KRB5 0)
endif ()

if (ENABLE_KRB5)

    set (USE_KRB5 1)
    set (KRB5_LIBRARY krb5)

    set (KRB5_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/krb5/include")

    list (APPEND KRB5_INCLUDE_DIR
        "${ClickHouse_SOURCE_DIR}/contrib/krb5/src/include"
        )

endif ()

message (STATUS "Using krb5=${USE_KRB5}: ${KRB5_INCLUDE_DIR} : ${KRB5_LIBRARY}")
