option (ENABLE_LDAP "Enable LDAP" ${ENABLE_LIBRARIES})

if (ENABLE_LDAP)
    option (USE_INTERNAL_LDAP_LIBRARY "Set to FALSE to use system *LDAP library instead of bundled" ${NOT_UNBUNDLED})

    if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/openldap/README")
        if (USE_INTERNAL_LDAP_LIBRARY)
            message (WARNING "Submodule contrib/openldap is missing. To fix try running:\n git submodule update --init --recursive")
        endif ()

        set (USE_INTERNAL_LDAP_LIBRARY 0)
        set (MISSING_INTERNAL_LDAP_LIBRARY 1)
    endif ()

    set (OPENLDAP_USE_STATIC_LIBS ${USE_STATIC_LIBRARIES})
    set (OPENLDAP_USE_REENTRANT_LIBS 1)

    if (NOT USE_INTERNAL_LDAP_LIBRARY)
        if (APPLE AND NOT OPENLDAP_ROOT_DIR)
            set (OPENLDAP_ROOT_DIR "/usr/local/opt/openldap")
        endif ()

        find_package (OpenLDAP)
    endif ()

    if (NOT OPENLDAP_FOUND AND NOT MISSING_INTERNAL_LDAP_LIBRARY)
        if (
            ( "${CMAKE_SYSTEM_NAME}" STREQUAL "Linux"   AND "${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "x86_64" ) OR
            ( "${CMAKE_SYSTEM_NAME}" STREQUAL "FreeBSD" AND "${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "amd64"  ) OR
            ( "${CMAKE_SYSTEM_NAME}" STREQUAL "Darwin"  AND "${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "x86_64" )
        )
            set (_ldap_supported_platform TRUE)
        endif ()

        if (NOT _ldap_supported_platform)
            message (WARNING "LDAP support using the bundled library is not implemented for ${CMAKE_SYSTEM_NAME} ${CMAKE_SYSTEM_PROCESSOR} platform.")
        elseif (NOT USE_SSL)
            message (WARNING "LDAP support using the bundled library is not possible if SSL is not used.")
        else ()
            set (USE_INTERNAL_LDAP_LIBRARY 1)
            set (OPENLDAP_ROOT_DIR "${ClickHouse_SOURCE_DIR}/contrib/openldap")
            set (OPENLDAP_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/openldap/include")
            # Below, 'ldap'/'ldap_r' and 'lber' will be resolved to
            # the targets defined in contrib/openldap-cmake/CMakeLists.txt
            if (OPENLDAP_USE_REENTRANT_LIBS)
                set (OPENLDAP_LDAP_LIBRARY "ldap_r")
            else ()
                set (OPENLDAP_LDAP_LIBRARY "ldap")
            endif()
            set (OPENLDAP_LBER_LIBRARY "lber")
            set (OPENLDAP_LIBRARIES ${OPENLDAP_LDAP_LIBRARY} ${OPENLDAP_LBER_LIBRARY})
            set (OPENLDAP_FOUND 1)
        endif ()
    endif ()

    if (OPENLDAP_FOUND)
        set (USE_LDAP 1)
    endif ()
endif ()

message (STATUS "Using ldap=${USE_LDAP}: ${OPENLDAP_INCLUDE_DIR} : ${OPENLDAP_LIBRARIES}")
