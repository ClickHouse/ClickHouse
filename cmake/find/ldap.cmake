if (UNBUNDLED AND USE_STATIC_LIBRARIES)
    set (ENABLE_LDAP OFF CACHE INTERNAL "")
endif()

option (ENABLE_LDAP "Enable LDAP" ${ENABLE_LIBRARIES})

if (NOT ENABLE_LDAP)
    if(USE_INTERNAL_LDAP_LIBRARY)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot use internal LDAP library with ENABLE_LDAP=OFF")
    endif ()
    return()
endif()

option (USE_INTERNAL_LDAP_LIBRARY "Set to FALSE to use system *LDAP library instead of bundled" ${NOT_UNBUNDLED})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/openldap/README")
    if (USE_INTERNAL_LDAP_LIBRARY)
        message (WARNING "Submodule contrib/openldap is missing. To fix try running:\n git submodule update --init --recursive")
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal LDAP library")
    endif ()

    set (USE_INTERNAL_LDAP_LIBRARY 0)
    set (MISSING_INTERNAL_LDAP_LIBRARY 1)
endif ()

set (OPENLDAP_USE_STATIC_LIBS ${USE_STATIC_LIBRARIES})
set (OPENLDAP_USE_REENTRANT_LIBS 1)

if (NOT USE_INTERNAL_LDAP_LIBRARY)
    if (OPENLDAP_USE_STATIC_LIBS)
        message (WARNING "Unable to use external static OpenLDAP libraries, falling back to the bundled version.")
        message (${RECONFIGURE_MESSAGE_LEVEL} "Unable to use external OpenLDAP")
        set (USE_INTERNAL_LDAP_LIBRARY 1)
    else ()
        if (APPLE AND NOT OPENLDAP_ROOT_DIR)
            set (OPENLDAP_ROOT_DIR "/usr/local/opt/openldap")
        endif ()

        find_package (OpenLDAP)

        if (NOT OPENLDAP_FOUND)
            message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system OpenLDAP")
        endif()
    endif ()
endif ()

if (NOT OPENLDAP_FOUND AND NOT MISSING_INTERNAL_LDAP_LIBRARY)
    string (TOLOWER "${CMAKE_SYSTEM_NAME}" _system_name)
    string (TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" _system_processor)

    if (
        "${_system_processor}" STREQUAL "amd64" OR
        "${_system_processor}" STREQUAL "x64"
    )
        set (_system_processor "x86_64")
    elseif (
        "${_system_processor}" STREQUAL "arm64"
    )
        set (_system_processor "aarch64")
    endif ()

    if (
        ( "${_system_name}" STREQUAL "linux"   AND "${_system_processor}" STREQUAL "x86_64"  ) OR
        ( "${_system_name}" STREQUAL "linux"   AND "${_system_processor}" STREQUAL "aarch64" ) OR
        ( "${_system_name}" STREQUAL "freebsd" AND "${_system_processor}" STREQUAL "x86_64"  ) OR
        ( "${_system_name}" STREQUAL "darwin"  AND "${_system_processor}" STREQUAL "x86_64"  )
    )
        set (_ldap_supported_platform TRUE)
    endif ()

    if (NOT _ldap_supported_platform)
        message (WARNING "LDAP support using the bundled library is not implemented for ${CMAKE_SYSTEM_NAME} ${CMAKE_SYSTEM_PROCESSOR} platform.")
        message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot enable LDAP support")
    elseif (NOT USE_SSL)
        message (WARNING "LDAP support using the bundled library is not possible if SSL is not used.")
        message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot enable LDAP support")
    else ()
        set (USE_INTERNAL_LDAP_LIBRARY 1)
        set (OPENLDAP_ROOT_DIR "${ClickHouse_SOURCE_DIR}/contrib/openldap")
        set (OPENLDAP_INCLUDE_DIRS
            "${ClickHouse_SOURCE_DIR}/contrib/openldap-cmake/${_system_name}_${_system_processor}/include"
            "${ClickHouse_SOURCE_DIR}/contrib/openldap/include"
        )
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

message (STATUS "Using ldap=${USE_LDAP}: ${OPENLDAP_INCLUDE_DIRS} : ${OPENLDAP_LIBRARIES}")
