option (ENABLE_ODBC "Enable ODBC library" ${ENABLE_LIBRARIES})

if (NOT OS_LINUX)
    if (ENABLE_ODBC)
        message(STATUS "ODBC is only supported on Linux")
    endif()
    set (ENABLE_ODBC OFF CACHE INTERNAL "")
endif ()

if (NOT ENABLE_ODBC)
    if (USE_INTERNAL_ODBC_LIBRARY)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use internal ODBC with ENABLE_ODBC=OFF")
    endif()

    add_library (unixodbc INTERFACE)
    target_compile_definitions (unixodbc INTERFACE USE_ODBC=0)

    message (STATUS "Not using unixodbc")
    return()
endif()

option (USE_INTERNAL_ODBC_LIBRARY "Use internal ODBC library" ${NOT_UNBUNDLED})

if (NOT USE_INTERNAL_ODBC_LIBRARY)
    find_library (LIBRARY_ODBC NAMES unixodbc odbc)
    find_path (INCLUDE_ODBC sql.h)

    if(LIBRARY_ODBC AND INCLUDE_ODBC)
        add_library (unixodbc INTERFACE)
        set_target_properties (unixodbc PROPERTIES INTERFACE_LINK_LIBRARIES ${LIBRARY_ODBC})
        set_target_properties (unixodbc PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${INCLUDE_ODBC})
        set_target_properties (unixodbc PROPERTIES INTERFACE_COMPILE_DEFINITIONS USE_ODBC=1)

        if (USE_STATIC_LIBRARIES)
            find_library(LTDL_LIBRARY ltdl)
            if (LTDL_LIBRARY)
                target_link_libraries(unixodbc INTERFACE ${LTDL_LIBRARY})
            endif()
        endif()

        set(EXTERNAL_ODBC_LIBRARY_FOUND 1)
        message (STATUS "Found odbc: ${LIBRARY_ODBC}")
    else()
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system ODBC library")
        set(EXTERNAL_ODBC_LIBRARY_FOUND 0)
    endif()
endif()

if (NOT EXTERNAL_ODBC_LIBRARY_FOUND)
    set (USE_INTERNAL_ODBC_LIBRARY 1)
endif ()

message (STATUS "Using unixodbc")
