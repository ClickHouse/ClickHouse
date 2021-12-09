
option(ENABLE_S2_GEOMETRY "Enable S2 geometry library" ${ENABLE_LIBRARIES})

if (ENABLE_S2_GEOMETRY)
    if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/s2geometry")
        message (WARNING "submodule contrib/s2geometry is missing. to fix try run: \n git submodule update --init")
        set (ENABLE_S2_GEOMETRY 0)
        set (USE_S2_GEOMETRY 0)
    else()
        if (OPENSSL_FOUND)
            set (S2_GEOMETRY_LIBRARY s2)
            set (S2_GEOMETRY_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/s2geometry/src/s2)
            set (USE_S2_GEOMETRY 1)
        else()
            message (WARNING "S2 uses OpenSSL, but the latter is absent.")
        endif()
    endif()

    if (NOT USE_S2_GEOMETRY)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't enable S2 geometry library")
    endif()
endif()

message (STATUS "Using s2geometry=${USE_S2_GEOMETRY} : ${S2_GEOMETRY_INCLUDE_DIR}")
