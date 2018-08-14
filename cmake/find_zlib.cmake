if (NOT OS_FREEBSD)
    option (USE_INTERNAL_ZLIB_LIBRARY "Set to FALSE to use system zlib library instead of bundled" ${NOT_UNBUNDLED})
endif ()

if (NOT USE_INTERNAL_ZLIB_LIBRARY)
    find_package (ZLIB)
endif ()

if (NOT ZLIB_FOUND)
    if (NOT MSVC)
        set (INTERNAL_ZLIB_NAME "zlib-ng")
    else ()
        set (INTERNAL_ZLIB_NAME "zlib")
        if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/${INTERNAL_ZLIB_NAME}")
            message (WARNING "Will use standard zlib, please clone manually:\n git clone https://github.com/madler/zlib.git ${ClickHouse_SOURCE_DIR}/contrib/${INTERNAL_ZLIB_NAME}")
        endif ()
    endif ()

    set (USE_INTERNAL_ZLIB_LIBRARY 1)
    set (ZLIB_COMPAT 1) # for zlib-ng, also enables WITH_GZFILEOP
    set (WITH_NATIVE_INSTRUCTIONS ${ARCHNATIVE})
    if (OS_FREEBSD OR ARCH_I386)
        set (WITH_OPTIM 0 CACHE INTERNAL "") # Bug in assembler
    endif ()
    if (ARCH_AARCH64)
        set(WITH_NEON 1 CACHE INTERNAL "")
        set(WITH_ACLE 1 CACHE INTERNAL "")
    endif ()
    set (ZLIB_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/${INTERNAL_ZLIB_NAME}" "${ClickHouse_BINARY_DIR}/contrib/${INTERNAL_ZLIB_NAME}") # generated zconf.h
    set (ZLIB_INCLUDE_DIRS ${ZLIB_INCLUDE_DIR}) # for poco
    set (ZLIB_FOUND 1) # for poco
    if (USE_STATIC_LIBRARIES)
        set (ZLIB_LIBRARIES zlibstatic)
    else ()
        set (ZLIB_LIBRARIES zlib)
    endif ()
endif ()

message (STATUS "Using zlib: ${ZLIB_INCLUDE_DIR} : ${ZLIB_LIBRARIES}")
