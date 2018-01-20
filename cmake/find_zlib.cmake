option (USE_INTERNAL_ZLIB_LIBRARY "Set to FALSE to use system zlib library instead of bundled" ${NOT_UNBUNDLED})

if (NOT USE_INTERNAL_ZLIB_LIBRARY)
    find_package (ZLIB)
endif ()

if (NOT ZLIB_FOUND)
    set (USE_INTERNAL_ZLIB_LIBRARY 1)
    set (ZLIB_COMPAT 1) # for zlib-ng, also enables WITH_GZFILEOP
    set (WITH_NATIVE_INSTRUCTIONS ${ARCHNATIVE})
    if (ARCH_FREEBSD OR ARCH_I386)
        set (WITH_OPTIM 0 CACHE INTERNAL "") # Bug in assembler
    endif ()
    if (ARCH_AARCH64)
        set(WITH_NEON 1 CACHE INTERNAL "")
        set(WITH_ACLE 1 CACHE INTERNAL "")
    endif ()
    set (ZLIB_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/zlib-ng" "${ClickHouse_BINARY_DIR}/contrib/zlib-ng") # generated zconf.h
    set (ZLIB_INCLUDE_DIRS ${ZLIB_INCLUDE_DIR}) # for poco
    set (ZLIB_FOUND 1) # for poco
    if (USE_STATIC_LIBRARIES)
        set (ZLIB_LIBRARIES zlibstatic)
    else ()
        set (ZLIB_LIBRARIES zlib)
    endif ()
endif ()

message (STATUS "Using zlib: ${ZLIB_INCLUDE_DIR} : ${ZLIB_LIBRARIES}")
