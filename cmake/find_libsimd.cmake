option (ENABLE_LIBSIMD "Enable libsimd")

if (ENABLE_LIBSIMD)

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/libsimd/CMakeLists.txt")
    message (FATAL_ERROR "submodule contrib/libsimd is missing. set ENABLE_LIBSIMD=0 or run:\n"
        "git submodule update --init --recursive\n")
else ()
    set (USE_LIBSIMD 1)
    set (LIBSIMD_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/libsimd)
endif ()

if (USE_LIBSIMD)
    message (STATUS "Using libsimd: ${LIBSIMD_INCLUDE_DIR}")
endif()

else ()
    set (USE_LIBSIMD 0)
endif()
