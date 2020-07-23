option(ENABLE_GSASL_LIBRARY "Enable gsasl library" ${ENABLE_LIBRARIES})

if (ENABLE_GSASL_LIBRARY)

option (USE_INTERNAL_LIBGSASL_LIBRARY "Set to FALSE to use system libgsasl library instead of bundled" ${NOT_UNBUNDLED})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/libgsasl/src/gsasl.h")
    if (USE_INTERNAL_LIBGSASL_LIBRARY)
        message (WARNING "submodule contrib/libgsasl is missing. to fix try run: \n git submodule update --init --recursive")
        set (USE_INTERNAL_LIBGSASL_LIBRARY 0)
    endif ()
    set (MISSING_INTERNAL_LIBGSASL_LIBRARY 1)
endif ()

if (NOT USE_INTERNAL_LIBGSASL_LIBRARY)
    find_library (LIBGSASL_LIBRARY gsasl)
    find_path (LIBGSASL_INCLUDE_DIR NAMES gsasl.h PATHS ${LIBGSASL_INCLUDE_PATHS})
endif ()

if (LIBGSASL_LIBRARY AND LIBGSASL_INCLUDE_DIR)
elseif (NOT MISSING_INTERNAL_LIBGSASL_LIBRARY)
    set (LIBGSASL_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/libgsasl/src ${ClickHouse_SOURCE_DIR}/contrib/libgsasl/linux_x86_64/include)
    set (USE_INTERNAL_LIBGSASL_LIBRARY 1)
    set (LIBGSASL_LIBRARY libgsasl)
endif ()

if(LIBGSASL_LIBRARY AND LIBGSASL_INCLUDE_DIR)
    set (USE_LIBGSASL 1)
endif()

endif()

message (STATUS "Using libgsasl=${USE_LIBGSASL}: ${LIBGSASL_INCLUDE_DIR} : ${LIBGSASL_LIBRARY}")
