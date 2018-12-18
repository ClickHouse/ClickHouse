if (NOT APPLE AND NOT ARCH_32)
    option (USE_INTERNAL_LIBGSASL_LIBRARY "Set to FALSE to use system libgsasl library instead of bundled" ${NOT_UNBUNDLED})
endif ()

if (USE_INTERNAL_LIBGSASL_LIBRARY AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/libgsasl/src/gsasl.h")
   message (WARNING "submodule contrib/libgsasl is missing. to fix try run: \n git submodule update --init --recursive")
   set (USE_INTERNAL_LIBGSASL_LIBRARY 0)
endif ()

if (NOT USE_INTERNAL_LIBGSASL_LIBRARY)
    find_library (LIBGSASL_LIBRARY gsasl)
    find_path (LIBGSASL_INCLUDE_DIR NAMES gsasl.h PATHS ${LIBGSASL_INCLUDE_PATHS})
endif ()

if (LIBGSASL_LIBRARY AND LIBGSASL_INCLUDE_DIR)
elseif (NOT APPLE AND NOT ARCH_32)
    set (LIBGSASL_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/libgsasl/src ${ClickHouse_SOURCE_DIR}/contrib/libgsasl/linux_x86_64/include)
    set (USE_INTERNAL_LIBGSASL_LIBRARY 1)
    set (LIBGSASL_LIBRARY libgsasl)
endif ()

message (STATUS "Using libgsasl: ${LIBGSASL_INCLUDE_DIR} : ${LIBGSASL_LIBRARY}")
