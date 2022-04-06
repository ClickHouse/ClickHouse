if(OS_LINUX AND TARGET OpenSSL::SSL)
    option(ENABLE_MYSQL "Enable MySQL" ${ENABLE_LIBRARIES})
else ()
    option(ENABLE_MYSQL "Enable MySQL" FALSE)
endif ()

if(NOT ENABLE_MYSQL)
    message (STATUS "Build without mysqlclient (support for MYSQL dictionary source will be disabled)")
    return()
endif()

if (GLIBC_COMPATIBILITY)
    set(LIBM glibc-compatibility)
endif()

# This is the LGPL libmariadb project.

set(CC_SOURCE_DIR ${CMAKE_SOURCE_DIR}/contrib/mariadb-connector-c)
set(CC_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR})

set(WITH_SSL ON)

set(MARIADB_CONNECTOR_C_COPYRIGHT "2013-2017 MariaDB Corporation Ab")

set(PROTOCOL_VERSION 10) # we adapted new password option from PHP's mysqlnd !

# if C/C is build as subproject inside MariaDB server tree we will
# use the version defined by server
if(MAJOR_VERSION)
  set(MARIADB_CLIENT_VERSION_MAJOR ${MAJOR_VERSION})
  set(MARIADB_CLIENT_VERSION_MINOR ${MINOR_VERSION})
  set(MARIADB_CLIENT_VERSION_PATCH ${PATCH_VERSION})
  set(MARIADB_CLIENT_VERSION_EXTRA ${EXTRA_VERSION})
else()
  set(MARIADB_CLIENT_VERSION_MAJOR "10")
  set(MARIADB_CLIENT_VERSION_MINOR "4")
  set(MARIADB_CLIENT_VERSION_PATCH "3")
  set(MARIADB_CLIENT_VERSION_EXTRA "")
endif()
set(MARIADB_CLIENT_VERSION "${MARIADB_CLIENT_VERSION_MAJOR}.${MARIADB_CLIENT_VERSION_MINOR}.${MARIADB_CLIENT_VERSION_PATCH}${MARIADB_CLIENT_VERSION_EXTRA}")
set(MARIADB_BASE_VERSION "mariadb-${MARIADB_CLIENT_VERSION_MAJOR}.${MARIADB_CLIENT_VERSION_MINOR}")
MATH(EXPR MARIADB_VERSION_ID "${MARIADB_CLIENT_VERSION_MAJOR} * 10000 +
                              ${MARIADB_CLIENT_VERSION_MINOR} * 100   +
                              ${MARIADB_CLIENT_VERSION_PATCH}")

IF (NOT MARIADB_PORT)
  set(MARIADB_PORT 3306)
ENDIF ()
if(NOT MARIADB_UNIX_ADDR)
  set(MARIADB_UNIX_ADDR "/tmp/mysql.sock")
endif()

set(HAVE_ALLOCA_H 1)
set(HAVE_ARPA_INET_H 1)
set(HAVE_DLFCN_H 1)
set(HAVE_FCNTL_H 1)
set(HAVE_FLOAT_H 1)
set(HAVE_LIMITS_H 1)
set(HAVE_PWD_H 1)
set(HAVE_SCHED_H 1)
set(HAVE_SELECT_H 0)
set(INCLUDE_SIGNAL 1)
set(HAVE_SIGNAL 1)
set(HAVE_STDDEF_H 1)
set(HAVE_STDINT_H 1)
set(HAVE_STDLIB_H 1)
set(HAVE_STRING_H 1)
set(HAVE_STRINGS_H 1)
set(HAVE_SYS_IOCTL_H 1)
set(HAVE_SYS_SELECT_H 1)
set(HAVE_SYS_SOCKET_H 1)
set(HAVE_SYS_TYPES_H 1)
set(HAVE_SYS_UN_H 1)
set(HAVE_UNISTD_H 1)
set(HAVE_UTIME_H 1)
set(HAVE_UCONTEXT_H 1)
set(HAVE_ALLOCA 1)
set(HAVE_DLERROR 0)
set(HAVE_DLOPEN 0)
set(HAVE_FCNTL 1)
set(HAVE_MEMCPY 1)
set(HAVE_NL_LANGINFO 0)
set(HAVE_SETLOCALE 0)
set(HAVE_POLL 1)

set(SIZEOF_CHARP 8)
set(SIZEOF_INT 4)
set(SIZEOF_LONG 8)
set(SIZEOF_LONG_LONG 8)
set(SIZEOF_SIZE_T 8)
set(SOCKET_SIZE_TYPE socklen_t)


set(SYSTEM_LIBS ${SYSTEM_LIBS} ch_contrib::zlib)

if(CMAKE_HAVE_PTHREAD_H)
  set(CMAKE_REQUIRED_INCLUDES pthread.h)
endif()

add_definitions(-DMARIADB_SYSTEM_TYPE="${CMAKE_SYSTEM_NAME}")
add_definitions(-DMARIADB_MACHINE_TYPE="${CMAKE_SYSTEM_PROCESSOR}")

set(HAVE_THREADS 1)
set(DEFAULT_CHARSET "utf8mb4")

add_definitions(-DHAVE_OPENSSL -DHAVE_TLS)
set(SSL_LIBRARIES OpenSSL::Crypto OpenSSL::SSL)
set(TLS_LIBRARY_VERSION "OpenSSL ${OPENSSL_VERSION}")

set(ENABLED_LOCAL_INFILE OFF)

CONFIGURE_FILE(${CC_SOURCE_DIR}/include/ma_config.h.in
               ${CC_BINARY_DIR}/include-private/ma_config.h)
CONFIGURE_FILE(${CC_SOURCE_DIR}/include/ma_config.h.in
               ${CC_BINARY_DIR}/include-private/config.h)
CONFIGURE_FILE(${CC_SOURCE_DIR}/include/mariadb_version.h.in
               ${CC_BINARY_DIR}/include-public/mariadb_version.h)

if(WITH_SSL)
  set(SYSTEM_LIBS ${SYSTEM_LIBS} ${SSL_LIBRARIES})
endif()


function(REGISTER_PLUGIN)

  SET(one_value_keywords TARGET TYPE)
  SET(multi_value_keywords SOURCES)

  cmake_parse_arguments(CC_PLUGIN
                        "${options}"
                        "${one_value_keywords}"
                        "${multi_value_keywords}"
                        ${ARGN})

  # overwrite default if it was specified with cmake option
  string(TOUPPER ${CC_PLUGIN_TARGET} cc_plugin)
  if(NOT "${CLIENT_PLUGIN_${cc_plugin}}" STREQUAL "")
    SET(CC_PLUGIN_DEFAULT ${CLIENT_PLUGIN_${cc_plugin}})
  endif()

  # use uppercase
  string(TOUPPER ${CC_PLUGIN_TARGET} target_name)
  string(TOUPPER "${CC_PLUGIN_CONFIGURATIONS}" CC_PLUGIN_CONFIGURATIONS)

  if(NOT ${PLUGIN_${target_name}} STREQUAL "")
    string(TOUPPER ${PLUGIN_${target_name}} PLUGIN_${target_name})
    set(CC_PLUGIN_DEFAULT ${PLUGIN_${target_name}})
  endif()

  set(PLUGINS_STATIC ${PLUGINS_STATIC} ${CC_PLUGIN_TARGET} PARENT_SCOPE)
  set(LIBMARIADB_PLUGIN_CFLAGS ${LIBMARIADB_PLUGIN_CFLAGS} ${CC_PLUGIN_COMPILE_OPTIONS} PARENT_SCOPE)
  set(LIBMARIADB_PLUGIN_INCLUDES ${LIBMARIADB_PLUGIN_INCLUDES} ${CC_PLUGIN_INCLUDES} PARENT_SCOPE)
  set(LIBMARIADB_PLUGIN_SOURCES ${LIBMARIADB_PLUGIN_SOURCES} ${CC_PLUGIN_SOURCES} PARENT_SCOPE)
  set(LIBMARIADB_PLUGIN_LIBS ${LIBMARIADB_PLUGIN_LIBS} ${CC_PLUGIN_LIBRARIES} PARENT_SCOPE)
endfunction()

SET(PLUGIN_EXTRA_FILES ${CC_SOURCE_DIR}/libmariadb/ma_errmsg.c)

#native password
REGISTER_PLUGIN(TARGET pvio_socket
                TYPE MARIADB_CLIENT_PLUGIN_PVIO
                SOURCES "${CC_SOURCE_DIR}/plugins/pvio/pvio_socket.c")

# SHA256 caching plugin for MySQL 8.0 connection
REGISTER_PLUGIN(TARGET caching_sha2_password
                TYPE MARIADB_CLIENT_PLUGIN_AUTH
                SOURCES "${CC_SOURCE_DIR}/plugins/auth/caching_sha2_pw.c")

REGISTER_PLUGIN(TARGET sha256_password
                TYPE MARIADB_CLIENT_PLUGIN_AUTH
                SOURCES "${CC_SOURCE_DIR}/plugins/auth/sha256_pw.c")

#native password
REGISTER_PLUGIN(TARGET mysql_native_password
                TYPE MARIADB_CLIENT_PLUGIN_AUTH
                SOURCES "${CC_SOURCE_DIR}/plugins/auth/my_auth.c")

REGISTER_PLUGIN(TARGET aurora
                TYPE MARIADB_CLIENT_PLUGIN_CONNECTION
                SOURCES "${CC_SOURCE_DIR}/plugins/connection/aurora.c")


add_definitions(-D HAVE_COMPRESS)
add_definitions(-D LIBMARIADB)
add_definitions(-D THREAD)

# handle static plugins
set(LIBMARIADB_SOURCES ${LIBMARIADB_PLUGIN_SOURCES})
set(SYSTEM_LIBS ${SYSTEM_LIBS} ${LIBMARIADB_PLUGIN_LIBS})
add_definitions(${LIBMARIADB_PLUGIN_DEFS})
FOREACH(plugin ${PLUGINS_STATIC})
  set(EXTERNAL_PLUGINS "${EXTERNAL_PLUGINS} extern struct st_mysql_client_plugin ${plugin}_client_plugin;\n")
  set(BUILTIN_PLUGINS "${BUILTIN_PLUGINS}   (struct st_mysql_client_plugin *)&${plugin}_client_plugin,\n")
ENDFOREACH()
CONFIGURE_FILE(${CC_SOURCE_DIR}/libmariadb/ma_client_plugin.c.in
               ${CC_BINARY_DIR}/libmariadb/ma_client_plugin.c)

set(LIBMARIADB_SOURCES ${LIBMARIADB_SOURCES}
${CC_SOURCE_DIR}/plugins/auth/my_auth.c
${CC_SOURCE_DIR}/libmariadb/ma_array.c
${CC_SOURCE_DIR}/libmariadb/ma_charset.c
${CC_SOURCE_DIR}/libmariadb/ma_hash.c
${CC_SOURCE_DIR}/libmariadb/ma_net.c
${CC_SOURCE_DIR}/libmariadb/mariadb_charset.c
${CC_SOURCE_DIR}/libmariadb/ma_time.c
${CC_SOURCE_DIR}/libmariadb/ma_default.c
${CC_SOURCE_DIR}/libmariadb/ma_errmsg.c
${CC_SOURCE_DIR}/libmariadb/mariadb_lib.c
${CC_SOURCE_DIR}/libmariadb/ma_list.c
${CC_SOURCE_DIR}/libmariadb/ma_pvio.c
${CC_SOURCE_DIR}/libmariadb/ma_tls.c
${CC_SOURCE_DIR}/libmariadb/ma_alloc.c
${CC_SOURCE_DIR}/libmariadb/ma_compress.c
${CC_SOURCE_DIR}/libmariadb/ma_init.c
${CC_SOURCE_DIR}/libmariadb/ma_password.c
${CC_SOURCE_DIR}/libmariadb/ma_ll2str.c
${CC_SOURCE_DIR}/libmariadb/ma_sha1.c
${CC_SOURCE_DIR}/libmariadb/mariadb_stmt.c
${CC_SOURCE_DIR}/libmariadb/ma_loaddata.c
${CC_SOURCE_DIR}/libmariadb/ma_stmt_codec.c
${CC_SOURCE_DIR}/libmariadb/ma_string.c
${CC_SOURCE_DIR}/libmariadb/ma_dtoa.c
${CC_SOURCE_DIR}/libmariadb/mariadb_rpl.c
${CC_SOURCE_DIR}/libmariadb/ma_io.c
${CC_SOURCE_DIR}/libmariadb/secure/openssl.c
${CC_SOURCE_DIR}/libmariadb/secure/openssl_crypt.c
${CC_BINARY_DIR}/libmariadb/ma_client_plugin.c
)

if(ICONV_INCLUDE_DIR)
   include_directories(BEFORE ${ICONV_INCLUDE_DIR})
endif()
add_definitions(-DLIBICONV_PLUG)

if(WITH_DYNCOL)
  set(LIBMARIADB_SOURCES ${LIBMARIADB_SOURCES} ${CC_SOURCE_DIR}/libmariadb/mariadb_dyncol.c)
endif()

set(LIBMARIADB_SOURCES ${LIBMARIADB_SOURCES} ${CC_SOURCE_DIR}/libmariadb/mariadb_async.c ${CC_SOURCE_DIR}/libmariadb/ma_context.c)


add_library(_mariadbclient STATIC ${LIBMARIADB_SOURCES})
target_link_libraries(_mariadbclient ${SYSTEM_LIBS})

target_include_directories(_mariadbclient PRIVATE ${CC_BINARY_DIR}/include-private)
target_include_directories(_mariadbclient SYSTEM PUBLIC ${CC_BINARY_DIR}/include-public ${CC_SOURCE_DIR}/include ${CC_SOURCE_DIR}/libmariadb)

set_target_properties(_mariadbclient PROPERTIES IMPORTED_INTERFACE_LINK_LIBRARIES "${SYSTEM_LIBS}")

add_library(ch_contrib::mariadbclient ALIAS _mariadbclient)
