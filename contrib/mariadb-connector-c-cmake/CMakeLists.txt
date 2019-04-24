set(MARIADB_CLIENT_SOURCE_DIR ${CMAKE_SOURCE_DIR}/contrib/mariadb-connector-c)
set(MARIADB_CLIENT_BINARY_DIR ${CMAKE_BINARY_DIR}/contrib/mariadb-connector-c)

set(SRCS
#${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/bmove_upp.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/get_password.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_alloc.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_array.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_charset.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_compress.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_context.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_default.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_dtoa.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_errmsg.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_hash.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_init.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_io.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_list.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_ll2str.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_loaddata.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_net.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_password.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_pvio.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/mariadb_async.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/mariadb_charset.c
#${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/mariadb_dyncol.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/mariadb_lib.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/mariadb_stmt.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_sha1.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_stmt_codec.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_string.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_time.c
${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/ma_tls.c
#${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/secure/gnutls.c
#${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/secure/ma_schannel.c
#${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/secure/schannel.c
#${MARIADB_CLIENT_SOURCE_DIR}/plugins/auth/auth_gssapi_client.c
#${MARIADB_CLIENT_SOURCE_DIR}/plugins/auth/dialog.c
#${MARIADB_CLIENT_SOURCE_DIR}/plugins/auth/gssapi_client.c
#${MARIADB_CLIENT_SOURCE_DIR}/plugins/auth/gssapi_errmsg.c
${MARIADB_CLIENT_SOURCE_DIR}/plugins/auth/mariadb_cleartext.c
${MARIADB_CLIENT_SOURCE_DIR}/plugins/auth/my_auth.c
${MARIADB_CLIENT_SOURCE_DIR}/plugins/auth/old_password.c
${MARIADB_CLIENT_SOURCE_DIR}/plugins/auth/sha256_pw.c
#${MARIADB_CLIENT_SOURCE_DIR}/plugins/auth/sspi_client.c
#${MARIADB_CLIENT_SOURCE_DIR}/plugins/auth/sspi_errmsg.c
${MARIADB_CLIENT_SOURCE_DIR}/plugins/connection/aurora.c
${MARIADB_CLIENT_SOURCE_DIR}/plugins/connection/replication.c
#${MARIADB_CLIENT_SOURCE_DIR}/plugins/io/remote_io.c
#${MARIADB_CLIENT_SOURCE_DIR}/plugins/pvio/pvio_npipe.c
#${MARIADB_CLIENT_SOURCE_DIR}/plugins/pvio/pvio_shmem.c
${MARIADB_CLIENT_SOURCE_DIR}/plugins/pvio/pvio_socket.c
#${MARIADB_CLIENT_SOURCE_DIR}/plugins/trace/trace_example.c
${CMAKE_CURRENT_SOURCE_DIR}/linux_x86_64/libmariadb/ma_client_plugin.c
)

if(OPENSSL_LIBRARIES)
    list(APPEND SRCS ${MARIADB_CLIENT_SOURCE_DIR}/libmariadb/secure/openssl.c)
endif()

add_library(mysqlclient ${SRCS})

if(OPENSSL_LIBRARIES)
    target_link_libraries(mysqlclient PRIVATE ${OPENSSL_LIBRARIES})
    target_compile_definitions(mysqlclient PRIVATE -D HAVE_OPENSSL -D HAVE_TLS)
endif()

target_include_directories(mysqlclient PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/linux_x86_64/include)
target_include_directories(mysqlclient PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/common/include)
target_include_directories(mysqlclient PUBLIC ${MARIADB_CLIENT_SOURCE_DIR}/include)

target_compile_definitions(mysqlclient PRIVATE -D THREAD)
