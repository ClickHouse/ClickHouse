set(CYRUS_SASL_SOURCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/cyrus-sasl)

add_library(sasl2)

target_sources(sasl2 PRIVATE
    ${CYRUS_SASL_SOURCE_DIR}/plugins/gssapi.c
    ${CYRUS_SASL_SOURCE_DIR}/plugins/gssapiv2_init.c
    ${CYRUS_SASL_SOURCE_DIR}/common/plugin_common.c
    ${CYRUS_SASL_SOURCE_DIR}/lib/common.c
    ${CYRUS_SASL_SOURCE_DIR}/lib/canonusr.c
    ${CYRUS_SASL_SOURCE_DIR}/lib/server.c
    ${CYRUS_SASL_SOURCE_DIR}/lib/config.c
    ${CYRUS_SASL_SOURCE_DIR}/lib/auxprop.c
    ${CYRUS_SASL_SOURCE_DIR}/lib/saslutil.c
    ${CYRUS_SASL_SOURCE_DIR}/lib/external.c
    ${CYRUS_SASL_SOURCE_DIR}/lib/seterror.c
    ${CYRUS_SASL_SOURCE_DIR}/lib/md5.c
    ${CYRUS_SASL_SOURCE_DIR}/lib/dlopen.c
    ${CYRUS_SASL_SOURCE_DIR}/lib/client.c
    ${CYRUS_SASL_SOURCE_DIR}/lib/checkpw.c
)
target_include_directories(sasl2 PUBLIC
    ${CYRUS_SASL_SOURCE_DIR}/plugins
    ${CYRUS_SASL_SOURCE_DIR}
    ${CYRUS_SASL_SOURCE_DIR}/include
    ${CYRUS_SASL_SOURCE_DIR}/lib
    ${CYRUS_SASL_SOURCE_DIR}/sasldb
    ${CYRUS_SASL_SOURCE_DIR}/common
    ${CYRUS_SASL_SOURCE_DIR}/saslauthd
    ${CYRUS_SASL_SOURCE_DIR}/sample
    ${CYRUS_SASL_SOURCE_DIR}/utils
    ${CYRUS_SASL_SOURCE_DIR}/tests
)
target_compile_definitions(sasl2 PUBLIC
    HAVE_CONFIG_H
    PLUGINDIR="/usr/local/lib/sasl2"
    # PIC
    # PLUGINDIR="../plugins/.libs"
    OBSOLETE_CRAM_ATTR=1
    SASLAUTHD_CONF_FILE_DEFAULT="/usr/local/etc/saslauthd.conf"
    CONFIGDIR="/usr/local/lib/sasl2:/usr/local/etc/sasl2"
    OBSOLETE_DIGEST_ATTR=1
    LIBSASL_EXPORTS=1
)

# add_executable(client)

# target_sources(client PUBLIC
#     ${CMAKE_CURRENT_SOURCE_DIR}/sample/client.c
#     ${CMAKE_CURRENT_SOURCE_DIR}/sample/common.c
# )

# target_include_directories(client PUBLIC
#     ${CYRUS_SASL_SOURCE_DIR}/plugins
#     ${CYRUS_SASL_SOURCE_DIR}
#     ${CYRUS_SASL_SOURCE_DIR}/include
#     ${CYRUS_SASL_SOURCE_DIR}/lib
#     ${CYRUS_SASL_SOURCE_DIR}/sasldb
#     ${CYRUS_SASL_SOURCE_DIR}/common
#     ${CYRUS_SASL_SOURCE_DIR}/saslauthd
#     ${CYRUS_SASL_SOURCE_DIR}/sample
#     ${CYRUS_SASL_SOURCE_DIR}/utils
#     ${CYRUS_SASL_SOURCE_DIR}/tests
# )
# target_compile_definitions(client PUBLIC
#     HAVE_CONFIG_H
#     PLUGINDIR="/usr/local/lib/sasl2"
#     # PIC
#     # PLUGINDIR="../plugins/.libs"
#     OBSOLETE_CRAM_ATTR=1
#     SASLAUTHD_CONF_FILE_DEFAULT="/usr/local/etc/saslauthd.conf"
#     CONFIGDIR="/usr/local/lib/sasl2:/usr/local/etc/sasl2"
#     OBSOLETE_DIGEST_ATTR=1
#     LIBSASL_EXPORTS=1
#     )

# target_link_libraries(client PRIVATE
#     ${PROJECT_NAME}
#     dl
#     krb5
#     gssapi_krb5
# )

# add_executable(server)

# target_sources(server PUBLIC
#     ${CMAKE_CURRENT_SOURCE_DIR}/sample/server.c
#     ${CMAKE_CURRENT_SOURCE_DIR}/sample/common.c
# )

# target_include_directories(server PUBLIC
#     ${CYRUS_SASL_SOURCE_DIR}/plugins
#     ${CYRUS_SASL_SOURCE_DIR}
#     ${CYRUS_SASL_SOURCE_DIR}/include
#     ${CYRUS_SASL_SOURCE_DIR}/lib
#     ${CYRUS_SASL_SOURCE_DIR}/sasldb
#     ${CYRUS_SASL_SOURCE_DIR}/common
#     ${CYRUS_SASL_SOURCE_DIR}/saslauthd
#     ${CYRUS_SASL_SOURCE_DIR}/sample
#     ${CYRUS_SASL_SOURCE_DIR}/utils
#     ${CYRUS_SASL_SOURCE_DIR}/tests
# )
# target_compile_definitions(server PUBLIC
#     HAVE_CONFIG_H
#     PLUGINDIR="/usr/local/lib/sasl2"
#     # PIC
#     # PLUGINDIR="../plugins/.libs"
#     OBSOLETE_CRAM_ATTR=1
#     SASLAUTHD_CONF_FILE_DEFAULT="/usr/local/etc/saslauthd.conf"
#     CONFIGDIR="/usr/local/lib/sasl2:/usr/local/etc/sasl2"
#     OBSOLETE_DIGEST_ATTR=1
#     LIBSASL_EXPORTS=1
#     )

# target_link_libraries(server PRIVATE
#     krb5
#     gssapi_krb5
#     ${PROJECT_NAME}
#     dl
# )
