if (${ENABLE_LIBRARIES} AND ${ENABLE_KRB5})
    set (DEFAULT_ENABLE_CYRUS_SASL 1)
else()
    set (DEFAULT_ENABLE_CYRUS_SASL 0)
endif()
option(ENABLE_CYRUS_SASL "Enable cyrus-sasl" ${DEFAULT_ENABLE_CYRUS_SASL})

if (NOT ENABLE_CYRUS_SASL)
    message(STATUS "Not using cyrus-sasl")
    return()
endif()

set(CYRUS_SASL_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/cyrus-sasl")

add_library(_sasl2)

target_sources(_sasl2 PRIVATE
    "${CYRUS_SASL_SOURCE_DIR}/plugins/gssapi.c"
    # "${CYRUS_SASL_SOURCE_DIR}/plugins/gssapiv2_init.c"
    "${CYRUS_SASL_SOURCE_DIR}/common/plugin_common.c"
    "${CYRUS_SASL_SOURCE_DIR}/lib/common.c"
    "${CYRUS_SASL_SOURCE_DIR}/lib/canonusr.c"
    "${CYRUS_SASL_SOURCE_DIR}/lib/server.c"
    "${CYRUS_SASL_SOURCE_DIR}/lib/config.c"
    "${CYRUS_SASL_SOURCE_DIR}/lib/auxprop.c"
    "${CYRUS_SASL_SOURCE_DIR}/lib/saslutil.c"
    "${CYRUS_SASL_SOURCE_DIR}/lib/external.c"
    "${CYRUS_SASL_SOURCE_DIR}/lib/seterror.c"
    "${CYRUS_SASL_SOURCE_DIR}/lib/md5.c"
    "${CYRUS_SASL_SOURCE_DIR}/lib/dlopen.c"
    "${CYRUS_SASL_SOURCE_DIR}/lib/client.c"
    "${CYRUS_SASL_SOURCE_DIR}/lib/checkpw.c"
)

target_include_directories(_sasl2 PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}
)

target_include_directories(_sasl2 PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}  # for config.h
    "${CYRUS_SASL_SOURCE_DIR}/plugins"
    ${CYRUS_SASL_SOURCE_DIR}
    "${CYRUS_SASL_SOURCE_DIR}/include"
    "${CYRUS_SASL_SOURCE_DIR}/lib"
    "${CYRUS_SASL_SOURCE_DIR}/sasldb"
    "${CYRUS_SASL_SOURCE_DIR}/common"
    "${CYRUS_SASL_SOURCE_DIR}/saslauthd"
    "${CYRUS_SASL_SOURCE_DIR}/sample"
    "${CYRUS_SASL_SOURCE_DIR}/utils"
    "${CYRUS_SASL_SOURCE_DIR}/tests"
)

target_compile_definitions(_sasl2 PUBLIC
    HAVE_CONFIG_H
    # PLUGINDIR="/usr/local/lib/sasl2"
    PLUGINDIR=""
    # PIC
    OBSOLETE_CRAM_ATTR=1
    # SASLAUTHD_CONF_FILE_DEFAULT="/usr/local/etc/saslauthd.conf"
    SASLAUTHD_CONF_FILE_DEFAULT=""
    # CONFIGDIR="/usr/local/lib/sasl2:/usr/local/etc/sasl2"
    CONFIGDIR=""
    OBSOLETE_DIGEST_ATTR=1
    LIBSASL_EXPORTS=1
)

file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/sasl")

file(COPY
    "${CYRUS_SASL_SOURCE_DIR}/include/sasl.h"
    DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/sasl"
)

file(COPY
    "${CYRUS_SASL_SOURCE_DIR}/include/prop.h"
    DESTINATION ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(_sasl2 PUBLIC ch_contrib::krb5)

add_library(ch_contrib::sasl2 ALIAS _sasl2)
