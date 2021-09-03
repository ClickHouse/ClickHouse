set(OPENLDAP_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/openldap")

# How these lists were generated?
# I compiled the original OpenLDAP with it's original build system and copied the list of source files from build commands.

set(_libs_type SHARED)
if(OPENLDAP_USE_STATIC_LIBS)
    set(_libs_type STATIC)
endif()

set(OPENLDAP_VERSION_STRING "2.5.X")

macro(mkversion _lib_name)
    add_custom_command(
        OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${_lib_name}-version.c"
        COMMAND ${CMAKE_COMMAND} -E env bash -c "${OPENLDAP_SOURCE_DIR}/build/mkversion -v '${OPENLDAP_VERSION_STRING}' liblber.la > \"${CMAKE_CURRENT_BINARY_DIR}/${_lib_name}-version.c\""
        MAIN_DEPENDENCY "${OPENLDAP_SOURCE_DIR}/build/mkversion"
        WORKING_DIRECTORY ${OPENLDAP_SOURCE_DIR}
        VERBATIM
    )
endmacro()

string(TOLOWER "${CMAKE_SYSTEM_NAME}" _system_name)
string(TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" _system_processor)

if(
    "${_system_processor}" STREQUAL "amd64" OR
    "${_system_processor}" STREQUAL "x64"
)
    set(_system_processor "x86_64")
elseif(
    "${_system_processor}" STREQUAL "arm64"
)
    set (_system_processor "aarch64")
endif()

set(_extra_build_dir "${CMAKE_CURRENT_SOURCE_DIR}/${_system_name}_${_system_processor}")

set(_lber_srcs
    "${OPENLDAP_SOURCE_DIR}/libraries/liblber/assert.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/liblber/decode.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/liblber/encode.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/liblber/io.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/liblber/bprint.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/liblber/debug.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/liblber/memory.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/liblber/options.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/liblber/sockbuf.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/liblber/stdio.c"
)

mkversion(lber)

add_library(lber ${_libs_type}
    ${_lber_srcs}
    "${CMAKE_CURRENT_BINARY_DIR}/lber-version.c"
)

target_link_libraries(lber
    PRIVATE ${OPENSSL_LIBRARIES}
)

target_include_directories(lber
    PRIVATE ${_extra_build_dir}/include
    PRIVATE "${OPENLDAP_SOURCE_DIR}/include"
    PRIVATE "${OPENLDAP_SOURCE_DIR}/libraries/liblber"
    PRIVATE ${OPENSSL_INCLUDE_DIR}
)

target_compile_definitions(lber
    PRIVATE LBER_LIBRARY
)

set(_ldap_srcs
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/bind.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/open.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/result.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/error.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/compare.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/search.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/controls.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/messages.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/references.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/extended.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/cyrus.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/modify.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/add.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/modrdn.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/delete.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/abandon.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/sasl.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/sbind.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/unbind.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/cancel.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/filter.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/free.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/sort.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/passwd.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/whoami.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/vc.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/getdn.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/getentry.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/getattr.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/getvalues.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/addentry.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/request.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/os-ip.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/url.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/pagectrl.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/sortctrl.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/vlvctrl.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/init.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/options.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/print.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/string.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/util-int.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/schema.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/charray.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/os-local.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/dnssrv.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/utf-8.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/utf-8-conv.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/tls2.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/tls_o.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/tls_g.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/turn.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/ppolicy.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/dds.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/txn.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/ldap_sync.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/stctrl.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/assertion.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/deref.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/ldifutil.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/ldif.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/fetch.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/lbase64.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/msctrl.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap/psearchctrl.c"
)

mkversion(ldap)

add_library(ldap ${_libs_type}
    ${_ldap_srcs}
    "${CMAKE_CURRENT_BINARY_DIR}/ldap-version.c"
)

target_link_libraries(ldap
    PRIVATE lber
    PRIVATE ${OPENSSL_LIBRARIES}
)

target_include_directories(ldap
    PRIVATE ${_extra_build_dir}/include
    PRIVATE "${OPENLDAP_SOURCE_DIR}/include"
    PRIVATE "${OPENLDAP_SOURCE_DIR}/libraries/libldap"
    PRIVATE ${OPENSSL_INCLUDE_DIR}
)

target_compile_definitions(ldap
    PRIVATE LDAP_LIBRARY
)

set(_ldap_r_specific_srcs
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap_r/threads.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap_r/rdwr.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap_r/tpool.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap_r/rq.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap_r/thr_posix.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap_r/thr_thr.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap_r/thr_nt.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap_r/thr_pth.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap_r/thr_stub.c"
    "${OPENLDAP_SOURCE_DIR}/libraries/libldap_r/thr_debug.c"
)

mkversion(ldap_r)

add_library(ldap_r ${_libs_type}
    ${_ldap_r_specific_srcs}
    ${_ldap_srcs}
    "${CMAKE_CURRENT_BINARY_DIR}/ldap_r-version.c"
)

target_link_libraries(ldap_r
    PRIVATE lber
    PRIVATE ${OPENSSL_LIBRARIES}
)

target_include_directories(ldap_r
    PRIVATE ${_extra_build_dir}/include
    PRIVATE "${OPENLDAP_SOURCE_DIR}/include"
    PRIVATE "${OPENLDAP_SOURCE_DIR}/libraries/libldap_r"
    PRIVATE "${OPENLDAP_SOURCE_DIR}/libraries/libldap"
    PRIVATE ${OPENSSL_INCLUDE_DIR}
)

target_compile_definitions(ldap_r
    PRIVATE LDAP_R_COMPILE
    PRIVATE LDAP_LIBRARY
)
