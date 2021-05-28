if (NOT USE_INTERNAL_CURL)
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/curl")

set (SRCS
    "${LIBRARY_DIR}/lib/file.c"
    "${LIBRARY_DIR}/lib/timeval.c"
    "${LIBRARY_DIR}/lib/base64.c"
    "${LIBRARY_DIR}/lib/hostip.c"
    "${LIBRARY_DIR}/lib/progress.c"
    "${LIBRARY_DIR}/lib/formdata.c"
    "${LIBRARY_DIR}/lib/cookie.c"
    "${LIBRARY_DIR}/lib/http.c"
    "${LIBRARY_DIR}/lib/sendf.c"
    "${LIBRARY_DIR}/lib/url.c"
    "${LIBRARY_DIR}/lib/dict.c"
    "${LIBRARY_DIR}/lib/if2ip.c"
    "${LIBRARY_DIR}/lib/speedcheck.c"
    "${LIBRARY_DIR}/lib/ldap.c"
    "${LIBRARY_DIR}/lib/version.c"
    "${LIBRARY_DIR}/lib/getenv.c"
    "${LIBRARY_DIR}/lib/escape.c"
    "${LIBRARY_DIR}/lib/mprintf.c"
    "${LIBRARY_DIR}/lib/telnet.c"
    "${LIBRARY_DIR}/lib/netrc.c"
    "${LIBRARY_DIR}/lib/getinfo.c"
    "${LIBRARY_DIR}/lib/transfer.c"
    "${LIBRARY_DIR}/lib/strcase.c"
    "${LIBRARY_DIR}/lib/easy.c"
    "${LIBRARY_DIR}/lib/security.c"
    "${LIBRARY_DIR}/lib/curl_fnmatch.c"
    "${LIBRARY_DIR}/lib/fileinfo.c"
    "${LIBRARY_DIR}/lib/wildcard.c"
    "${LIBRARY_DIR}/lib/krb5.c"
    "${LIBRARY_DIR}/lib/memdebug.c"
    "${LIBRARY_DIR}/lib/http_chunks.c"
    "${LIBRARY_DIR}/lib/strtok.c"
    "${LIBRARY_DIR}/lib/connect.c"
    "${LIBRARY_DIR}/lib/llist.c"
    "${LIBRARY_DIR}/lib/hash.c"
    "${LIBRARY_DIR}/lib/multi.c"
    "${LIBRARY_DIR}/lib/content_encoding.c"
    "${LIBRARY_DIR}/lib/share.c"
    "${LIBRARY_DIR}/lib/http_digest.c"
    "${LIBRARY_DIR}/lib/md4.c"
    "${LIBRARY_DIR}/lib/md5.c"
    "${LIBRARY_DIR}/lib/http_negotiate.c"
    "${LIBRARY_DIR}/lib/inet_pton.c"
    "${LIBRARY_DIR}/lib/strtoofft.c"
    "${LIBRARY_DIR}/lib/strerror.c"
    "${LIBRARY_DIR}/lib/amigaos.c"
    "${LIBRARY_DIR}/lib/hostasyn.c"
    "${LIBRARY_DIR}/lib/hostip4.c"
    "${LIBRARY_DIR}/lib/hostip6.c"
    "${LIBRARY_DIR}/lib/hostsyn.c"
    "${LIBRARY_DIR}/lib/inet_ntop.c"
    "${LIBRARY_DIR}/lib/parsedate.c"
    "${LIBRARY_DIR}/lib/select.c"
    "${LIBRARY_DIR}/lib/splay.c"
    "${LIBRARY_DIR}/lib/strdup.c"
    "${LIBRARY_DIR}/lib/socks.c"
    "${LIBRARY_DIR}/lib/curl_addrinfo.c"
    "${LIBRARY_DIR}/lib/socks_gssapi.c"
    "${LIBRARY_DIR}/lib/socks_sspi.c"
    "${LIBRARY_DIR}/lib/curl_sspi.c"
    "${LIBRARY_DIR}/lib/slist.c"
    "${LIBRARY_DIR}/lib/nonblock.c"
    "${LIBRARY_DIR}/lib/curl_memrchr.c"
    "${LIBRARY_DIR}/lib/imap.c"
    "${LIBRARY_DIR}/lib/pop3.c"
    "${LIBRARY_DIR}/lib/smtp.c"
    "${LIBRARY_DIR}/lib/pingpong.c"
    "${LIBRARY_DIR}/lib/rtsp.c"
    "${LIBRARY_DIR}/lib/curl_threads.c"
    "${LIBRARY_DIR}/lib/warnless.c"
    "${LIBRARY_DIR}/lib/hmac.c"
    "${LIBRARY_DIR}/lib/curl_rtmp.c"
    "${LIBRARY_DIR}/lib/openldap.c"
    "${LIBRARY_DIR}/lib/curl_gethostname.c"
    "${LIBRARY_DIR}/lib/gopher.c"
    "${LIBRARY_DIR}/lib/idn_win32.c"
    "${LIBRARY_DIR}/lib/http_proxy.c"
    "${LIBRARY_DIR}/lib/non-ascii.c"
    "${LIBRARY_DIR}/lib/asyn-thread.c"
    "${LIBRARY_DIR}/lib/curl_gssapi.c"
    "${LIBRARY_DIR}/lib/http_ntlm.c"
    "${LIBRARY_DIR}/lib/curl_ntlm_wb.c"
    "${LIBRARY_DIR}/lib/curl_ntlm_core.c"
    "${LIBRARY_DIR}/lib/curl_sasl.c"
    "${LIBRARY_DIR}/lib/rand.c"
    "${LIBRARY_DIR}/lib/curl_multibyte.c"
    "${LIBRARY_DIR}/lib/hostcheck.c"
    "${LIBRARY_DIR}/lib/conncache.c"
    "${LIBRARY_DIR}/lib/dotdot.c"
    "${LIBRARY_DIR}/lib/x509asn1.c"
    "${LIBRARY_DIR}/lib/http2.c"
    "${LIBRARY_DIR}/lib/smb.c"
    "${LIBRARY_DIR}/lib/curl_endian.c"
    "${LIBRARY_DIR}/lib/curl_des.c"
    "${LIBRARY_DIR}/lib/system_win32.c"
    "${LIBRARY_DIR}/lib/mime.c"
    "${LIBRARY_DIR}/lib/sha256.c"
    "${LIBRARY_DIR}/lib/setopt.c"
    "${LIBRARY_DIR}/lib/curl_path.c"
    "${LIBRARY_DIR}/lib/curl_ctype.c"
    "${LIBRARY_DIR}/lib/curl_range.c"
    "${LIBRARY_DIR}/lib/psl.c"
    "${LIBRARY_DIR}/lib/doh.c"
    "${LIBRARY_DIR}/lib/urlapi.c"
    "${LIBRARY_DIR}/lib/curl_get_line.c"
    "${LIBRARY_DIR}/lib/altsvc.c"
    "${LIBRARY_DIR}/lib/socketpair.c"
    "${LIBRARY_DIR}/lib/vauth/vauth.c"
    "${LIBRARY_DIR}/lib/vauth/cleartext.c"
    "${LIBRARY_DIR}/lib/vauth/cram.c"
    "${LIBRARY_DIR}/lib/vauth/digest.c"
    "${LIBRARY_DIR}/lib/vauth/digest_sspi.c"
    "${LIBRARY_DIR}/lib/vauth/krb5_gssapi.c"
    "${LIBRARY_DIR}/lib/vauth/krb5_sspi.c"
    "${LIBRARY_DIR}/lib/vauth/ntlm.c"
    "${LIBRARY_DIR}/lib/vauth/ntlm_sspi.c"
    "${LIBRARY_DIR}/lib/vauth/oauth2.c"
    "${LIBRARY_DIR}/lib/vauth/spnego_gssapi.c"
    "${LIBRARY_DIR}/lib/vauth/spnego_sspi.c"
    "${LIBRARY_DIR}/lib/vtls/openssl.c"
    "${LIBRARY_DIR}/lib/vtls/gtls.c"
    "${LIBRARY_DIR}/lib/vtls/vtls.c"
    "${LIBRARY_DIR}/lib/vtls/nss.c"
    "${LIBRARY_DIR}/lib/vtls/polarssl.c"
    "${LIBRARY_DIR}/lib/vtls/polarssl_threadlock.c"
    "${LIBRARY_DIR}/lib/vtls/wolfssl.c"
    "${LIBRARY_DIR}/lib/vtls/schannel.c"
    "${LIBRARY_DIR}/lib/vtls/schannel_verify.c"
    "${LIBRARY_DIR}/lib/vtls/sectransp.c"
    "${LIBRARY_DIR}/lib/vtls/gskit.c"
    "${LIBRARY_DIR}/lib/vtls/mbedtls.c"
    "${LIBRARY_DIR}/lib/vtls/mesalink.c"
    "${LIBRARY_DIR}/lib/vtls/bearssl.c"
    "${LIBRARY_DIR}/lib/vquic/ngtcp2.c"
    "${LIBRARY_DIR}/lib/vquic/quiche.c"
    "${LIBRARY_DIR}/lib/vssh/libssh2.c"
    "${LIBRARY_DIR}/lib/vssh/libssh.c"
)

add_library (curl ${SRCS})

target_compile_definitions (curl PRIVATE
    HAVE_CONFIG_H
    BUILDING_LIBCURL
    CURL_HIDDEN_SYMBOLS
    libcurl_EXPORTS
    OS="${CMAKE_SYSTEM_NAME}"
)
target_include_directories (curl PUBLIC
    "${LIBRARY_DIR}/include"
    "${LIBRARY_DIR}/lib"
    . # curl_config.h
)

target_link_libraries (curl PRIVATE ssl)

# The library is large - avoid bloat (XXX: is it?)
target_compile_options (curl PRIVATE -g0)

# find_package(CURL) compatibility for the following packages that uses
# find_package(CURL)/include(FindCURL):
# - mariadb-connector-c
# - aws-s3-cmake
# - sentry-native
set (CURL_FOUND ON CACHE BOOL "")
set (CURL_ROOT_DIR ${LIBRARY_DIR} CACHE PATH "")
set (CURL_INCLUDE_DIR "${LIBRARY_DIR}/include" CACHE PATH "")
set (CURL_INCLUDE_DIRS "${LIBRARY_DIR}/include" CACHE PATH "")
set (CURL_LIBRARY curl CACHE STRING "")
set (CURL_LIBRARIES ${CURL_LIBRARY} CACHE STRING "")
set (CURL_VERSION_STRING 7.67.0 CACHE STRING "")
add_library (CURL::libcurl ALIAS ${CURL_LIBRARY})
