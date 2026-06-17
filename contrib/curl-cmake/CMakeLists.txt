option (ENABLE_CURL "Enable curl" ${ENABLE_LIBRARIES})

if (NOT ENABLE_CURL)
    message(STATUS "Not using curl")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/curl")

set (SRCS
    "${LIBRARY_DIR}/lib/asyn-ares.c"
    "${LIBRARY_DIR}/lib/asyn-base.c"
    "${LIBRARY_DIR}/lib/bufq.c"
    "${LIBRARY_DIR}/lib/bufref.c"
    "${LIBRARY_DIR}/lib/cf-dns.c"
    "${LIBRARY_DIR}/lib/cf-h1-proxy.c"
    "${LIBRARY_DIR}/lib/cf-haproxy.c"
    "${LIBRARY_DIR}/lib/cf-https-connect.c"
    "${LIBRARY_DIR}/lib/cf-ip-happy.c"
    "${LIBRARY_DIR}/lib/cf-socket.c"
    "${LIBRARY_DIR}/lib/cfilters.c"
    "${LIBRARY_DIR}/lib/conncache.c"
    "${LIBRARY_DIR}/lib/connect.c"
    "${LIBRARY_DIR}/lib/content_encoding.c"
    "${LIBRARY_DIR}/lib/cshutdn.c"
    "${LIBRARY_DIR}/lib/curl_addrinfo.c"
    "${LIBRARY_DIR}/lib/curl_endian.c"
    "${LIBRARY_DIR}/lib/curl_gethostname.c"
    "${LIBRARY_DIR}/lib/curl_memrchr.c"
    "${LIBRARY_DIR}/lib/curl_sha512_256.c"
    "${LIBRARY_DIR}/lib/curl_share.c"
    "${LIBRARY_DIR}/lib/curl_threads.c"
    "${LIBRARY_DIR}/lib/curl_trc.c"
    "${LIBRARY_DIR}/lib/cw-out.c"
    "${LIBRARY_DIR}/lib/cw-pause.c"
    "${LIBRARY_DIR}/lib/dnscache.c"
    "${LIBRARY_DIR}/lib/dynhds.c"
    "${LIBRARY_DIR}/lib/easy.c"
    "${LIBRARY_DIR}/lib/easygetopt.c"
    "${LIBRARY_DIR}/lib/easyoptions.c"
    "${LIBRARY_DIR}/lib/escape.c"
    "${LIBRARY_DIR}/lib/formdata.c"
    "${LIBRARY_DIR}/lib/getenv.c"
    "${LIBRARY_DIR}/lib/getinfo.c"
    "${LIBRARY_DIR}/lib/hash.c"
    "${LIBRARY_DIR}/lib/headers.c"
    "${LIBRARY_DIR}/lib/hmac.c"
    "${LIBRARY_DIR}/lib/hostip.c"
    "${LIBRARY_DIR}/lib/http.c"
    "${LIBRARY_DIR}/lib/http1.c"
    "${LIBRARY_DIR}/lib/http2.c"
    "${LIBRARY_DIR}/lib/http_chunks.c"
    "${LIBRARY_DIR}/lib/http_digest.c"
    "${LIBRARY_DIR}/lib/http_proxy.c"
    "${LIBRARY_DIR}/lib/idn.c"
    "${LIBRARY_DIR}/lib/if2ip.c"
    "${LIBRARY_DIR}/lib/llist.c"
    "${LIBRARY_DIR}/lib/macos.c"
    "${LIBRARY_DIR}/lib/md5.c"
    "${LIBRARY_DIR}/lib/mime.c"
    "${LIBRARY_DIR}/lib/mprintf.c"
    "${LIBRARY_DIR}/lib/multi.c"
    "${LIBRARY_DIR}/lib/multi_ev.c"
    "${LIBRARY_DIR}/lib/multi_ntfy.c"
    "${LIBRARY_DIR}/lib/noproxy.c"
    "${LIBRARY_DIR}/lib/parsedate.c"
    "${LIBRARY_DIR}/lib/progress.c"
    "${LIBRARY_DIR}/lib/protocol.c"
    "${LIBRARY_DIR}/lib/rand.c"
    "${LIBRARY_DIR}/lib/ratelimit.c"
    "${LIBRARY_DIR}/lib/request.c"
    "${LIBRARY_DIR}/lib/select.c"
    "${LIBRARY_DIR}/lib/sendf.c"
    "${LIBRARY_DIR}/lib/setopt.c"
    "${LIBRARY_DIR}/lib/sha256.c"
    "${LIBRARY_DIR}/lib/slist.c"
    "${LIBRARY_DIR}/lib/socketpair.c"
    "${LIBRARY_DIR}/lib/socks.c"
    "${LIBRARY_DIR}/lib/splay.c"
    "${LIBRARY_DIR}/lib/strcase.c"
    "${LIBRARY_DIR}/lib/strequal.c"
    "${LIBRARY_DIR}/lib/strerror.c"
    "${LIBRARY_DIR}/lib/transfer.c"
    "${LIBRARY_DIR}/lib/uint-bset.c"
    "${LIBRARY_DIR}/lib/uint-hash.c"
    "${LIBRARY_DIR}/lib/uint-spbset.c"
    "${LIBRARY_DIR}/lib/uint-table.c"
    "${LIBRARY_DIR}/lib/url.c"
    "${LIBRARY_DIR}/lib/urlapi.c"
    "${LIBRARY_DIR}/lib/version.c"
    "${LIBRARY_DIR}/lib/ws.c"
    "${LIBRARY_DIR}/lib/vauth/cram.c"
    "${LIBRARY_DIR}/lib/vauth/digest.c"
    "${LIBRARY_DIR}/lib/vauth/vauth.c"
    "${LIBRARY_DIR}/lib/vquic/vquic.c"

    "${LIBRARY_DIR}/lib/vtls/hostcheck.c"
    "${LIBRARY_DIR}/lib/vtls/keylog.c"
    "${LIBRARY_DIR}/lib/vtls/openssl.c"
    "${LIBRARY_DIR}/lib/vtls/vtls.c"
    "${LIBRARY_DIR}/lib/vtls/vtls_scache.c"
    "${LIBRARY_DIR}/lib/curlx/base64.c"
    "${LIBRARY_DIR}/lib/curlx/basename.c"
    "${LIBRARY_DIR}/lib/curlx/dynbuf.c"
    "${LIBRARY_DIR}/lib/curlx/fopen.c"
    "${LIBRARY_DIR}/lib/curlx/inet_ntop.c"
    "${LIBRARY_DIR}/lib/curlx/inet_pton.c"
    "${LIBRARY_DIR}/lib/curlx/nonblock.c"
    "${LIBRARY_DIR}/lib/curlx/strcopy.c"
    "${LIBRARY_DIR}/lib/curlx/strdup.c"
    "${LIBRARY_DIR}/lib/curlx/strerr.c"
    "${LIBRARY_DIR}/lib/curlx/strparse.c"
    "${LIBRARY_DIR}/lib/curlx/timediff.c"
    "${LIBRARY_DIR}/lib/curlx/timeval.c"
    "${LIBRARY_DIR}/lib/curlx/wait.c"
    "${LIBRARY_DIR}/lib/curlx/warnless.c"
)

add_library (_curl ${SRCS})

target_compile_definitions (_curl PRIVATE
    HAVE_CONFIG_H
    BUILDING_LIBCURL
    CURL_HIDDEN_SYMBOLS
    libcurl_EXPORTS
    OPENSSL_NO_ENGINE
    CURL_OS="${CMAKE_SYSTEM_NAME}"
)

target_include_directories (_curl SYSTEM PUBLIC
    "${LIBRARY_DIR}/include"
    "${LIBRARY_DIR}/lib"
    . # curl_config.h
)

target_link_libraries (_curl PRIVATE OpenSSL::SSL ch_contrib::c-ares)

# The library is large - avoid bloat (XXX: is it?)
if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options (_curl PRIVATE -g1)
endif()

add_library (ch_contrib::curl ALIAS _curl)
