option (ENABLE_NGHTTP2 "Use nghttp2" ${ENABLE_LIBRARIES})
if (NOT ENABLE_NGHTTP2)
    message(STATUS "Not using nghttp2")
    return()
endif()

set(NGHTTP2_DIR "${ClickHouse_SOURCE_DIR}/contrib/nghttp2")

# We use this trick because nghttp2 contains a LICENSE file with "See COPYING" in it
# It breaks ClickHouse license checker (because LICENSE is prioritized over COPYING) so we just remove it
# Unfortunately, it actually removes the file from the filesystem but I can't think of a better solution now
file(REMOVE "${NGHTTP2_DIR}/LICENSE")

file(GLOB_RECURSE NGHTTP2_SRCS "${NGHTTP2_DIR}/lib/*.c")

set(HAVE_ARPA_INET_H 1)
add_definitions(-DHAVE_CONFIG_H)
configure_file("${NGHTTP2_DIR}/cmakeconfig.h.in" "${NGHTTP2_DIR}/config.h")

set(PACKAGE_VERSION "1.68.90")
# Converts a version such as 1.2.255 to 0x0102ff
function(HexVersion version_hex_var major minor patch)
  math(EXPR version_dec "${major} * 256 * 256 + ${minor} * 256 + ${patch}")
  set(version_hex "0x")
  foreach(i RANGE 5 0 -1)
    math(EXPR num "(${version_dec} >> (4 * ${i})) & 15")
    string(SUBSTRING "0123456789abcdef" ${num} 1 num_hex)
    set(version_hex "${version_hex}${num_hex}")
  endforeach()
  set(${version_hex_var} "${version_hex}" PARENT_SCOPE)
endfunction()
HexVersion(PACKAGE_VERSION_NUM 1 68 90)
configure_file("${NGHTTP2_DIR}/lib/includes/nghttp2/nghttp2ver.h.in" "${NGHTTP2_DIR}/lib/includes/nghttp2/nghttp2ver.h")

add_library(nghttp2 STATIC ${NGHTTP2_SRCS})

target_include_directories(nghttp2 PUBLIC "${NGHTTP2_DIR}/lib/includes")
target_include_directories(nghttp2 PRIVATE "${NGHTTP2_DIR}/")

add_library(ch_contrib::nghttp2 ALIAS nghttp2)
