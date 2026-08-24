option(ENABLE_BCRYPT "Enable bcrypt" ${ENABLE_LIBRARIES})

if (NOT ENABLE_BCRYPT)
    message(STATUS "Not using bcrypt")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/libbcrypt")

set(SRCS
    "${LIBRARY_DIR}/bcrypt.c"
    "${LIBRARY_DIR}/crypt_blowfish/crypt_blowfish.c"
    "${LIBRARY_DIR}/crypt_blowfish/crypt_gensalt.c"
    "${LIBRARY_DIR}/crypt_blowfish/wrapper.c"
)

add_library(_bcrypt ${SRCS})
target_include_directories(_bcrypt SYSTEM PUBLIC "${LIBRARY_DIR}")
# Avoid conflicts for crypt_r on FreeBSD [1]:
#
# - char *crypt_r(__const char *key, __const char *setting, void *data);
# - char *crypt_r(const char *, const char *, struct crypt_data *);
#
#   [1]: https://github.com/freebsd/freebsd-src/commit/5f521d7ba72145092ea23ff6081d8791ad6c1f9d
#
# NOTE: ow-crypt.h is unsed only internally, so PRIVATE is enough
target_compile_definitions(_bcrypt PRIVATE -D__SKIP_GNU)
add_library(ch_contrib::bcrypt ALIAS _bcrypt)
