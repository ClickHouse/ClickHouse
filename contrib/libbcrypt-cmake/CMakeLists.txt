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
add_library(ch_contrib::bcrypt ALIAS _bcrypt)
