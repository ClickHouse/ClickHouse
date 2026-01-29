option (ENABLE_LIBCOTP "Enable libcotp" ${ENABLE_LIBRARIES})

if (NOT ENABLE_LIBCOTP OR NOT ENABLE_SSL OR OS_FREEBSD)
  # OpenSSL is required for libcotp for hmac calculations
  # FreeBSD: skip due to libcotp build issues
  message (STATUS "Not using libcotp")
  return()
endif()

set (LIBCOTP_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libcotp")
set (LIBCOTP_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/libcotp")

set(SRCS
    "${LIBCOTP_SOURCE_DIR}/src/otp.c"
    "${LIBCOTP_SOURCE_DIR}/src/utils/base32.c"
    "${LIBCOTP_SOURCE_DIR}/src/utils/whmac_openssl.c"
)

add_library (_libcotp ${SRCS})

file(MAKE_DIRECTORY "${LIBCOTP_BINARY_DIR}/include")
file(COPY "${LIBCOTP_SOURCE_DIR}/src/" DESTINATION "${LIBCOTP_BINARY_DIR}/include" FILES_MATCHING PATTERN "*.h")
target_include_directories(_libcotp SYSTEM BEFORE PUBLIC "${LIBCOTP_BINARY_DIR}/include")
target_link_libraries(_libcotp PRIVATE OpenSSL::Crypto)

add_library(ch_contrib::libcotp ALIAS _libcotp)
