if(NOT ENABLE_SSL)
  message(STATUS "libcotp: DISABLED because ENABLE_SSL=OFF")
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
