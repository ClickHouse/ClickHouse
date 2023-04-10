set(ACME_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/acme-lw/lib")
set(ACME_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/acme-lw/lib")

set(SRCS
    "${ACME_SOURCE_DIR}/acme-lw.cpp"
    "${ACME_SOURCE_DIR}/acme-exception.cpp"
)

set(CMAKE_REQUIRED_LIBRARIES OpenSSL::Crypto)

add_library(_acme-lw ${SRCS})

target_include_directories(_acme-lw SYSTEM BEFORE PUBLIC "${ACME_SOURCE_DIR}/../internal" "${CURL_INCLUDE_DIR}" "${OPENSSL_INCLUDE_DIR}")
target_link_libraries(_acme-lw PUBLIC OpenSSL::Crypto OpenSSL::SSL)

add_library(ch_contrib::acme-lw ALIAS _acme-lw)
