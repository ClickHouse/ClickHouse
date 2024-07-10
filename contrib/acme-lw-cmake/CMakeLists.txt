option (ENABLE_ACME_LW "Enable acme-lw library" ${ENABLE_LIBRARIES})

if (NOT ENABLE_ACME_LW)
    message(STATUS "Not using acme-lw")
    return()
endif()

if (ENABLE_ACME_LW)
    if(NOT TARGET OpenSSL::Crypto)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use acme-lw without OpenSSL")
    endif()
endif()

set(ACME_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/acme-lw")
set(ACME_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/acme-lw")

set(SRCS
    "${ACME_SOURCE_DIR}/lib/acme-lw.cpp"
    "${ACME_SOURCE_DIR}/lib/acme-exception.cpp"
    "${ACME_SOURCE_DIR}/internal/http.cpp"
)

add_library(_acme-lw ${SRCS})

target_link_libraries(_acme-lw PRIVATE OpenSSL::Crypto OpenSSL::Crypto)
target_link_libraries(_acme-lw PRIVATE ch_contrib::curl)

target_include_directories(_acme-lw SYSTEM BEFORE PUBLIC "${ACME_SOURCE_DIR}/lib" "${ACME_SOURCE_DIR}/internal" "${OPENSSL_INCLUDE_DIR}")

add_library(ch_contrib::acme-lw ALIAS _acme-lw)
