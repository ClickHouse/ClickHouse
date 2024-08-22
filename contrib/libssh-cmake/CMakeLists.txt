option (ENABLE_SSH "Enable support for libssh" ${ENABLE_LIBRARIES})

if (NOT ENABLE_SSH)
    message(STATUS "Not using libssh")
    return()
endif()

# CMake variables needed by libssh_version.h.cmake, update them when you update libssh
set(libssh_VERSION_MAJOR 0)
set(libssh_VERSION_MINOR 9)
set(libssh_VERSION_PATCH 8)

set(LIB_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libssh")
set(LIB_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/libssh")

set(libssh_SRCS
    ${LIB_SOURCE_DIR}/src/agent.c
    ${LIB_SOURCE_DIR}/src/auth.c
    ${LIB_SOURCE_DIR}/src/base64.c
    ${LIB_SOURCE_DIR}/src/bignum.c
    ${LIB_SOURCE_DIR}/src/buffer.c
    ${LIB_SOURCE_DIR}/src/callbacks.c
    ${LIB_SOURCE_DIR}/src/chachapoly.c
    ${LIB_SOURCE_DIR}/src/channels.c
    ${LIB_SOURCE_DIR}/src/client.c
    ${LIB_SOURCE_DIR}/src/config.c
    ${LIB_SOURCE_DIR}/src/config_parser.c
    ${LIB_SOURCE_DIR}/src/connect.c
    ${LIB_SOURCE_DIR}/src/connector.c
    ${LIB_SOURCE_DIR}/src/curve25519.c
    ${LIB_SOURCE_DIR}/src/dh.c
    ${LIB_SOURCE_DIR}/src/ecdh.c
    ${LIB_SOURCE_DIR}/src/error.c
    ${LIB_SOURCE_DIR}/src/external/bcrypt_pbkdf.c
    ${LIB_SOURCE_DIR}/src/external/blowfish.c
    ${LIB_SOURCE_DIR}/src/external/chacha.c
    ${LIB_SOURCE_DIR}/src/external/poly1305.c
    ${LIB_SOURCE_DIR}/src/getpass.c
    ${LIB_SOURCE_DIR}/src/init.c
    ${LIB_SOURCE_DIR}/src/kdf.c
    ${LIB_SOURCE_DIR}/src/kex.c
    ${LIB_SOURCE_DIR}/src/known_hosts.c
    ${LIB_SOURCE_DIR}/src/knownhosts.c
    ${LIB_SOURCE_DIR}/src/legacy.c
    ${LIB_SOURCE_DIR}/src/log.c
    ${LIB_SOURCE_DIR}/src/match.c
    ${LIB_SOURCE_DIR}/src/messages.c
    ${LIB_SOURCE_DIR}/src/misc.c
    ${LIB_SOURCE_DIR}/src/options.c
    ${LIB_SOURCE_DIR}/src/packet.c
    ${LIB_SOURCE_DIR}/src/packet_cb.c
    ${LIB_SOURCE_DIR}/src/packet_crypt.c
    ${LIB_SOURCE_DIR}/src/pcap.c
    ${LIB_SOURCE_DIR}/src/pki.c
    ${LIB_SOURCE_DIR}/src/pki_container_openssh.c
    ${LIB_SOURCE_DIR}/src/pki_ed25519_common.c
    ${LIB_SOURCE_DIR}/src/poll.c
    ${LIB_SOURCE_DIR}/src/scp.c
    ${LIB_SOURCE_DIR}/src/session.c
    ${LIB_SOURCE_DIR}/src/socket.c
    ${LIB_SOURCE_DIR}/src/string.c
    ${LIB_SOURCE_DIR}/src/threads.c
    ${LIB_SOURCE_DIR}/src/token.c
    ${LIB_SOURCE_DIR}/src/wrapper.c
    # some files of libssh/src/ are missing - why?

    ${LIB_SOURCE_DIR}/src/threads/noop.c
    ${LIB_SOURCE_DIR}/src/threads/pthread.c
    # files missing - why?

    # LIBCRYPT specific
    ${LIB_SOURCE_DIR}/src/dh_crypto.c
    ${LIB_SOURCE_DIR}/src/ecdh_crypto.c
    ${LIB_SOURCE_DIR}/src/libcrypto.c
    ${LIB_SOURCE_DIR}/src/pki_crypto.c
    ${LIB_SOURCE_DIR}/src/threads/libcrypto.c

    ${LIB_SOURCE_DIR}/src/bind.c
    ${LIB_SOURCE_DIR}/src/bind_config.c
    ${LIB_SOURCE_DIR}/src/options.c
    ${LIB_SOURCE_DIR}/src/server.c
)

configure_file(${LIB_SOURCE_DIR}/include/libssh/libssh_version.h.cmake ${LIB_BINARY_DIR}/include/libssh/libssh_version.h @ONLY)

add_library(_ssh ${libssh_SRCS})
add_library(ch_contrib::ssh ALIAS _ssh)

target_link_libraries(_ssh PRIVATE OpenSSL::Crypto)

target_include_directories(_ssh PUBLIC "${LIB_SOURCE_DIR}/include" "${LIB_BINARY_DIR}/include")

# These headers need to be generated using the native build system on each platform.
if (OS_LINUX)
    if (ARCH_AMD64)
        if (USE_MUSL)
            target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/linux/x86-64-musl")
        else()
            target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/linux/x86-64")
        endif ()
    elseif (ARCH_AARCH64)
        if (USE_MUSL)
            target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/linux/aarch64-musl")
        else()
            target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/linux/aarch64")
        endif ()
    elseif (ARCH_PPC64LE)
        target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/linux/ppc64le")
    elseif (ARCH_S390X)
        target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/linux/s390x")
    elseif (ARCH_RISCV64)
        target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/linux/riscv64")
    elseif (ARCH_LOONGARCH64)
        target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/linux/loongarch64")
    else ()
        message(FATAL_ERROR "Platform is not supported")
    endif ()
elseif (OS_DARWIN)
    target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/darwin")
elseif (OS_FREEBSD)
    target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/freebsd")
else ()
    message(FATAL_ERROR "Platform is not supported")
endif()
