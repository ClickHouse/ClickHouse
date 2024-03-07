set(LIBSSH_LINK_LIBRARIES
  ${LIBSSH_LINK_LIBRARIES}
  OpenSSL::Crypto
)

set(libssh_SRCS
  ${LIB_SOURCE_DIR}/src/agent.c
  ${LIB_SOURCE_DIR}/src/auth.c
  ${LIB_SOURCE_DIR}/src/base64.c
  ${LIB_SOURCE_DIR}/src/bignum.c
  ${LIB_SOURCE_DIR}/src/buffer.c
  ${LIB_SOURCE_DIR}/src/callbacks.c
  ${LIB_SOURCE_DIR}/src/channels.c
  ${LIB_SOURCE_DIR}/src/client.c
  ${LIB_SOURCE_DIR}/src/config.c
  ${LIB_SOURCE_DIR}/src/connect.c
  ${LIB_SOURCE_DIR}/src/connector.c
  ${LIB_SOURCE_DIR}/src/curve25519.c
  ${LIB_SOURCE_DIR}/src/dh.c
  ${LIB_SOURCE_DIR}/src/ecdh.c
  ${LIB_SOURCE_DIR}/src/error.c
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
  ${LIB_SOURCE_DIR}/src/poll.c
  ${LIB_SOURCE_DIR}/src/session.c
  ${LIB_SOURCE_DIR}/src/scp.c
  ${LIB_SOURCE_DIR}/src/socket.c
  ${LIB_SOURCE_DIR}/src/string.c
  ${LIB_SOURCE_DIR}/src/threads.c
  ${LIB_SOURCE_DIR}/src/wrapper.c
  ${LIB_SOURCE_DIR}/src/external/bcrypt_pbkdf.c
  ${LIB_SOURCE_DIR}/src/external/blowfish.c
  ${LIB_SOURCE_DIR}/src/external/chacha.c
  ${LIB_SOURCE_DIR}/src/external/poly1305.c
  ${LIB_SOURCE_DIR}/src/chachapoly.c
  ${LIB_SOURCE_DIR}/src/config_parser.c
  ${LIB_SOURCE_DIR}/src/token.c
  ${LIB_SOURCE_DIR}/src/pki_ed25519_common.c
)

set(libssh_SRCS
    ${libssh_SRCS}
    ${LIB_SOURCE_DIR}/src/threads/noop.c
    ${LIB_SOURCE_DIR}/src/threads/pthread.c
)

# LIBCRYPT specific
set(libssh_SRCS
    ${libssh_SRCS}
    ${LIB_SOURCE_DIR}/src/threads/libcrypto.c
    ${LIB_SOURCE_DIR}/src/pki_crypto.c
    ${LIB_SOURCE_DIR}/src/ecdh_crypto.c
    ${LIB_SOURCE_DIR}/src/libcrypto.c
    ${LIB_SOURCE_DIR}/src/dh_crypto.c
)

if (NOT (ENABLE_OPENSSL OR ENABLE_OPENSSL_DYNAMIC))
    add_compile_definitions(USE_BORINGSSL=1)
endif()

set(libssh_SRCS
${libssh_SRCS}
${LIB_SOURCE_DIR}/src/options.c
${LIB_SOURCE_DIR}/src/server.c
${LIB_SOURCE_DIR}/src/bind.c
${LIB_SOURCE_DIR}/src/bind_config.c
)


add_library(_ssh STATIC ${libssh_SRCS})

target_include_directories(_ssh PRIVATE ${LIB_BINARY_DIR})
target_include_directories(_ssh PUBLIC "${LIB_SOURCE_DIR}/include" "${LIB_BINARY_DIR}/include")
target_link_libraries(_ssh
                      PRIVATE ${LIBSSH_LINK_LIBRARIES})

add_library(ch_contrib::ssh ALIAS _ssh)

target_compile_options(_ssh
                     PRIVATE
                        ${DEFAULT_C_COMPILE_FLAGS}
                        -D_GNU_SOURCE)
