include_directories(${BORINGSSL_SOURCE_DIR}/ssl/../include)

add_library(
  boringssl

  ${BORINGSSL_SOURCE_DIR}/ssl/bio_ssl.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/d1_both.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/d1_lib.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/d1_pkt.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/d1_srtp.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/dtls_method.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/dtls_record.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/handoff.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/handshake.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/handshake_client.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/handshake_server.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/s3_both.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/s3_lib.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/s3_pkt.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/ssl_aead_ctx.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/ssl_asn1.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/ssl_buffer.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/ssl_cert.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/ssl_cipher.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/ssl_file.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/ssl_key_share.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/ssl_lib.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/ssl_privkey.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/ssl_session.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/ssl_stat.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/ssl_transcript.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/ssl_versions.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/ssl_x509.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/t1_enc.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/t1_lib.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/tls_method.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/tls_record.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/tls13_both.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/tls13_client.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/tls13_enc.cc
  ${BORINGSSL_SOURCE_DIR}/ssl/tls13_server.cc
)

add_dependencies(boringssl global_target)

target_link_libraries(boringssl boringssl_crypto)