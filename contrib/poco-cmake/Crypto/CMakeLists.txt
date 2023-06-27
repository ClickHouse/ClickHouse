if (ENABLE_SSL)
    set (SRCS
        "${LIBRARY_DIR}/Crypto/src/Cipher.cpp"
        "${LIBRARY_DIR}/Crypto/src/CipherFactory.cpp"
        "${LIBRARY_DIR}/Crypto/src/CipherImpl.cpp"
        "${LIBRARY_DIR}/Crypto/src/CipherKey.cpp"
        "${LIBRARY_DIR}/Crypto/src/CipherKeyImpl.cpp"
        "${LIBRARY_DIR}/Crypto/src/CryptoException.cpp"
        "${LIBRARY_DIR}/Crypto/src/CryptoStream.cpp"
        "${LIBRARY_DIR}/Crypto/src/CryptoTransform.cpp"
        "${LIBRARY_DIR}/Crypto/src/DigestEngine.cpp"
        "${LIBRARY_DIR}/Crypto/src/ECDSADigestEngine.cpp"
        "${LIBRARY_DIR}/Crypto/src/ECKey.cpp"
        "${LIBRARY_DIR}/Crypto/src/ECKeyImpl.cpp"
        "${LIBRARY_DIR}/Crypto/src/EVPPKey.cpp"
        "${LIBRARY_DIR}/Crypto/src/KeyPair.cpp"
        "${LIBRARY_DIR}/Crypto/src/KeyPairImpl.cpp"
        "${LIBRARY_DIR}/Crypto/src/OpenSSLInitializer.cpp"
        "${LIBRARY_DIR}/Crypto/src/PKCS12Container.cpp"
        "${LIBRARY_DIR}/Crypto/src/RSACipherImpl.cpp"
        "${LIBRARY_DIR}/Crypto/src/RSADigestEngine.cpp"
        "${LIBRARY_DIR}/Crypto/src/RSAKey.cpp"
        "${LIBRARY_DIR}/Crypto/src/RSAKeyImpl.cpp"
        "${LIBRARY_DIR}/Crypto/src/X509Certificate.cpp"
    )

    add_library (_poco_crypto ${SRCS})
    add_library (Poco::Crypto ALIAS _poco_crypto)

    target_compile_options (_poco_crypto PRIVATE -Wno-newline-eof)
    target_include_directories (_poco_crypto SYSTEM PUBLIC "${LIBRARY_DIR}/Crypto/include")
    target_link_libraries (_poco_crypto PUBLIC Poco::Foundation OpenSSL::SSL OpenSSL::Crypto)

    message (STATUS "Using Poco::Crypto")
else ()
    add_library (_poco_crypto INTERFACE)
    add_library (Poco::Crypto ALIAS _poco_crypto)

    message (STATUS "Not using Poco::Crypto")
endif ()
