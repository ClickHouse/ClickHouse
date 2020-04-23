if (ENABLE_SSL)
    if (USE_INTERNAL_POCO_LIBRARY)
        set (SRCS
            ${LIBRARY_DIR}/Crypto/src/Cipher.cpp
            ${LIBRARY_DIR}/Crypto/src/CipherFactory.cpp
            ${LIBRARY_DIR}/Crypto/src/CipherImpl.cpp
            ${LIBRARY_DIR}/Crypto/src/CipherKey.cpp
            ${LIBRARY_DIR}/Crypto/src/CipherKeyImpl.cpp
            ${LIBRARY_DIR}/Crypto/src/CryptoException.cpp
            ${LIBRARY_DIR}/Crypto/src/CryptoStream.cpp
            ${LIBRARY_DIR}/Crypto/src/CryptoTransform.cpp
            ${LIBRARY_DIR}/Crypto/src/DigestEngine.cpp
            ${LIBRARY_DIR}/Crypto/src/ECDSADigestEngine.cpp
            ${LIBRARY_DIR}/Crypto/src/ECKey.cpp
            ${LIBRARY_DIR}/Crypto/src/ECKeyImpl.cpp
            ${LIBRARY_DIR}/Crypto/src/EVPPKey.cpp
            ${LIBRARY_DIR}/Crypto/src/KeyPair.cpp
            ${LIBRARY_DIR}/Crypto/src/KeyPairImpl.cpp
            ${LIBRARY_DIR}/Crypto/src/OpenSSLInitializer.cpp
            ${LIBRARY_DIR}/Crypto/src/PKCS12Container.cpp
            ${LIBRARY_DIR}/Crypto/src/RSACipherImpl.cpp
            ${LIBRARY_DIR}/Crypto/src/RSADigestEngine.cpp
            ${LIBRARY_DIR}/Crypto/src/RSAKey.cpp
            ${LIBRARY_DIR}/Crypto/src/RSAKeyImpl.cpp
            ${LIBRARY_DIR}/Crypto/src/X509Certificate.cpp
        )

        add_library (poco_crypto ${SRCS})

        target_compile_options (poco_crypto PUBLIC -Wno-newline-eof)
        target_include_directories (poco_crypto PUBLIC ${LIBRARY_DIR}/Crypto/include)
        target_link_libraries (poco_crypto PUBLIC poco_foundation ssl)
    endif ()
endif ()
