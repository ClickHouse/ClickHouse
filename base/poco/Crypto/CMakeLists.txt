if (ENABLE_SSL)
    set (SRCS
        src/Cipher.cpp
        src/CipherFactory.cpp
        src/CipherImpl.cpp
        src/CipherKey.cpp
        src/CipherKeyImpl.cpp
        src/CryptoException.cpp
        src/CryptoStream.cpp
        src/CryptoTransform.cpp
        src/DigestEngine.cpp
        src/ECDSADigestEngine.cpp
        src/ECKey.cpp
        src/ECKeyImpl.cpp
        src/EVPPKey.cpp
        src/KeyPair.cpp
        src/KeyPairImpl.cpp
        src/OpenSSLInitializer.cpp
        src/PKCS12Container.cpp
        src/RSACipherImpl.cpp
        src/RSADigestEngine.cpp
        src/RSAKey.cpp
        src/RSAKeyImpl.cpp
        src/X509Certificate.cpp
    )

    add_library (_poco_crypto ${SRCS})
    add_library (Poco::Crypto ALIAS _poco_crypto)

    target_compile_options (_poco_crypto PRIVATE -Wno-newline-eof)
    target_include_directories (_poco_crypto SYSTEM PUBLIC "include")
    target_link_libraries (_poco_crypto PUBLIC Poco::Foundation OpenSSL::SSL OpenSSL::Crypto)

    message (STATUS "Using Poco::Crypto")
else ()
    add_library (_poco_crypto INTERFACE)
    add_library (Poco::Crypto ALIAS _poco_crypto)

    message (STATUS "Not using Poco::Crypto")
endif ()
