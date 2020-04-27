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
    else ()
        find_library(LIBRARY_POCO_CRYPTO PocoCrypto)
        find_path(INCLUDE_POCO_CRYPTO Poco/Crypto/Crypto.h)

        add_library (poco_crypto UNKNOWN IMPORTED)
        set_property (TARGET poco_crypto PROPERTY IMPORTED_LOCATION ${LIBRARY_POCO_CRYPTO})
        set_property (TARGET poco_crypto PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${INCLUDE_POCO_CRYPTO})

        set (CMAKE_REQUIRED_LIBRARIES poco_crypto)
        check_cxx_source_compiles (
            "
            #include <Poco/Crypto/Crypto.h>

            int main() {
                Poco::Crypto::initializeCrypto();
            }
            "
            EXTERNAL_POCO_CRYPTO_WORKS
        )

        if (NOT EXTERNAL_POCO_CRYPTO_WORKS)
            message (FATAL_ERROR "Poco::Crypto is unusable: ${LIBRARY_POCO_CRYPTO} ${INCLUDE_POCO_CRYPTO}")
        endif ()
    endif ()

    message (STATUS "Using Poco::Crypto")
else ()
    add_library (poco_crypto INTERFACE)

    message (STATUS "Not using Poco::Crypto")
endif ()
