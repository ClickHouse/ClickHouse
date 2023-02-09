if (ENABLE_SSL)
    file (GLOB SRCS src/*.cpp)

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
