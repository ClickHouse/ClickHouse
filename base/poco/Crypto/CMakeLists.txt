if (ENABLE_SSL)
    file (GLOB SRCS src/*.cpp)

    add_library (_poco_crypto ${SRCS})
    add_library (Poco::Crypto ALIAS _poco_crypto)

    # TODO: remove these warning exclusions
    target_compile_options (_poco_crypto
        PRIVATE
            -Wno-covered-switch-default
            -Wno-deprecated-dynamic-exception-spec
            -Wno-extra-semi-stmt
            -Wno-missing-noreturn
            -Wno-newline-eof
            -Wno-old-style-cast
            -Wno-shadow
            -Wno-shorten-64-to-32
            -Wno-sign-compare
            -Wno-suggest-destructor-override
            -Wno-suggest-override
            -Wno-unreachable-code-return
            -Wno-unused-parameter
            -Wno-zero-as-null-pointer-constant
            -Wno-used-but-marked-unused
    )
    target_include_directories (_poco_crypto SYSTEM PUBLIC "include")
    target_link_libraries (_poco_crypto PUBLIC Poco::Foundation OpenSSL::SSL OpenSSL::Crypto)

    message (STATUS "Using Poco::Crypto")
else ()
    add_library (_poco_crypto INTERFACE)
    add_library (Poco::Crypto ALIAS _poco_crypto)

    message (STATUS "Not using Poco::Crypto")
endif ()
