if (ENABLE_SSL)
    file (GLOB SRCS src/*.cpp)

    add_library (_poco_net_ssl ${SRCS})
    add_library (Poco::Net::SSL ALIAS _poco_net_ssl)

    # TODO: remove these warning exclusions
    target_compile_options (_poco_net_ssl
        PRIVATE
            -Wno-cast-qual
            -Wno-covered-switch-default
            -Wno-deprecated-copy-with-user-provided-dtor
            -Wno-deprecated-dynamic-exception-spec
            -Wno-extra-semi
            -Wno-extra-semi-stmt
            -Wno-implicit-fallthrough
            -Wno-old-style-cast
            -Wno-shorten-64-to-32
            -Wno-sign-compare
            -Wno-unused-exception-parameter
            -Wno-unused-macros
            -Wno-unused-parameter
            -Wno-zero-as-null-pointer-constant
    )
    target_include_directories (_poco_net_ssl SYSTEM PUBLIC "include")
    target_link_libraries (_poco_net_ssl PUBLIC Poco::Net Poco::Util OpenSSL::SSL OpenSSL::Crypto)
else ()
    add_library (_poco_net_ssl INTERFACE)
    add_library (Poco::Net::SSL ALIAS _poco_net_ssl)

    message (STATUS "Not using Poco::Net::SSL")
endif ()
