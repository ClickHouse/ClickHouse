if (ENABLE_SSL)
    file (GLOB SRCS src/*.cpp)

    add_library (_poco_net_ssl ${SRCS})
    add_library (Poco::Net::SSL ALIAS _poco_net_ssl)

    # CA certificates embedded into the binary. They are used as a fallback when no CA certificates
    # can be found on the filesystem, e.g. when running in a container built "from scratch".
    # The bundle is the root store maintained in the gRPC submodule (the Chromium root store in PEM format).
    option (ENABLE_EMBEDDED_CA_CERTIFICATES "Embed CA certificates into the binary as a fallback for when the system has none" ON)
    if (ENABLE_EMBEDDED_CA_CERTIFICATES)
        set (EMBEDDED_CA_CERTIFICATES_FILE "${ClickHouse_SOURCE_DIR}/contrib/grpc/etc/roots.pem")
        if (EXISTS "${EMBEDDED_CA_CERTIFICATES_FILE}")
            set_source_files_properties (src/EmbeddedCertificates.cpp PROPERTIES
                COMPILE_DEFINITIONS POCO_EMBEDDED_CA_CERTIFICATES=1
                OBJECT_DEPENDS "${EMBEDDED_CA_CERTIFICATES_FILE}")
        elseif (USE_MUSL)
            # Musl builds are expected to be self-contained: do not let the feature silently disappear.
            message (FATAL_ERROR "Embedded CA certificates require ${EMBEDDED_CA_CERTIFICATES_FILE}; check out the grpc submodule or set ENABLE_EMBEDDED_CA_CERTIFICATES=0")
        else ()
            message (STATUS "Not embedding CA certificates: ${EMBEDDED_CA_CERTIFICATES_FILE} does not exist (the grpc submodule is not checked out)")
        endif ()
    endif ()

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
