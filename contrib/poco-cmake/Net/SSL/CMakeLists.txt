if (ENABLE_SSL)
    if (USE_INTERNAL_POCO_LIBRARY)
        set (SRCS
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/AcceptCertificateHandler.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/CertificateHandlerFactory.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/CertificateHandlerFactoryMgr.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/ConsoleCertificateHandler.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/Context.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/HTTPSClientSession.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/HTTPSSessionInstantiator.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/HTTPSStreamFactory.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/InvalidCertificateHandler.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/KeyConsoleHandler.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/KeyFileHandler.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/PrivateKeyFactory.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/PrivateKeyFactoryMgr.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/PrivateKeyPassphraseHandler.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/RejectCertificateHandler.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/SecureServerSocket.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/SecureServerSocketImpl.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/SecureSMTPClientSession.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/SecureSocketImpl.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/SecureStreamSocket.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/SecureStreamSocketImpl.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/Session.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/SSLException.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/SSLManager.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/Utility.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/VerificationErrorArgs.cpp"
            "${LIBRARY_DIR}/NetSSL_OpenSSL/src/X509Certificate.cpp"
        )

        add_library (_poco_net_ssl ${SRCS})
        add_library (Poco::Net::SSL ALIAS _poco_net_ssl)

        target_include_directories (_poco_net_ssl SYSTEM PUBLIC "${LIBRARY_DIR}/NetSSL_OpenSSL/include")
        target_link_libraries (_poco_net_ssl PUBLIC Poco::Crypto Poco::Net Poco::Util)
    else ()
        add_library (Poco::Net::SSL UNKNOWN IMPORTED GLOBAL)

        find_library (LIBRARY_POCO_NET_SSL PocoNetSSL)
        find_path (INCLUDE_POCO_NET_SSL Poco/Net/NetSSL.h)
        set_target_properties (Poco::Net::SSL PROPERTIES IMPORTED_LOCATION ${LIBRARY_POCO_NET_SSL})
        set_target_properties (Poco::Net::SSL PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${INCLUDE_POCO_NET_SSL})

        message (STATUS "Using Poco::Net::SSL: ${LIBRARY_POCO_NET_SSL} ${INCLUDE_POCO_NET_SSL}")
    endif ()
else ()
    add_library (_poco_net_ssl INTERFACE)
    add_library (Poco::Net::SSL ALIAS _poco_net_ssl)

    message (STATUS "Not using Poco::Net::SSL")
endif ()
