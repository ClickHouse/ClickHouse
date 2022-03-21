if (ENABLE_SSL)
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
    add_library (_poco_net_ssl INTERFACE)
    add_library (Poco::Net::SSL ALIAS _poco_net_ssl)

    message (STATUS "Not using Poco::Net::SSL")
endif ()
