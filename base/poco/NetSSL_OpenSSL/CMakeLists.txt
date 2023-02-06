if (ENABLE_SSL)
    set (SRCS
        src/AcceptCertificateHandler.cpp
        src/CertificateHandlerFactory.cpp
        src/CertificateHandlerFactoryMgr.cpp
        src/ConsoleCertificateHandler.cpp
        src/Context.cpp
        src/HTTPSClientSession.cpp
        src/HTTPSSessionInstantiator.cpp
        src/HTTPSStreamFactory.cpp
        src/InvalidCertificateHandler.cpp
        src/KeyConsoleHandler.cpp
        src/KeyFileHandler.cpp
        src/PrivateKeyFactory.cpp
        src/PrivateKeyFactoryMgr.cpp
        src/PrivateKeyPassphraseHandler.cpp
        src/RejectCertificateHandler.cpp
        src/SecureServerSocket.cpp
        src/SecureServerSocketImpl.cpp
        src/SecureSMTPClientSession.cpp
        src/SecureSocketImpl.cpp
        src/SecureStreamSocket.cpp
        src/SecureStreamSocketImpl.cpp
        src/Session.cpp
        src/SSLException.cpp
        src/SSLManager.cpp
        src/Utility.cpp
        src/VerificationErrorArgs.cpp
        src/X509Certificate.cpp
    )

    add_library (_poco_net_ssl ${SRCS})
    add_library (Poco::Net::SSL ALIAS _poco_net_ssl)

    target_include_directories (_poco_net_ssl SYSTEM PUBLIC "include")
    target_link_libraries (_poco_net_ssl PUBLIC Poco::Crypto Poco::Net Poco::Util)
else ()
    add_library (_poco_net_ssl INTERFACE)
    add_library (Poco::Net::SSL ALIAS _poco_net_ssl)

    message (STATUS "Not using Poco::Net::SSL")
endif ()
