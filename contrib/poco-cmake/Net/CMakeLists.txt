if (USE_INTERNAL_POCO_LIBRARY)
    set (SRCS
        "${LIBRARY_DIR}/Net/src/AbstractHTTPRequestHandler.cpp"
        "${LIBRARY_DIR}/Net/src/DatagramSocket.cpp"
        "${LIBRARY_DIR}/Net/src/DatagramSocketImpl.cpp"
        "${LIBRARY_DIR}/Net/src/DialogSocket.cpp"
        "${LIBRARY_DIR}/Net/src/DNS.cpp"
        "${LIBRARY_DIR}/Net/src/FilePartSource.cpp"
        "${LIBRARY_DIR}/Net/src/FTPClientSession.cpp"
        "${LIBRARY_DIR}/Net/src/FTPStreamFactory.cpp"
        "${LIBRARY_DIR}/Net/src/HostEntry.cpp"
        "${LIBRARY_DIR}/Net/src/HTMLForm.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPAuthenticationParams.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPBasicCredentials.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPBufferAllocator.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPChunkedStream.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPClientSession.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPCookie.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPCredentials.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPDigestCredentials.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPFixedLengthStream.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPHeaderStream.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPIOStream.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPMessage.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPRequest.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPRequestHandler.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPRequestHandlerFactory.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPResponse.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPServer.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPServerConnection.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPServerConnectionFactory.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPServerParams.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPServerRequest.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPServerRequestImpl.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPServerResponse.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPServerResponseImpl.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPServerSession.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPSession.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPSessionFactory.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPSessionInstantiator.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPStream.cpp"
        "${LIBRARY_DIR}/Net/src/HTTPStreamFactory.cpp"
        "${LIBRARY_DIR}/Net/src/ICMPClient.cpp"
        "${LIBRARY_DIR}/Net/src/ICMPEventArgs.cpp"
        "${LIBRARY_DIR}/Net/src/ICMPPacket.cpp"
        "${LIBRARY_DIR}/Net/src/ICMPPacketImpl.cpp"
        "${LIBRARY_DIR}/Net/src/ICMPSocket.cpp"
        "${LIBRARY_DIR}/Net/src/ICMPSocketImpl.cpp"
        "${LIBRARY_DIR}/Net/src/ICMPv4PacketImpl.cpp"
        "${LIBRARY_DIR}/Net/src/IPAddress.cpp"
        "${LIBRARY_DIR}/Net/src/IPAddressImpl.cpp"
        "${LIBRARY_DIR}/Net/src/MailMessage.cpp"
        "${LIBRARY_DIR}/Net/src/MailRecipient.cpp"
        "${LIBRARY_DIR}/Net/src/MailStream.cpp"
        "${LIBRARY_DIR}/Net/src/MediaType.cpp"
        "${LIBRARY_DIR}/Net/src/MessageHeader.cpp"
        "${LIBRARY_DIR}/Net/src/MulticastSocket.cpp"
        "${LIBRARY_DIR}/Net/src/MultipartReader.cpp"
        "${LIBRARY_DIR}/Net/src/MultipartWriter.cpp"
        "${LIBRARY_DIR}/Net/src/NameValueCollection.cpp"
        "${LIBRARY_DIR}/Net/src/Net.cpp"
        "${LIBRARY_DIR}/Net/src/NetException.cpp"
        "${LIBRARY_DIR}/Net/src/NetworkInterface.cpp"
        "${LIBRARY_DIR}/Net/src/NTPClient.cpp"
        "${LIBRARY_DIR}/Net/src/NTPEventArgs.cpp"
        "${LIBRARY_DIR}/Net/src/NTPPacket.cpp"
        "${LIBRARY_DIR}/Net/src/NullPartHandler.cpp"
        "${LIBRARY_DIR}/Net/src/OAuth10Credentials.cpp"
        "${LIBRARY_DIR}/Net/src/OAuth20Credentials.cpp"
        "${LIBRARY_DIR}/Net/src/PartHandler.cpp"
        "${LIBRARY_DIR}/Net/src/PartSource.cpp"
        "${LIBRARY_DIR}/Net/src/PartStore.cpp"
        "${LIBRARY_DIR}/Net/src/PollSet.cpp"
        "${LIBRARY_DIR}/Net/src/POP3ClientSession.cpp"
        "${LIBRARY_DIR}/Net/src/QuotedPrintableDecoder.cpp"
        "${LIBRARY_DIR}/Net/src/QuotedPrintableEncoder.cpp"
        "${LIBRARY_DIR}/Net/src/RawSocket.cpp"
        "${LIBRARY_DIR}/Net/src/RawSocketImpl.cpp"
        "${LIBRARY_DIR}/Net/src/RemoteSyslogChannel.cpp"
        "${LIBRARY_DIR}/Net/src/RemoteSyslogListener.cpp"
        "${LIBRARY_DIR}/Net/src/ServerSocket.cpp"
        "${LIBRARY_DIR}/Net/src/ServerSocketImpl.cpp"
        "${LIBRARY_DIR}/Net/src/SMTPChannel.cpp"
        "${LIBRARY_DIR}/Net/src/SMTPClientSession.cpp"
        "${LIBRARY_DIR}/Net/src/Socket.cpp"
        "${LIBRARY_DIR}/Net/src/SocketAddress.cpp"
        "${LIBRARY_DIR}/Net/src/SocketAddressImpl.cpp"
        "${LIBRARY_DIR}/Net/src/SocketImpl.cpp"
        "${LIBRARY_DIR}/Net/src/SocketNotification.cpp"
        "${LIBRARY_DIR}/Net/src/SocketNotifier.cpp"
        "${LIBRARY_DIR}/Net/src/SocketReactor.cpp"
        "${LIBRARY_DIR}/Net/src/SocketStream.cpp"
        "${LIBRARY_DIR}/Net/src/StreamSocket.cpp"
        "${LIBRARY_DIR}/Net/src/StreamSocketImpl.cpp"
        "${LIBRARY_DIR}/Net/src/StringPartSource.cpp"
        "${LIBRARY_DIR}/Net/src/TCPServer.cpp"
        "${LIBRARY_DIR}/Net/src/TCPServerConnection.cpp"
        "${LIBRARY_DIR}/Net/src/TCPServerConnectionFactory.cpp"
        "${LIBRARY_DIR}/Net/src/TCPServerDispatcher.cpp"
        "${LIBRARY_DIR}/Net/src/TCPServerParams.cpp"
        "${LIBRARY_DIR}/Net/src/WebSocket.cpp"
        "${LIBRARY_DIR}/Net/src/WebSocketImpl.cpp"
    )

    add_library (_poco_net ${SRCS})
    add_library (Poco::Net ALIAS _poco_net)

    if (OS_LINUX)
        target_compile_definitions (_poco_net PUBLIC POCO_HAVE_FD_EPOLL)
    elseif (OS_DARWIN OR OS_FREEBSD)
        target_compile_definitions (_poco_net PUBLIC POCO_HAVE_FD_POLL)
    endif ()

    if (COMPILER_CLANG)
        # clang-specific warnings
        target_compile_options (_poco_net
            PRIVATE
                -Wno-atomic-implicit-seq-cst
                -Wno-extra-semi-stmt
                -Wno-extra-semi
        )
    endif ()
    target_compile_options (_poco_net
        PRIVATE
            -Wno-deprecated
            -Wno-extra-semi
    )
    target_include_directories (_poco_net SYSTEM PUBLIC "${LIBRARY_DIR}/Net/include")
    target_link_libraries (_poco_net PUBLIC Poco::Foundation)
else ()
    add_library (Poco::Net UNKNOWN IMPORTED GLOBAL)

    find_library (LIBRARY_POCO_NET PocoNet)
    find_path (INCLUDE_POCO_NET Poco/Net/Net.h)
    set_target_properties (Poco::Net PROPERTIES IMPORTED_LOCATION ${LIBRARY_POCO_NET})
    set_target_properties (Poco::Net PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${INCLUDE_POCO_NET})

    message (STATUS "Using Poco::Net: ${LIBRARY_POCO_NET} ${INCLUDE_POCO_NET}")
endif ()
