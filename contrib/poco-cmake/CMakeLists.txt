option (ENABLE_POCO_NETSSL "Use Poco NetSSL library" ${ENABLE_LIBRARIES})

if (NOT ENABLE_SSL)
    set (ENABLE_POCO_NETSSL OFF)
endif ()

option (USE_INTERNAL_POCO_LIBRARY "Use internal Poco library" ${NOT_UNBUNDLED})

if (USE_INTERNAL_POCO_LIBRARY)
    set (LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/poco)

    # Poco::Crypto
    if (ENABLE_POCO_NETSSL)
        set (SRCS_CRYPTO
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

        add_library (poco_crypto ${SRCS_CRYPTO})

        target_compile_options (poco_crypto PUBLIC -Wno-newline-eof)
        target_include_directories (poco_crypto PUBLIC ${LIBRARY_DIR}/Crypto/include)
        target_link_libraries (poco_crypto PUBLIC poco_foundation ssl)
    endif ()

    # Poco::Data

    set (SRCS_DATA
        ${LIBRARY_DIR}/Data/src/AbstractBinder.cpp
        ${LIBRARY_DIR}/Data/src/AbstractBinding.cpp
        ${LIBRARY_DIR}/Data/src/AbstractExtraction.cpp
        ${LIBRARY_DIR}/Data/src/AbstractExtractor.cpp
        ${LIBRARY_DIR}/Data/src/AbstractPreparation.cpp
        ${LIBRARY_DIR}/Data/src/AbstractPreparator.cpp
        ${LIBRARY_DIR}/Data/src/ArchiveStrategy.cpp
        ${LIBRARY_DIR}/Data/src/Bulk.cpp
        ${LIBRARY_DIR}/Data/src/Connector.cpp
        ${LIBRARY_DIR}/Data/src/DataException.cpp
        ${LIBRARY_DIR}/Data/src/Date.cpp
        ${LIBRARY_DIR}/Data/src/DynamicLOB.cpp
        ${LIBRARY_DIR}/Data/src/Limit.cpp
        ${LIBRARY_DIR}/Data/src/MetaColumn.cpp
        ${LIBRARY_DIR}/Data/src/PooledSessionHolder.cpp
        ${LIBRARY_DIR}/Data/src/PooledSessionImpl.cpp
        ${LIBRARY_DIR}/Data/src/Position.cpp
        ${LIBRARY_DIR}/Data/src/Range.cpp
        ${LIBRARY_DIR}/Data/src/RecordSet.cpp
        ${LIBRARY_DIR}/Data/src/Row.cpp
        ${LIBRARY_DIR}/Data/src/RowFilter.cpp
        ${LIBRARY_DIR}/Data/src/RowFormatter.cpp
        ${LIBRARY_DIR}/Data/src/RowIterator.cpp
        ${LIBRARY_DIR}/Data/src/Session.cpp
        ${LIBRARY_DIR}/Data/src/SessionFactory.cpp
        ${LIBRARY_DIR}/Data/src/SessionImpl.cpp
        ${LIBRARY_DIR}/Data/src/SessionPool.cpp
        ${LIBRARY_DIR}/Data/src/SessionPoolContainer.cpp
        ${LIBRARY_DIR}/Data/src/SimpleRowFormatter.cpp
        ${LIBRARY_DIR}/Data/src/SQLChannel.cpp
        ${LIBRARY_DIR}/Data/src/Statement.cpp
        ${LIBRARY_DIR}/Data/src/StatementCreator.cpp
        ${LIBRARY_DIR}/Data/src/StatementImpl.cpp
        ${LIBRARY_DIR}/Data/src/Time.cpp
        ${LIBRARY_DIR}/Data/src/Transaction.cpp
    )

    add_library (poco_data ${SRCS_DATA})

    if (COMPILER_GCC)
        target_compile_options (poco_data PUBLIC -Wno-deprecated-copy)
    endif ()
    target_include_directories (poco_data PUBLIC ${LIBRARY_DIR}/Data/include)
    target_link_libraries (poco_data PUBLIC poco_foundation)

    # Poco::Foundation

    set (SRCS_FOUNDATION
        ${LIBRARY_DIR}/Foundation/src/AbstractObserver.cpp
        ${LIBRARY_DIR}/Foundation/src/ActiveDispatcher.cpp
        ${LIBRARY_DIR}/Foundation/src/ArchiveStrategy.cpp
        ${LIBRARY_DIR}/Foundation/src/Ascii.cpp
        ${LIBRARY_DIR}/Foundation/src/ASCIIEncoding.cpp
        ${LIBRARY_DIR}/Foundation/src/AsyncChannel.cpp
        ${LIBRARY_DIR}/Foundation/src/AtomicCounter.cpp
        ${LIBRARY_DIR}/Foundation/src/Base32Decoder.cpp
        ${LIBRARY_DIR}/Foundation/src/Base32Encoder.cpp
        ${LIBRARY_DIR}/Foundation/src/Base64Decoder.cpp
        ${LIBRARY_DIR}/Foundation/src/Base64Encoder.cpp
        ${LIBRARY_DIR}/Foundation/src/BinaryReader.cpp
        ${LIBRARY_DIR}/Foundation/src/BinaryWriter.cpp
        ${LIBRARY_DIR}/Foundation/src/Bugcheck.cpp
        ${LIBRARY_DIR}/Foundation/src/ByteOrder.cpp
        ${LIBRARY_DIR}/Foundation/src/Channel.cpp
        ${LIBRARY_DIR}/Foundation/src/Checksum.cpp
        ${LIBRARY_DIR}/Foundation/src/Clock.cpp
        ${LIBRARY_DIR}/Foundation/src/Condition.cpp
        ${LIBRARY_DIR}/Foundation/src/Configurable.cpp
        ${LIBRARY_DIR}/Foundation/src/ConsoleChannel.cpp
        ${LIBRARY_DIR}/Foundation/src/CountingStream.cpp
        ${LIBRARY_DIR}/Foundation/src/DateTime.cpp
        ${LIBRARY_DIR}/Foundation/src/DateTimeFormat.cpp
        ${LIBRARY_DIR}/Foundation/src/DateTimeFormatter.cpp
        ${LIBRARY_DIR}/Foundation/src/DateTimeParser.cpp
        ${LIBRARY_DIR}/Foundation/src/Debugger.cpp
        ${LIBRARY_DIR}/Foundation/src/DeflatingStream.cpp
        ${LIBRARY_DIR}/Foundation/src/DigestEngine.cpp
        ${LIBRARY_DIR}/Foundation/src/DigestStream.cpp
        ${LIBRARY_DIR}/Foundation/src/DirectoryIterator.cpp
        ${LIBRARY_DIR}/Foundation/src/DirectoryIteratorStrategy.cpp
        ${LIBRARY_DIR}/Foundation/src/DirectoryWatcher.cpp
        ${LIBRARY_DIR}/Foundation/src/Environment.cpp
        ${LIBRARY_DIR}/Foundation/src/Error.cpp
        ${LIBRARY_DIR}/Foundation/src/ErrorHandler.cpp
        ${LIBRARY_DIR}/Foundation/src/Event.cpp
        ${LIBRARY_DIR}/Foundation/src/EventArgs.cpp
        ${LIBRARY_DIR}/Foundation/src/EventChannel.cpp
        ${LIBRARY_DIR}/Foundation/src/Exception.cpp
        ${LIBRARY_DIR}/Foundation/src/FIFOBufferStream.cpp
        ${LIBRARY_DIR}/Foundation/src/File.cpp
        ${LIBRARY_DIR}/Foundation/src/FileChannel.cpp
        ${LIBRARY_DIR}/Foundation/src/FileStream.cpp
        ${LIBRARY_DIR}/Foundation/src/FileStreamFactory.cpp
        ${LIBRARY_DIR}/Foundation/src/Format.cpp
        ${LIBRARY_DIR}/Foundation/src/Formatter.cpp
        ${LIBRARY_DIR}/Foundation/src/FormattingChannel.cpp
        ${LIBRARY_DIR}/Foundation/src/FPEnvironment.cpp
        ${LIBRARY_DIR}/Foundation/src/Glob.cpp
        ${LIBRARY_DIR}/Foundation/src/Hash.cpp
        ${LIBRARY_DIR}/Foundation/src/HashStatistic.cpp
        ${LIBRARY_DIR}/Foundation/src/HexBinaryDecoder.cpp
        ${LIBRARY_DIR}/Foundation/src/HexBinaryEncoder.cpp
        ${LIBRARY_DIR}/Foundation/src/InflatingStream.cpp
        ${LIBRARY_DIR}/Foundation/src/JSONString.cpp
        ${LIBRARY_DIR}/Foundation/src/Latin1Encoding.cpp
        ${LIBRARY_DIR}/Foundation/src/Latin2Encoding.cpp
        ${LIBRARY_DIR}/Foundation/src/Latin9Encoding.cpp
        ${LIBRARY_DIR}/Foundation/src/LineEndingConverter.cpp
        ${LIBRARY_DIR}/Foundation/src/LocalDateTime.cpp
        ${LIBRARY_DIR}/Foundation/src/LogFile.cpp
        ${LIBRARY_DIR}/Foundation/src/Logger.cpp
        ${LIBRARY_DIR}/Foundation/src/LoggingFactory.cpp
        ${LIBRARY_DIR}/Foundation/src/LoggingRegistry.cpp
        ${LIBRARY_DIR}/Foundation/src/LogStream.cpp
        ${LIBRARY_DIR}/Foundation/src/Manifest.cpp
        ${LIBRARY_DIR}/Foundation/src/MD4Engine.cpp
        ${LIBRARY_DIR}/Foundation/src/MD5Engine.cpp
        ${LIBRARY_DIR}/Foundation/src/MemoryPool.cpp
        ${LIBRARY_DIR}/Foundation/src/MemoryStream.cpp
        ${LIBRARY_DIR}/Foundation/src/Message.cpp
        ${LIBRARY_DIR}/Foundation/src/Mutex.cpp
        ${LIBRARY_DIR}/Foundation/src/NamedEvent.cpp
        ${LIBRARY_DIR}/Foundation/src/NamedMutex.cpp
        ${LIBRARY_DIR}/Foundation/src/NestedDiagnosticContext.cpp
        ${LIBRARY_DIR}/Foundation/src/Notification.cpp
        ${LIBRARY_DIR}/Foundation/src/NotificationCenter.cpp
        ${LIBRARY_DIR}/Foundation/src/NotificationQueue.cpp
        ${LIBRARY_DIR}/Foundation/src/NullChannel.cpp
        ${LIBRARY_DIR}/Foundation/src/NullStream.cpp
        ${LIBRARY_DIR}/Foundation/src/NumberFormatter.cpp
        ${LIBRARY_DIR}/Foundation/src/NumberParser.cpp
        ${LIBRARY_DIR}/Foundation/src/NumericString.cpp
        ${LIBRARY_DIR}/Foundation/src/Path.cpp
        ${LIBRARY_DIR}/Foundation/src/PatternFormatter.cpp
        ${LIBRARY_DIR}/Foundation/src/Pipe.cpp
        ${LIBRARY_DIR}/Foundation/src/PipeImpl.cpp
        ${LIBRARY_DIR}/Foundation/src/PipeStream.cpp
        ${LIBRARY_DIR}/Foundation/src/PriorityNotificationQueue.cpp
        ${LIBRARY_DIR}/Foundation/src/Process.cpp
        ${LIBRARY_DIR}/Foundation/src/PurgeStrategy.cpp
        ${LIBRARY_DIR}/Foundation/src/Random.cpp
        ${LIBRARY_DIR}/Foundation/src/RandomStream.cpp
        ${LIBRARY_DIR}/Foundation/src/RefCountedObject.cpp
        ${LIBRARY_DIR}/Foundation/src/RegularExpression.cpp
        ${LIBRARY_DIR}/Foundation/src/RotateStrategy.cpp
        ${LIBRARY_DIR}/Foundation/src/Runnable.cpp
        ${LIBRARY_DIR}/Foundation/src/RWLock.cpp
        ${LIBRARY_DIR}/Foundation/src/Semaphore.cpp
        ${LIBRARY_DIR}/Foundation/src/SHA1Engine.cpp
        ${LIBRARY_DIR}/Foundation/src/SharedLibrary.cpp
        ${LIBRARY_DIR}/Foundation/src/SharedMemory.cpp
        ${LIBRARY_DIR}/Foundation/src/SignalHandler.cpp
        ${LIBRARY_DIR}/Foundation/src/SimpleFileChannel.cpp
        ${LIBRARY_DIR}/Foundation/src/SortedDirectoryIterator.cpp
        ${LIBRARY_DIR}/Foundation/src/SplitterChannel.cpp
        ${LIBRARY_DIR}/Foundation/src/Stopwatch.cpp
        ${LIBRARY_DIR}/Foundation/src/StreamChannel.cpp
        ${LIBRARY_DIR}/Foundation/src/StreamConverter.cpp
        ${LIBRARY_DIR}/Foundation/src/StreamCopier.cpp
        ${LIBRARY_DIR}/Foundation/src/StreamTokenizer.cpp
        ${LIBRARY_DIR}/Foundation/src/String.cpp
        ${LIBRARY_DIR}/Foundation/src/StringTokenizer.cpp
        ${LIBRARY_DIR}/Foundation/src/SynchronizedObject.cpp
        ${LIBRARY_DIR}/Foundation/src/SyslogChannel.cpp
        ${LIBRARY_DIR}/Foundation/src/Task.cpp
        ${LIBRARY_DIR}/Foundation/src/TaskManager.cpp
        ${LIBRARY_DIR}/Foundation/src/TaskNotification.cpp
        ${LIBRARY_DIR}/Foundation/src/TeeStream.cpp
        ${LIBRARY_DIR}/Foundation/src/TemporaryFile.cpp
        ${LIBRARY_DIR}/Foundation/src/TextBufferIterator.cpp
        ${LIBRARY_DIR}/Foundation/src/TextConverter.cpp
        ${LIBRARY_DIR}/Foundation/src/TextEncoding.cpp
        ${LIBRARY_DIR}/Foundation/src/TextIterator.cpp
        ${LIBRARY_DIR}/Foundation/src/Thread.cpp
        ${LIBRARY_DIR}/Foundation/src/ThreadLocal.cpp
        ${LIBRARY_DIR}/Foundation/src/ThreadPool.cpp
        ${LIBRARY_DIR}/Foundation/src/ThreadTarget.cpp
        ${LIBRARY_DIR}/Foundation/src/TimedNotificationQueue.cpp
        ${LIBRARY_DIR}/Foundation/src/Timer.cpp
        ${LIBRARY_DIR}/Foundation/src/Timespan.cpp
        ${LIBRARY_DIR}/Foundation/src/Timestamp.cpp
        ${LIBRARY_DIR}/Foundation/src/Timezone.cpp
        ${LIBRARY_DIR}/Foundation/src/Token.cpp
        ${LIBRARY_DIR}/Foundation/src/Unicode.cpp
        ${LIBRARY_DIR}/Foundation/src/UnicodeConverter.cpp
        ${LIBRARY_DIR}/Foundation/src/URI.cpp
        ${LIBRARY_DIR}/Foundation/src/URIStreamFactory.cpp
        ${LIBRARY_DIR}/Foundation/src/URIStreamOpener.cpp
        ${LIBRARY_DIR}/Foundation/src/UTF16Encoding.cpp
        ${LIBRARY_DIR}/Foundation/src/UTF32Encoding.cpp
        ${LIBRARY_DIR}/Foundation/src/UTF8Encoding.cpp
        ${LIBRARY_DIR}/Foundation/src/UTF8String.cpp
        ${LIBRARY_DIR}/Foundation/src/UUID.cpp
        ${LIBRARY_DIR}/Foundation/src/UUIDGenerator.cpp
        ${LIBRARY_DIR}/Foundation/src/Var.cpp
        ${LIBRARY_DIR}/Foundation/src/VarHolder.cpp
        ${LIBRARY_DIR}/Foundation/src/VarIterator.cpp
        ${LIBRARY_DIR}/Foundation/src/Void.cpp
        ${LIBRARY_DIR}/Foundation/src/Windows1250Encoding.cpp
        ${LIBRARY_DIR}/Foundation/src/Windows1251Encoding.cpp
        ${LIBRARY_DIR}/Foundation/src/Windows1252Encoding.cpp
    )

    add_library (poco_foundation ${SRCS_FOUNDATION})

    if (COMPILER_GCC)
        target_compile_options (poco_foundation
            PUBLIC
                -Wno-suggest-override
        )
    elseif (COMPILER_CLANG)
        target_compile_options (poco_foundation
            PUBLIC
                -Wno-atomic-implicit-seq-cst
                -Wno-deprecated
                -Wno-extra-semi-stmt
                -Wno-zero-as-null-pointer-constant
        )
    endif ()
    target_compile_options (poco_foundation
        PUBLIC
            -Wno-sign-compare
            -Wno-unused-parameter
    )
    target_compile_definitions (poco_foundation
        PRIVATE
            POCO_UNBUNDLED
            POCO_UNBUNDLED_ZLIB
        PUBLIC
            POCO_ENABLE_CPP11
            POCO_OS_FAMILY_UNIX
    )
    target_include_directories (poco_foundation PUBLIC ${LIBRARY_DIR}/Foundation/include)
    target_link_libraries (poco_foundation PRIVATE poco_foundation_pcre zlib)

    # Poco::Foundation (pcre)

    set (SRCS_FOUNDATION_PCRE
        ${LIBRARY_DIR}/Foundation/src/pcre_config.c
		${LIBRARY_DIR}/Foundation/src/pcre_byte_order.c
		${LIBRARY_DIR}/Foundation/src/pcre_chartables.c
		${LIBRARY_DIR}/Foundation/src/pcre_compile.c
		${LIBRARY_DIR}/Foundation/src/pcre_exec.c
		${LIBRARY_DIR}/Foundation/src/pcre_fullinfo.c
		${LIBRARY_DIR}/Foundation/src/pcre_globals.c
		${LIBRARY_DIR}/Foundation/src/pcre_maketables.c
		${LIBRARY_DIR}/Foundation/src/pcre_newline.c
		${LIBRARY_DIR}/Foundation/src/pcre_ord2utf8.c
		${LIBRARY_DIR}/Foundation/src/pcre_study.c
		${LIBRARY_DIR}/Foundation/src/pcre_tables.c
		${LIBRARY_DIR}/Foundation/src/pcre_dfa_exec.c
		${LIBRARY_DIR}/Foundation/src/pcre_get.c
		${LIBRARY_DIR}/Foundation/src/pcre_jit_compile.c
		${LIBRARY_DIR}/Foundation/src/pcre_refcount.c
		${LIBRARY_DIR}/Foundation/src/pcre_string_utils.c
		${LIBRARY_DIR}/Foundation/src/pcre_version.c
		${LIBRARY_DIR}/Foundation/src/pcre_ucd.c
		${LIBRARY_DIR}/Foundation/src/pcre_valid_utf8.c
		${LIBRARY_DIR}/Foundation/src/pcre_xclass.c
    )

    add_library (poco_foundation_pcre ${SRCS_FOUNDATION_PCRE})

    target_compile_options (poco_foundation_pcre
        PRIVATE
            -Wno-sign-compare
    )

    # Poco::JSON

    set (SRCS_JSON
        ${LIBRARY_DIR}/JSON/src/Array.cpp
        ${LIBRARY_DIR}/JSON/src/Handler.cpp
        ${LIBRARY_DIR}/JSON/src/JSONException.cpp
        ${LIBRARY_DIR}/JSON/src/Object.cpp
        ${LIBRARY_DIR}/JSON/src/ParseHandler.cpp
        ${LIBRARY_DIR}/JSON/src/Parser.cpp
        ${LIBRARY_DIR}/JSON/src/ParserImpl.cpp
        ${LIBRARY_DIR}/JSON/src/PrintHandler.cpp
        ${LIBRARY_DIR}/JSON/src/Query.cpp
        ${LIBRARY_DIR}/JSON/src/Stringifier.cpp
        ${LIBRARY_DIR}/JSON/src/Template.cpp
        ${LIBRARY_DIR}/JSON/src/TemplateCache.cpp
    )

    add_library (poco_json ${SRCS_JSON})

    target_include_directories (poco_json PUBLIC ${LIBRARY_DIR}/JSON/include)
    target_link_libraries (poco_json PUBLIC poco_foundation poco_json_pdjson)

    # Poco::JSON (pdjson)

    set (SRCS_JSON_PDJSON
        ${LIBRARY_DIR}/JSON/src/pdjson.c
    )

    add_library (poco_json_pdjson ${SRCS_JSON_PDJSON})

    # Poco::Net

    set (SRCS_NET
        ${LIBRARY_DIR}/Net/src/AbstractHTTPRequestHandler.cpp
        ${LIBRARY_DIR}/Net/src/DatagramSocket.cpp
        ${LIBRARY_DIR}/Net/src/DatagramSocketImpl.cpp
        ${LIBRARY_DIR}/Net/src/DialogSocket.cpp
        ${LIBRARY_DIR}/Net/src/DNS.cpp
        ${LIBRARY_DIR}/Net/src/FilePartSource.cpp
        ${LIBRARY_DIR}/Net/src/FTPClientSession.cpp
        ${LIBRARY_DIR}/Net/src/FTPStreamFactory.cpp
        ${LIBRARY_DIR}/Net/src/HostEntry.cpp
        ${LIBRARY_DIR}/Net/src/HTMLForm.cpp
        ${LIBRARY_DIR}/Net/src/HTTPAuthenticationParams.cpp
        ${LIBRARY_DIR}/Net/src/HTTPBasicCredentials.cpp
        ${LIBRARY_DIR}/Net/src/HTTPBufferAllocator.cpp
        ${LIBRARY_DIR}/Net/src/HTTPChunkedStream.cpp
        ${LIBRARY_DIR}/Net/src/HTTPClientSession.cpp
        ${LIBRARY_DIR}/Net/src/HTTPCookie.cpp
        ${LIBRARY_DIR}/Net/src/HTTPCredentials.cpp
        ${LIBRARY_DIR}/Net/src/HTTPDigestCredentials.cpp
        ${LIBRARY_DIR}/Net/src/HTTPFixedLengthStream.cpp
        ${LIBRARY_DIR}/Net/src/HTTPHeaderStream.cpp
        ${LIBRARY_DIR}/Net/src/HTTPIOStream.cpp
        ${LIBRARY_DIR}/Net/src/HTTPMessage.cpp
        ${LIBRARY_DIR}/Net/src/HTTPRequest.cpp
        ${LIBRARY_DIR}/Net/src/HTTPRequestHandler.cpp
        ${LIBRARY_DIR}/Net/src/HTTPRequestHandlerFactory.cpp
        ${LIBRARY_DIR}/Net/src/HTTPResponse.cpp
        ${LIBRARY_DIR}/Net/src/HTTPServer.cpp
        ${LIBRARY_DIR}/Net/src/HTTPServerConnection.cpp
        ${LIBRARY_DIR}/Net/src/HTTPServerConnectionFactory.cpp
        ${LIBRARY_DIR}/Net/src/HTTPServerParams.cpp
        ${LIBRARY_DIR}/Net/src/HTTPServerRequest.cpp
        ${LIBRARY_DIR}/Net/src/HTTPServerRequestImpl.cpp
        ${LIBRARY_DIR}/Net/src/HTTPServerResponse.cpp
        ${LIBRARY_DIR}/Net/src/HTTPServerResponseImpl.cpp
        ${LIBRARY_DIR}/Net/src/HTTPServerSession.cpp
        ${LIBRARY_DIR}/Net/src/HTTPSession.cpp
        ${LIBRARY_DIR}/Net/src/HTTPSessionFactory.cpp
        ${LIBRARY_DIR}/Net/src/HTTPSessionInstantiator.cpp
        ${LIBRARY_DIR}/Net/src/HTTPStream.cpp
        ${LIBRARY_DIR}/Net/src/HTTPStreamFactory.cpp
        ${LIBRARY_DIR}/Net/src/ICMPClient.cpp
        ${LIBRARY_DIR}/Net/src/ICMPEventArgs.cpp
        ${LIBRARY_DIR}/Net/src/ICMPPacket.cpp
        ${LIBRARY_DIR}/Net/src/ICMPPacketImpl.cpp
        ${LIBRARY_DIR}/Net/src/ICMPSocket.cpp
        ${LIBRARY_DIR}/Net/src/ICMPSocketImpl.cpp
        ${LIBRARY_DIR}/Net/src/ICMPv4PacketImpl.cpp
        ${LIBRARY_DIR}/Net/src/IPAddress.cpp
        ${LIBRARY_DIR}/Net/src/IPAddressImpl.cpp
        ${LIBRARY_DIR}/Net/src/MailMessage.cpp
        ${LIBRARY_DIR}/Net/src/MailRecipient.cpp
        ${LIBRARY_DIR}/Net/src/MailStream.cpp
        ${LIBRARY_DIR}/Net/src/MediaType.cpp
        ${LIBRARY_DIR}/Net/src/MessageHeader.cpp
        ${LIBRARY_DIR}/Net/src/MulticastSocket.cpp
        ${LIBRARY_DIR}/Net/src/MultipartReader.cpp
        ${LIBRARY_DIR}/Net/src/MultipartWriter.cpp
        ${LIBRARY_DIR}/Net/src/NameValueCollection.cpp
        ${LIBRARY_DIR}/Net/src/Net.cpp
        ${LIBRARY_DIR}/Net/src/NetException.cpp
        ${LIBRARY_DIR}/Net/src/NetworkInterface.cpp
        ${LIBRARY_DIR}/Net/src/NTPClient.cpp
        ${LIBRARY_DIR}/Net/src/NTPEventArgs.cpp
        ${LIBRARY_DIR}/Net/src/NTPPacket.cpp
        ${LIBRARY_DIR}/Net/src/NullPartHandler.cpp
        ${LIBRARY_DIR}/Net/src/OAuth10Credentials.cpp
        ${LIBRARY_DIR}/Net/src/OAuth20Credentials.cpp
        ${LIBRARY_DIR}/Net/src/PartHandler.cpp
        ${LIBRARY_DIR}/Net/src/PartSource.cpp
        ${LIBRARY_DIR}/Net/src/PartStore.cpp
        ${LIBRARY_DIR}/Net/src/PollSet.cpp
        ${LIBRARY_DIR}/Net/src/POP3ClientSession.cpp
        ${LIBRARY_DIR}/Net/src/QuotedPrintableDecoder.cpp
        ${LIBRARY_DIR}/Net/src/QuotedPrintableEncoder.cpp
        ${LIBRARY_DIR}/Net/src/RawSocket.cpp
        ${LIBRARY_DIR}/Net/src/RawSocketImpl.cpp
        ${LIBRARY_DIR}/Net/src/RemoteSyslogChannel.cpp
        ${LIBRARY_DIR}/Net/src/RemoteSyslogListener.cpp
        ${LIBRARY_DIR}/Net/src/ServerSocket.cpp
        ${LIBRARY_DIR}/Net/src/ServerSocketImpl.cpp
        ${LIBRARY_DIR}/Net/src/SMTPChannel.cpp
        ${LIBRARY_DIR}/Net/src/SMTPClientSession.cpp
        ${LIBRARY_DIR}/Net/src/Socket.cpp
        ${LIBRARY_DIR}/Net/src/SocketAddress.cpp
        ${LIBRARY_DIR}/Net/src/SocketAddressImpl.cpp
        ${LIBRARY_DIR}/Net/src/SocketImpl.cpp
        ${LIBRARY_DIR}/Net/src/SocketNotification.cpp
        ${LIBRARY_DIR}/Net/src/SocketNotifier.cpp
        ${LIBRARY_DIR}/Net/src/SocketReactor.cpp
        ${LIBRARY_DIR}/Net/src/SocketStream.cpp
        ${LIBRARY_DIR}/Net/src/StreamSocket.cpp
        ${LIBRARY_DIR}/Net/src/StreamSocketImpl.cpp
        ${LIBRARY_DIR}/Net/src/StringPartSource.cpp
        ${LIBRARY_DIR}/Net/src/TCPServer.cpp
        ${LIBRARY_DIR}/Net/src/TCPServerConnection.cpp
        ${LIBRARY_DIR}/Net/src/TCPServerConnectionFactory.cpp
        ${LIBRARY_DIR}/Net/src/TCPServerDispatcher.cpp
        ${LIBRARY_DIR}/Net/src/TCPServerParams.cpp
        ${LIBRARY_DIR}/Net/src/WebSocket.cpp
        ${LIBRARY_DIR}/Net/src/WebSocketImpl.cpp
    )

    add_library (poco_net ${SRCS_NET})

    if (COMPILER_CLANG)
        # clang-specific warnings
        target_compile_options (poco_net
            PRIVATE
                -Wno-atomic-implicit-seq-cst
                -Wno-extra-semi-stmt
            PUBLIC
                -Wno-extra-semi
        )
    endif ()
    target_compile_options (poco_net
        PRIVATE
            -Wno-deprecated
            -Wno-extra-semi
    )
    target_include_directories (poco_net PUBLIC ${LIBRARY_DIR}/Net/include)
    target_link_libraries (poco_net PUBLIC poco_foundation)

    # Poco::NetSSL
    if (ENABLE_POCO_NETSSL)
        set (SRCS_NETSSL
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/AcceptCertificateHandler.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/CertificateHandlerFactory.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/CertificateHandlerFactoryMgr.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/ConsoleCertificateHandler.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/Context.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/HTTPSClientSession.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/HTTPSSessionInstantiator.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/HTTPSStreamFactory.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/InvalidCertificateHandler.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/KeyConsoleHandler.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/KeyFileHandler.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/PrivateKeyFactory.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/PrivateKeyFactoryMgr.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/PrivateKeyPassphraseHandler.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/RejectCertificateHandler.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/SecureServerSocket.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/SecureServerSocketImpl.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/SecureSMTPClientSession.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/SecureSocketImpl.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/SecureStreamSocket.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/SecureStreamSocketImpl.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/Session.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/SSLException.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/SSLManager.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/Utility.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/VerificationErrorArgs.cpp
            ${LIBRARY_DIR}/NetSSL_OpenSSL/src/X509Certificate.cpp
        )

        add_library (poco_netssl ${SRCS_NETSSL})

        target_compile_definitions (poco_netssl PUBLIC USE_POCO_NETSSL=1)
        target_include_directories (poco_netssl PUBLIC ${LIBRARY_DIR}/NetSSL_OpenSSL/include)
        target_link_libraries (poco_netssl PUBLIC poco_crypto poco_net poco_util)
    endif ()

    # Poco::Util

    set (SRCS_UTIL
        ${LIBRARY_DIR}/Util/src/AbstractConfiguration.cpp
        ${LIBRARY_DIR}/Util/src/Application.cpp
        ${LIBRARY_DIR}/Util/src/ConfigurationMapper.cpp
        ${LIBRARY_DIR}/Util/src/ConfigurationView.cpp
        ${LIBRARY_DIR}/Util/src/FilesystemConfiguration.cpp
        ${LIBRARY_DIR}/Util/src/HelpFormatter.cpp
        ${LIBRARY_DIR}/Util/src/IniFileConfiguration.cpp
        ${LIBRARY_DIR}/Util/src/IntValidator.cpp
        ${LIBRARY_DIR}/Util/src/JSONConfiguration.cpp
        ${LIBRARY_DIR}/Util/src/LayeredConfiguration.cpp
        ${LIBRARY_DIR}/Util/src/LoggingConfigurator.cpp
        ${LIBRARY_DIR}/Util/src/LoggingSubsystem.cpp
        ${LIBRARY_DIR}/Util/src/MapConfiguration.cpp
        ${LIBRARY_DIR}/Util/src/Option.cpp
        ${LIBRARY_DIR}/Util/src/OptionCallback.cpp
        ${LIBRARY_DIR}/Util/src/OptionException.cpp
        ${LIBRARY_DIR}/Util/src/OptionProcessor.cpp
        ${LIBRARY_DIR}/Util/src/OptionSet.cpp
        ${LIBRARY_DIR}/Util/src/PropertyFileConfiguration.cpp
        ${LIBRARY_DIR}/Util/src/RegExpValidator.cpp
        ${LIBRARY_DIR}/Util/src/ServerApplication.cpp
        ${LIBRARY_DIR}/Util/src/Subsystem.cpp
        ${LIBRARY_DIR}/Util/src/SystemConfiguration.cpp
        ${LIBRARY_DIR}/Util/src/Timer.cpp
        ${LIBRARY_DIR}/Util/src/TimerTask.cpp
        ${LIBRARY_DIR}/Util/src/Validator.cpp
        ${LIBRARY_DIR}/Util/src/XMLConfiguration.cpp
    )

    add_library (poco_util ${SRCS_UTIL})

    target_include_directories (poco_util PUBLIC ${LIBRARY_DIR}/Util/include)
    target_link_libraries (poco_util PUBLIC poco_json poco_xml)

    # Poco::XML

    set (SRCS_XML
        ${LIBRARY_DIR}/XML/src/AbstractContainerNode.cpp
        ${LIBRARY_DIR}/XML/src/AbstractNode.cpp
        ${LIBRARY_DIR}/XML/src/Attr.cpp
        ${LIBRARY_DIR}/XML/src/Attributes.cpp
        ${LIBRARY_DIR}/XML/src/AttributesImpl.cpp
        ${LIBRARY_DIR}/XML/src/AttrMap.cpp
        ${LIBRARY_DIR}/XML/src/CDATASection.cpp
        ${LIBRARY_DIR}/XML/src/CharacterData.cpp
        ${LIBRARY_DIR}/XML/src/ChildNodesList.cpp
        ${LIBRARY_DIR}/XML/src/Comment.cpp
        ${LIBRARY_DIR}/XML/src/ContentHandler.cpp
        ${LIBRARY_DIR}/XML/src/DeclHandler.cpp
        ${LIBRARY_DIR}/XML/src/DefaultHandler.cpp
        ${LIBRARY_DIR}/XML/src/Document.cpp
        ${LIBRARY_DIR}/XML/src/DocumentEvent.cpp
        ${LIBRARY_DIR}/XML/src/DocumentFragment.cpp
        ${LIBRARY_DIR}/XML/src/DocumentType.cpp
        ${LIBRARY_DIR}/XML/src/DOMBuilder.cpp
        ${LIBRARY_DIR}/XML/src/DOMException.cpp
        ${LIBRARY_DIR}/XML/src/DOMImplementation.cpp
        ${LIBRARY_DIR}/XML/src/DOMObject.cpp
        ${LIBRARY_DIR}/XML/src/DOMParser.cpp
        ${LIBRARY_DIR}/XML/src/DOMSerializer.cpp
        ${LIBRARY_DIR}/XML/src/DOMWriter.cpp
        ${LIBRARY_DIR}/XML/src/DTDHandler.cpp
        ${LIBRARY_DIR}/XML/src/DTDMap.cpp
        ${LIBRARY_DIR}/XML/src/Element.cpp
        ${LIBRARY_DIR}/XML/src/ElementsByTagNameList.cpp
        ${LIBRARY_DIR}/XML/src/Entity.cpp
        ${LIBRARY_DIR}/XML/src/EntityReference.cpp
        ${LIBRARY_DIR}/XML/src/EntityResolver.cpp
        ${LIBRARY_DIR}/XML/src/EntityResolverImpl.cpp
        ${LIBRARY_DIR}/XML/src/ErrorHandler.cpp
        ${LIBRARY_DIR}/XML/src/Event.cpp
        ${LIBRARY_DIR}/XML/src/EventDispatcher.cpp
        ${LIBRARY_DIR}/XML/src/EventException.cpp
        ${LIBRARY_DIR}/XML/src/EventListener.cpp
        ${LIBRARY_DIR}/XML/src/EventTarget.cpp
        ${LIBRARY_DIR}/XML/src/InputSource.cpp
        ${LIBRARY_DIR}/XML/src/LexicalHandler.cpp
        ${LIBRARY_DIR}/XML/src/Locator.cpp
        ${LIBRARY_DIR}/XML/src/LocatorImpl.cpp
        ${LIBRARY_DIR}/XML/src/MutationEvent.cpp
        ${LIBRARY_DIR}/XML/src/Name.cpp
        ${LIBRARY_DIR}/XML/src/NamedNodeMap.cpp
        ${LIBRARY_DIR}/XML/src/NamePool.cpp
        ${LIBRARY_DIR}/XML/src/NamespaceStrategy.cpp
        ${LIBRARY_DIR}/XML/src/NamespaceSupport.cpp
        ${LIBRARY_DIR}/XML/src/Node.cpp
        ${LIBRARY_DIR}/XML/src/NodeAppender.cpp
        ${LIBRARY_DIR}/XML/src/NodeFilter.cpp
        ${LIBRARY_DIR}/XML/src/NodeIterator.cpp
        ${LIBRARY_DIR}/XML/src/NodeList.cpp
        ${LIBRARY_DIR}/XML/src/Notation.cpp
        ${LIBRARY_DIR}/XML/src/ParserEngine.cpp
        ${LIBRARY_DIR}/XML/src/ProcessingInstruction.cpp
        ${LIBRARY_DIR}/XML/src/QName.cpp
        ${LIBRARY_DIR}/XML/src/SAXException.cpp
        ${LIBRARY_DIR}/XML/src/SAXParser.cpp
        ${LIBRARY_DIR}/XML/src/Text.cpp
        ${LIBRARY_DIR}/XML/src/TreeWalker.cpp
        ${LIBRARY_DIR}/XML/src/ValueTraits.cpp
        ${LIBRARY_DIR}/XML/src/WhitespaceFilter.cpp
        ${LIBRARY_DIR}/XML/src/XMLException.cpp
        ${LIBRARY_DIR}/XML/src/XMLFilter.cpp
        ${LIBRARY_DIR}/XML/src/XMLFilterImpl.cpp
        ${LIBRARY_DIR}/XML/src/XMLReader.cpp
        ${LIBRARY_DIR}/XML/src/XMLStreamParser.cpp
        ${LIBRARY_DIR}/XML/src/XMLStreamParserException.cpp
        ${LIBRARY_DIR}/XML/src/XMLString.cpp
        ${LIBRARY_DIR}/XML/src/XMLWriter.cpp

        # expat
        ${LIBRARY_DIR}/XML/src/xmlparse.cpp
    )

    add_library (poco_xml ${SRCS_XML})

    target_compile_options (poco_xml PUBLIC -Wno-old-style-cast)
    target_include_directories (poco_xml PUBLIC ${LIBRARY_DIR}/XML/include)
    target_link_libraries (poco_xml PUBLIC poco_foundation poco_xml_expat)

    # Poco::XML (expat)

    set (SRCS_XML_EXPAT
        ${LIBRARY_DIR}/XML/src/xmlrole.c
        ${LIBRARY_DIR}/XML/src/xmltok_impl.c
        ${LIBRARY_DIR}/XML/src/xmltok_ns.c
        ${LIBRARY_DIR}/XML/src/xmltok.c
    )

    add_library (poco_xml_expat ${SRCS_XML_EXPAT})

    target_include_directories (poco_xml_expat PUBLIC ${LIBRARY_DIR}/XML/include)
else ()
endif ()
