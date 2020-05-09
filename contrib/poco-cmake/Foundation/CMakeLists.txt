if (USE_INTERNAL_POCO_LIBRARY)
    # Foundation (pcre)

    set (SRCS_PCRE
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

    add_library (_poco_foundation_pcre ${SRCS_PCRE})
    add_library (Poco::Foundation::PCRE ALIAS _poco_foundation_pcre)

    target_compile_options (_poco_foundation_pcre PRIVATE -Wno-sign-compare)

    # Foundation

    set (SRCS
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

    add_library (_poco_foundation ${SRCS})
    add_library (Poco::Foundation ALIAS _poco_foundation)

    if (COMPILER_GCC)
        target_compile_options (_poco_foundation
            PRIVATE
                -Wno-suggest-override
        )
    elseif (COMPILER_CLANG)
        target_compile_options (_poco_foundation
            PRIVATE
                -Wno-atomic-implicit-seq-cst
                -Wno-deprecated
                -Wno-extra-semi-stmt
                -Wno-zero-as-null-pointer-constant
                -Wno-implicit-int-float-conversion
                -Wno-thread-safety-analysis
                -Wno-thread-safety-negative
        )
    endif ()
    target_compile_options (_poco_foundation
        PRIVATE
            -Wno-sign-compare
            -Wno-unused-parameter
    )
    target_compile_definitions (_poco_foundation
        PRIVATE
            POCO_UNBUNDLED
            POCO_UNBUNDLED_ZLIB
        PUBLIC
            POCO_ENABLE_CPP11
            POCO_OS_FAMILY_UNIX
    )
    target_include_directories (_poco_foundation SYSTEM PUBLIC ${LIBRARY_DIR}/Foundation/include)
    target_link_libraries (_poco_foundation PRIVATE Poco::Foundation::PCRE zlib)
else ()
    add_library (Poco::Foundation UNKNOWN IMPORTED GLOBAL)

    find_library (LIBRARY_POCO_FOUNDATION PocoFoundation)
    find_path (INCLUDE_POCO_FOUNDATION Poco/Foundation.h)
    set_target_properties (Poco::Foundation PROPERTIES IMPORTED_LOCATION ${LIBRARY_POCO_FOUNDATION})
    set_target_properties (Poco::Foundation PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${INCLUDE_POCO_FOUNDATION})

    message (STATUS "Using Poco::Foundation: ${LIBRARY_POCO_FOUNDATION} ${INCLUDE_POCO_FOUNDATION}")
endif ()
