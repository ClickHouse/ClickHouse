option(ENABLE_THRIFT "Enable Thrift" ${ENABLE_LIBRARIES})

if (NOT ENABLE_THRIFT)
    message (STATUS "Not using thrift")
    return()
endif()

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/thrift/lib/cpp")
set(thriftcpp_SOURCES
        "${LIBRARY_DIR}/src/thrift/TApplicationException.cpp"
        "${LIBRARY_DIR}/src/thrift/TOutput.cpp"
        "${LIBRARY_DIR}/src/thrift/async/TAsyncChannel.cpp"
        "${LIBRARY_DIR}/src/thrift/async/TAsyncProtocolProcessor.cpp"
        "${LIBRARY_DIR}/src/thrift/async/TConcurrentClientSyncInfo.h"
        "${LIBRARY_DIR}/src/thrift/async/TConcurrentClientSyncInfo.cpp"
        "${LIBRARY_DIR}/src/thrift/concurrency/ThreadManager.cpp"
        "${LIBRARY_DIR}/src/thrift/concurrency/TimerManager.cpp"
        "${LIBRARY_DIR}/src/thrift/processor/PeekProcessor.cpp"
        "${LIBRARY_DIR}/src/thrift/protocol/TBase64Utils.cpp"
        "${LIBRARY_DIR}/src/thrift/protocol/TDebugProtocol.cpp"
        "${LIBRARY_DIR}/src/thrift/protocol/TJSONProtocol.cpp"
        "${LIBRARY_DIR}/src/thrift/protocol/TMultiplexedProtocol.cpp"
        "${LIBRARY_DIR}/src/thrift/protocol/TProtocol.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/TTransportException.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/TFDTransport.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/TSimpleFileTransport.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/THttpTransport.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/THttpClient.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/THttpServer.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/TSocket.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/TSocketPool.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/TServerSocket.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/TTransportUtils.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/TBufferTransports.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/SocketCommon.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/TWebSocketServer.cpp"
        "${LIBRARY_DIR}/src/thrift/server/TConnectedClient.cpp"
        "${LIBRARY_DIR}/src/thrift/server/TServerFramework.cpp"
        "${LIBRARY_DIR}/src/thrift/server/TSimpleServer.cpp"
        "${LIBRARY_DIR}/src/thrift/server/TThreadPoolServer.cpp"
        "${LIBRARY_DIR}/src/thrift/server/TThreadedServer.cpp"
        )
set(thriftcpp_threads_SOURCES
        "${LIBRARY_DIR}/src/thrift/concurrency/ThreadFactory.cpp"
        "${LIBRARY_DIR}/src/thrift/concurrency/Thread.cpp"
        "${LIBRARY_DIR}/src/thrift/concurrency/Monitor.cpp"
        "${LIBRARY_DIR}/src/thrift/concurrency/Mutex.cpp"
        )

set (HAVE_ARPA_INET_H 1)
set (HAVE_FCNTL_H 1)
set (HAVE_GETOPT_H 1)
set (HAVE_INTTYPES_H 1)
set (HAVE_NETDB_H 1)
set (HAVE_NETINET_IN_H 1)
set (HAVE_SIGNAL_H 1)
set (HAVE_STDINT_H 1)
set (HAVE_UNISTD_H 1)
set (HAVE_PTHREAD_H 1)
set (HAVE_SYS_IOCTL_H 1)
set (HAVE_SYS_PARAM_H 1)
set (HAVE_SYS_RESOURCE_H 1)
set (HAVE_SYS_SOCKET_H 1)
set (HAVE_SYS_STAT_H 1)
set (HAVE_SYS_TIME_H 1)
set (HAVE_SYS_UN_H 1)
set (HAVE_POLL_H 1)
set (HAVE_SYS_POLL_H 1)
set (HAVE_SYS_SELECT_H 1)
set (HAVE_SCHED_H 1)
set (HAVE_STRING_H 1)
set (HAVE_STRINGS_H 1)
set (HAVE_GETHOSTBYNAME 1)
set (HAVE_STRERROR_R 1)
set (HAVE_SCHED_GET_PRIORITY_MAX 1)
set (HAVE_SCHED_GET_PRIORITY_MIN 1)

if (OS_LINUX AND NOT USE_MUSL)
    set (STRERROR_R_CHAR_P 1)
endif ()

# generate a config.h file
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/build/cmake/config.h.in" "${CMAKE_CURRENT_BINARY_DIR}/thrift/config.h")

include_directories("${CMAKE_CURRENT_BINARY_DIR}")

add_library(_thrift ${thriftcpp_SOURCES} ${thriftcpp_threads_SOURCES})
add_library(ch_contrib::thrift ALIAS _thrift)
target_include_directories(_thrift SYSTEM PUBLIC "${ClickHouse_SOURCE_DIR}/contrib/thrift/lib/cpp/src" ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries (_thrift PUBLIC OpenSSL::SSL boost::headers_only)
