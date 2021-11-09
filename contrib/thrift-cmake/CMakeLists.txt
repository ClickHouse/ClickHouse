# === thrift
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
        "${LIBRARY_DIR}/src/thrift/concurrency/Util.cpp"
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

include("${ClickHouse_SOURCE_DIR}/contrib/thrift/build/cmake/ConfigureChecks.cmake") # makes config.h

add_library(${THRIFT_LIBRARY} ${thriftcpp_SOURCES} ${thriftcpp_threads_SOURCES})
target_include_directories(${THRIFT_LIBRARY} SYSTEM PUBLIC "${THRIFT_INCLUDE_DIR}" ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries (${THRIFT_LIBRARY} PUBLIC boost::headers_only)

# set(COMPILER_DIR "${ClickHouse_SOURCE_DIR}/contrib/thrift/compiler/cpp")
# configure_file(${COMPILER_DIR}/src/thrift/version.h.in ${COMPILER_DIR}/src/thrift/version.h)
# set(thrift_compiler_SOURCES
#     ${COMPILER_DIR}/src/thrift/main.cc
#     ${COMPILER_DIR}/src/thrift/audit/t_audit.cpp
#     ${COMPILER_DIR}/src/thrift/generate/t_cpp_generator.cc
#     ${COMPILER_DIR}/src/thrift/common.cc
#     ${COMPILER_DIR}/src/thrift/generate/t_generator.cc
#     ${COMPILER_DIR}/src/thrift/parse/t_typedef.cc
#     ${COMPILER_DIR}/src/thrift/parse/parse.cc
#     ${COMPILER_DIR}/src/thrift/version.h
#     ${COMPILER_DIR}/src/thrift/thrifty.cc
#     ${COMPILER_DIR}/src/thrift/thriftl.cc
#     ${COMPILER_DIR}/src/thrift/thrifty.hh
# )
# add_executable(${THRIFT_COMPILER} ${thrift_compiler_SOURCES})
# include_directories(${THRIFT_COMPILER} SYSTEM ${COMPILER_DIR}/src)
