if (ENABLE_PROTOBUF AND NOT USE_INTERNAL_PROTOBUF_LIBRARY)
    option(PROTOBUF_OLD_ABI_COMPAT "Set to ON for compatiability with external protobuf which was compiled old C++ ABI" OFF)
endif()

if (PROTOBUF_OLD_ABI_COMPAT)
    if (NOT ENABLE_PROTOBUF OR USE_INTERNAL_PROTOBUF_LIBRARY)
        message (${RECONFIGURE_MESSAGE_LEVEL} "PROTOBUF_OLD_ABI_COMPAT option is ignored")
    endif()
endif()

if (NOT USE_INTERNAL_PROTOBUF_LIBRARY AND PROTOBUF_OLD_ABI_COMPAT)
    # compatiable with protobuf which was compiled old C++ ABI
    set(CMAKE_CXX_FLAGS "-D_GLIBCXX_USE_CXX11_ABI=0")
    set(CMAKE_C_FLAGS "")
    if (NOT (CMAKE_VERSION VERSION_LESS "3.8.0"))
        unset(CMAKE_CXX_STANDARD)
    endif ()
endif()

set(WITH_KERBEROS false)
# project and source dir
set(HDFS3_ROOT_DIR ${ClickHouse_SOURCE_DIR}/contrib/libhdfs3)
set(HDFS3_SOURCE_DIR ${HDFS3_ROOT_DIR}/src)
set(HDFS3_COMMON_DIR ${HDFS3_SOURCE_DIR}/common)

# module
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})
include(Platform)
include(Options)

# prefer shared libraries
if (WITH_KERBEROS)
    find_package(KERBEROS REQUIRED)
endif()

# source
set(PROTO_FILES
    #${HDFS3_SOURCE_DIR}/proto/encryption.proto
    ${HDFS3_SOURCE_DIR}/proto/ClientDatanodeProtocol.proto
    ${HDFS3_SOURCE_DIR}/proto/hdfs.proto
    ${HDFS3_SOURCE_DIR}/proto/Security.proto
    ${HDFS3_SOURCE_DIR}/proto/ProtobufRpcEngine.proto
    ${HDFS3_SOURCE_DIR}/proto/ClientNamenodeProtocol.proto
    ${HDFS3_SOURCE_DIR}/proto/IpcConnectionContext.proto
    ${HDFS3_SOURCE_DIR}/proto/RpcHeader.proto
    ${HDFS3_SOURCE_DIR}/proto/datatransfer.proto
    )

if(USE_PROTOBUF)
    PROTOBUF_GENERATE_CPP(PROTO_SOURCES PROTO_HEADERS ${PROTO_FILES})
endif()

configure_file(${HDFS3_SOURCE_DIR}/platform.h.in ${CMAKE_CURRENT_BINARY_DIR}/platform.h)

set(SRCS
    ${HDFS3_SOURCE_DIR}/network/TcpSocket.cpp
    ${HDFS3_SOURCE_DIR}/network/DomainSocket.cpp
    ${HDFS3_SOURCE_DIR}/network/BufferedSocketReader.cpp
    ${HDFS3_SOURCE_DIR}/client/ReadShortCircuitInfo.cpp
    ${HDFS3_SOURCE_DIR}/client/Pipeline.cpp
    ${HDFS3_SOURCE_DIR}/client/Hdfs.cpp
    ${HDFS3_SOURCE_DIR}/client/Packet.cpp
    ${HDFS3_SOURCE_DIR}/client/OutputStreamImpl.cpp
    ${HDFS3_SOURCE_DIR}/client/KerberosName.cpp
    ${HDFS3_SOURCE_DIR}/client/PacketHeader.cpp
    ${HDFS3_SOURCE_DIR}/client/LocalBlockReader.cpp
    ${HDFS3_SOURCE_DIR}/client/UserInfo.cpp
    ${HDFS3_SOURCE_DIR}/client/RemoteBlockReader.cpp
    ${HDFS3_SOURCE_DIR}/client/Permission.cpp
    ${HDFS3_SOURCE_DIR}/client/FileSystemImpl.cpp
    ${HDFS3_SOURCE_DIR}/client/DirectoryIterator.cpp
    ${HDFS3_SOURCE_DIR}/client/FileSystemKey.cpp
    ${HDFS3_SOURCE_DIR}/client/DataTransferProtocolSender.cpp
    ${HDFS3_SOURCE_DIR}/client/LeaseRenewer.cpp
    ${HDFS3_SOURCE_DIR}/client/PeerCache.cpp
    ${HDFS3_SOURCE_DIR}/client/InputStream.cpp
    ${HDFS3_SOURCE_DIR}/client/FileSystem.cpp
    ${HDFS3_SOURCE_DIR}/client/InputStreamImpl.cpp
    ${HDFS3_SOURCE_DIR}/client/Token.cpp
    ${HDFS3_SOURCE_DIR}/client/PacketPool.cpp
    ${HDFS3_SOURCE_DIR}/client/OutputStream.cpp
    ${HDFS3_SOURCE_DIR}/rpc/RpcChannelKey.cpp
    ${HDFS3_SOURCE_DIR}/rpc/RpcProtocolInfo.cpp
    ${HDFS3_SOURCE_DIR}/rpc/RpcClient.cpp
    ${HDFS3_SOURCE_DIR}/rpc/RpcRemoteCall.cpp
    ${HDFS3_SOURCE_DIR}/rpc/RpcChannel.cpp
    ${HDFS3_SOURCE_DIR}/rpc/RpcAuth.cpp
    ${HDFS3_SOURCE_DIR}/rpc/RpcContentWrapper.cpp
    ${HDFS3_SOURCE_DIR}/rpc/RpcConfig.cpp
    ${HDFS3_SOURCE_DIR}/rpc/RpcServerInfo.cpp
    ${HDFS3_SOURCE_DIR}/rpc/SaslClient.cpp
    ${HDFS3_SOURCE_DIR}/server/Datanode.cpp
    ${HDFS3_SOURCE_DIR}/server/LocatedBlocks.cpp
    ${HDFS3_SOURCE_DIR}/server/NamenodeProxy.cpp
    ${HDFS3_SOURCE_DIR}/server/NamenodeImpl.cpp
    ${HDFS3_SOURCE_DIR}/server/NamenodeInfo.cpp
    ${HDFS3_SOURCE_DIR}/common/WritableUtils.cpp
    ${HDFS3_SOURCE_DIR}/common/ExceptionInternal.cpp
    ${HDFS3_SOURCE_DIR}/common/SessionConfig.cpp
    ${HDFS3_SOURCE_DIR}/common/StackPrinter.cpp
    ${HDFS3_SOURCE_DIR}/common/Exception.cpp
    ${HDFS3_SOURCE_DIR}/common/Logger.cpp
    ${HDFS3_SOURCE_DIR}/common/CFileWrapper.cpp
    ${HDFS3_SOURCE_DIR}/common/XmlConfig.cpp
    ${HDFS3_SOURCE_DIR}/common/WriteBuffer.cpp
    ${HDFS3_SOURCE_DIR}/common/HWCrc32c.cpp
    ${HDFS3_SOURCE_DIR}/common/MappedFileWrapper.cpp
    ${HDFS3_SOURCE_DIR}/common/Hash.cpp
    ${HDFS3_SOURCE_DIR}/common/SWCrc32c.cpp
    ${HDFS3_SOURCE_DIR}/common/Thread.cpp

    ${HDFS3_SOURCE_DIR}/network/TcpSocket.h
    ${HDFS3_SOURCE_DIR}/network/BufferedSocketReader.h
    ${HDFS3_SOURCE_DIR}/network/Socket.h
    ${HDFS3_SOURCE_DIR}/network/DomainSocket.h
    ${HDFS3_SOURCE_DIR}/network/Syscall.h
    ${HDFS3_SOURCE_DIR}/client/InputStreamImpl.h
    ${HDFS3_SOURCE_DIR}/client/FileSystem.h
    ${HDFS3_SOURCE_DIR}/client/ReadShortCircuitInfo.h
    ${HDFS3_SOURCE_DIR}/client/InputStreamInter.h
    ${HDFS3_SOURCE_DIR}/client/FileSystemImpl.h
    ${HDFS3_SOURCE_DIR}/client/PacketPool.h
    ${HDFS3_SOURCE_DIR}/client/Pipeline.h
    ${HDFS3_SOURCE_DIR}/client/OutputStreamInter.h
    ${HDFS3_SOURCE_DIR}/client/RemoteBlockReader.h
    ${HDFS3_SOURCE_DIR}/client/Token.h
    ${HDFS3_SOURCE_DIR}/client/KerberosName.h
    ${HDFS3_SOURCE_DIR}/client/DirectoryIterator.h
    ${HDFS3_SOURCE_DIR}/client/hdfs.h
    ${HDFS3_SOURCE_DIR}/client/FileSystemStats.h
    ${HDFS3_SOURCE_DIR}/client/FileSystemKey.h
    ${HDFS3_SOURCE_DIR}/client/DataTransferProtocolSender.h
    ${HDFS3_SOURCE_DIR}/client/Packet.h
    ${HDFS3_SOURCE_DIR}/client/PacketHeader.h
    ${HDFS3_SOURCE_DIR}/client/FileSystemInter.h
    ${HDFS3_SOURCE_DIR}/client/LocalBlockReader.h
    ${HDFS3_SOURCE_DIR}/client/TokenInternal.h
    ${HDFS3_SOURCE_DIR}/client/InputStream.h
    ${HDFS3_SOURCE_DIR}/client/PipelineAck.h
    ${HDFS3_SOURCE_DIR}/client/BlockReader.h
    ${HDFS3_SOURCE_DIR}/client/Permission.h
    ${HDFS3_SOURCE_DIR}/client/OutputStreamImpl.h
    ${HDFS3_SOURCE_DIR}/client/LeaseRenewer.h
    ${HDFS3_SOURCE_DIR}/client/UserInfo.h
    ${HDFS3_SOURCE_DIR}/client/PeerCache.h
    ${HDFS3_SOURCE_DIR}/client/OutputStream.h
    ${HDFS3_SOURCE_DIR}/client/FileStatus.h
    ${HDFS3_SOURCE_DIR}/client/DataTransferProtocol.h
    ${HDFS3_SOURCE_DIR}/client/BlockLocation.h
    ${HDFS3_SOURCE_DIR}/rpc/RpcConfig.h
    ${HDFS3_SOURCE_DIR}/rpc/SaslClient.h
    ${HDFS3_SOURCE_DIR}/rpc/RpcAuth.h
    ${HDFS3_SOURCE_DIR}/rpc/RpcClient.h
    ${HDFS3_SOURCE_DIR}/rpc/RpcCall.h
    ${HDFS3_SOURCE_DIR}/rpc/RpcContentWrapper.h
    ${HDFS3_SOURCE_DIR}/rpc/RpcProtocolInfo.h
    ${HDFS3_SOURCE_DIR}/rpc/RpcRemoteCall.h
    ${HDFS3_SOURCE_DIR}/rpc/RpcServerInfo.h
    ${HDFS3_SOURCE_DIR}/rpc/RpcChannel.h
    ${HDFS3_SOURCE_DIR}/rpc/RpcChannelKey.h
    ${HDFS3_SOURCE_DIR}/server/BlockLocalPathInfo.h
    ${HDFS3_SOURCE_DIR}/server/LocatedBlocks.h
    ${HDFS3_SOURCE_DIR}/server/DatanodeInfo.h
    ${HDFS3_SOURCE_DIR}/server/RpcHelper.h
    ${HDFS3_SOURCE_DIR}/server/ExtendedBlock.h
    ${HDFS3_SOURCE_DIR}/server/NamenodeInfo.h
    ${HDFS3_SOURCE_DIR}/server/NamenodeImpl.h
    ${HDFS3_SOURCE_DIR}/server/LocatedBlock.h
    ${HDFS3_SOURCE_DIR}/server/NamenodeProxy.h
    ${HDFS3_SOURCE_DIR}/server/Datanode.h
    ${HDFS3_SOURCE_DIR}/server/Namenode.h
    ${HDFS3_SOURCE_DIR}/common/XmlConfig.h
    ${HDFS3_SOURCE_DIR}/common/Logger.h
    ${HDFS3_SOURCE_DIR}/common/WriteBuffer.h
    ${HDFS3_SOURCE_DIR}/common/HWCrc32c.h
    ${HDFS3_SOURCE_DIR}/common/Checksum.h
    ${HDFS3_SOURCE_DIR}/common/SessionConfig.h
    ${HDFS3_SOURCE_DIR}/common/Unordered.h
    ${HDFS3_SOURCE_DIR}/common/BigEndian.h
    ${HDFS3_SOURCE_DIR}/common/Thread.h
    ${HDFS3_SOURCE_DIR}/common/StackPrinter.h
    ${HDFS3_SOURCE_DIR}/common/Exception.h
    ${HDFS3_SOURCE_DIR}/common/WritableUtils.h
    ${HDFS3_SOURCE_DIR}/common/StringUtil.h
    ${HDFS3_SOURCE_DIR}/common/LruMap.h
    ${HDFS3_SOURCE_DIR}/common/Function.h
    ${HDFS3_SOURCE_DIR}/common/DateTime.h
    ${HDFS3_SOURCE_DIR}/common/Hash.h
    ${HDFS3_SOURCE_DIR}/common/SWCrc32c.h
    ${HDFS3_SOURCE_DIR}/common/ExceptionInternal.h
    ${HDFS3_SOURCE_DIR}/common/Memory.h
    ${HDFS3_SOURCE_DIR}/common/FileWrapper.h
    )

# old kernels (< 3.17) doesn't have SYS_getrandom. Always use POSIX implementation to have better compatibility
set_source_files_properties(${HDFS3_SOURCE_DIR}/rpc/RpcClient.cpp PROPERTIES COMPILE_FLAGS "-DBOOST_UUID_RANDOM_PROVIDER_FORCE_POSIX=1")

# target
add_library(hdfs3 ${SRCS} ${PROTO_SOURCES} ${PROTO_HEADERS})

if(USE_INTERNAL_PROTOBUF_LIBRARY)
    add_dependencies(hdfs3 protoc)
endif()

target_include_directories(hdfs3 PRIVATE ${HDFS3_SOURCE_DIR})
target_include_directories(hdfs3 PRIVATE ${HDFS3_COMMON_DIR})
target_include_directories(hdfs3 PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_include_directories(hdfs3 PRIVATE ${LIBGSASL_INCLUDE_DIR})
if (WITH_KERBEROS)
    target_include_directories(hdfs3 PRIVATE ${KERBEROS_INCLUDE_DIRS})
endif()
target_include_directories(hdfs3 PRIVATE ${LIBXML2_INCLUDE_DIR})

target_link_libraries(hdfs3 PRIVATE ${LIBGSASL_LIBRARY})
if (WITH_KERBEROS)
    target_link_libraries(hdfs3 PRIVATE ${KERBEROS_LIBRARIES})
endif()
target_link_libraries(hdfs3 PRIVATE ${LIBXML2_LIBRARIES})

# inherit from parent cmake
target_include_directories(hdfs3 PRIVATE ${Protobuf_INCLUDE_DIR})
target_link_libraries(hdfs3 PRIVATE ${Protobuf_LIBRARY} boost::headers_only)
if(OPENSSL_INCLUDE_DIR AND OPENSSL_LIBRARIES)
    target_include_directories(hdfs3 PRIVATE ${OPENSSL_INCLUDE_DIR})
    target_link_libraries(hdfs3 PRIVATE ${OPENSSL_LIBRARIES})
endif()
