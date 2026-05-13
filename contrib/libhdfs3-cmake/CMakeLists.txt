if(NOT OS_FREEBSD AND NOT ARCH_PPC64LE)
    option(ENABLE_HDFS "Enable HDFS" ${ENABLE_LIBRARIES})
elseif(ENABLE_HDFS)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot use HDFS3 with current configuration")
endif()

if(NOT ENABLE_HDFS)
    message(STATUS "Not using HDFS")
    return()
endif()

if (TARGET ch_contrib::krb5)
    SET(WITH_KERBEROS 1)
else()
    SET(WITH_KERBEROS 0)
endif()

# project and source dir
set(HDFS3_ROOT_DIR "${ClickHouse_SOURCE_DIR}/contrib/libhdfs3")
set(HDFS3_SOURCE_DIR "${HDFS3_ROOT_DIR}/src")
set(HDFS3_COMMON_DIR "${HDFS3_SOURCE_DIR}/common")

ADD_DEFINITIONS(-DTEST_HDFS_PREFIX="${TEST_HDFS_PREFIX}")
ADD_DEFINITIONS(-D__STDC_FORMAT_MACROS)
ADD_DEFINITIONS(-D_GNU_SOURCE)
ADD_DEFINITIONS(-D_GLIBCXX_USE_NANOSLEEP)
ADD_DEFINITIONS(-DHAVE_NANOSLEEP)

if (USE_MUSL OR APPLE)
    ADD_DEFINITIONS(-DSTRERROR_R_RETURN_INT)
endif ()

set(HAVE_STEADY_CLOCK 1)
set(HAVE_NESTED_EXCEPTION 1)
SET(HAVE_BOOST_CHRONO 0)
SET(HAVE_BOOST_ATOMIC 0)
SET(HAVE_STD_CHRONO 1)
SET(HAVE_STD_ATOMIC 1)

# source
set(PROTO_FILES
    #"${HDFS3_SOURCE_DIR}/proto/encryption.proto"
    "${HDFS3_SOURCE_DIR}/proto/ClientDatanodeProtocol.proto"
    "${HDFS3_SOURCE_DIR}/proto/hdfs.proto"
    "${HDFS3_SOURCE_DIR}/proto/Security.proto"
    "${HDFS3_SOURCE_DIR}/proto/ProtobufRpcEngine.proto"
    "${HDFS3_SOURCE_DIR}/proto/ClientNamenodeProtocol.proto"
    "${HDFS3_SOURCE_DIR}/proto/IpcConnectionContext.proto"
    "${HDFS3_SOURCE_DIR}/proto/RpcHeader.proto"
    "${HDFS3_SOURCE_DIR}/proto/datatransfer.proto"
    )

PROTOBUF_GENERATE_CPP(PROTO_SOURCES PROTO_HEADERS ${PROTO_FILES} APPEND_PATH)

configure_file("${HDFS3_SOURCE_DIR}/platform.h.in" "${CMAKE_CURRENT_BINARY_DIR}/platform.h")

set(SRCS
    "${HDFS3_SOURCE_DIR}/network/TcpSocket.cpp"
    "${HDFS3_SOURCE_DIR}/network/DomainSocket.cpp"
    "${HDFS3_SOURCE_DIR}/network/BufferedSocketReader.cpp"
    "${HDFS3_SOURCE_DIR}/client/ReadShortCircuitInfo.cpp"
    "${HDFS3_SOURCE_DIR}/client/Pipeline.cpp"
    "${HDFS3_SOURCE_DIR}/client/Hdfs.cpp"
    "${HDFS3_SOURCE_DIR}/client/Packet.cpp"
    "${HDFS3_SOURCE_DIR}/client/OutputStreamImpl.cpp"
    "${HDFS3_SOURCE_DIR}/client/KerberosName.cpp"
    "${HDFS3_SOURCE_DIR}/client/PacketHeader.cpp"
    "${HDFS3_SOURCE_DIR}/client/LocalBlockReader.cpp"
    "${HDFS3_SOURCE_DIR}/client/UserInfo.cpp"
    "${HDFS3_SOURCE_DIR}/client/RemoteBlockReader.cpp"
    "${HDFS3_SOURCE_DIR}/client/Permission.cpp"
    "${HDFS3_SOURCE_DIR}/client/FileSystemImpl.cpp"
    "${HDFS3_SOURCE_DIR}/client/DirectoryIterator.cpp"
    "${HDFS3_SOURCE_DIR}/client/FileSystemKey.cpp"
    "${HDFS3_SOURCE_DIR}/client/DataTransferProtocolSender.cpp"
    "${HDFS3_SOURCE_DIR}/client/LeaseRenewer.cpp"
    "${HDFS3_SOURCE_DIR}/client/PeerCache.cpp"
    "${HDFS3_SOURCE_DIR}/client/InputStream.cpp"
    "${HDFS3_SOURCE_DIR}/client/FileSystem.cpp"
    "${HDFS3_SOURCE_DIR}/client/InputStreamImpl.cpp"
    "${HDFS3_SOURCE_DIR}/client/Token.cpp"
    "${HDFS3_SOURCE_DIR}/client/PacketPool.cpp"
    "${HDFS3_SOURCE_DIR}/client/OutputStream.cpp"
    "${HDFS3_SOURCE_DIR}/client/AbstractNativeRawDecoder.cpp"
    "${HDFS3_SOURCE_DIR}/client/AbstractNativeRawEncoder.cpp"
    "${HDFS3_SOURCE_DIR}/client/ByteBufferDecodingState.cpp"
    "${HDFS3_SOURCE_DIR}/client/ByteBufferEncodingState.cpp"
    "${HDFS3_SOURCE_DIR}/client/CoderUtil.cpp"
    "${HDFS3_SOURCE_DIR}/client/ECChunk.cpp"
    "${HDFS3_SOURCE_DIR}/client/ErasureCoderOptions.cpp"
    "${HDFS3_SOURCE_DIR}/client/GF256.cpp"
    "${HDFS3_SOURCE_DIR}/client/GaloisField.cpp"
    "${HDFS3_SOURCE_DIR}/client/NativeRSRawDecoder.cpp"
    "${HDFS3_SOURCE_DIR}/client/NativeRSRawEncoder.cpp"
    "${HDFS3_SOURCE_DIR}/client/Preconditions.cpp"
    "${HDFS3_SOURCE_DIR}/client/RSUtil.cpp"
    "${HDFS3_SOURCE_DIR}/client/RawErasureCoderFactory.cpp"
    "${HDFS3_SOURCE_DIR}/client/RawErasureDecoder.cpp"
    "${HDFS3_SOURCE_DIR}/client/RawErasureEncoder.cpp"
    "${HDFS3_SOURCE_DIR}/client/StatefulStripeReader.cpp"
    "${HDFS3_SOURCE_DIR}/client/StripeReader.cpp"
    "${HDFS3_SOURCE_DIR}/client/StripedBlockUtil.cpp"
    "${HDFS3_SOURCE_DIR}/client/StripedInputStreamImpl.cpp"
    "${HDFS3_SOURCE_DIR}/client/StripedOutputStreamImpl.cpp"
    "${HDFS3_SOURCE_DIR}/client/SystemECPolicies.cpp"
    "${HDFS3_SOURCE_DIR}/client/dump.cpp"
    "${HDFS3_SOURCE_DIR}/client/erasure_coder.cpp"
    "${HDFS3_SOURCE_DIR}/rpc/RpcChannelKey.cpp"
    "${HDFS3_SOURCE_DIR}/rpc/RpcProtocolInfo.cpp"
    "${HDFS3_SOURCE_DIR}/rpc/RpcClient.cpp"
    "${HDFS3_SOURCE_DIR}/rpc/RpcRemoteCall.cpp"
    "${HDFS3_SOURCE_DIR}/rpc/RpcChannel.cpp"
    "${HDFS3_SOURCE_DIR}/rpc/RpcAuth.cpp"
    "${HDFS3_SOURCE_DIR}/rpc/RpcContentWrapper.cpp"
    "${HDFS3_SOURCE_DIR}/rpc/RpcConfig.cpp"
    "${HDFS3_SOURCE_DIR}/rpc/RpcServerInfo.cpp"
    "${HDFS3_SOURCE_DIR}/rpc/SaslClient.cpp"
    "${HDFS3_SOURCE_DIR}/server/Datanode.cpp"
    "${HDFS3_SOURCE_DIR}/server/LocatedBlocks.cpp"
    "${HDFS3_SOURCE_DIR}/server/NamenodeProxy.cpp"
    "${HDFS3_SOURCE_DIR}/server/NamenodeImpl.cpp"
    "${HDFS3_SOURCE_DIR}/server/NamenodeInfo.cpp"
    "${HDFS3_SOURCE_DIR}/common/WritableUtils.cpp"
    "${HDFS3_SOURCE_DIR}/common/ExceptionInternal.cpp"
    "${HDFS3_SOURCE_DIR}/common/SessionConfig.cpp"
    "${HDFS3_SOURCE_DIR}/common/StackPrinter.cpp"
    "${HDFS3_SOURCE_DIR}/common/Exception.cpp"
    "${HDFS3_SOURCE_DIR}/common/Logger.cpp"
    "${HDFS3_SOURCE_DIR}/common/CFileWrapper.cpp"
    "${HDFS3_SOURCE_DIR}/common/XmlConfig.cpp"
    "${HDFS3_SOURCE_DIR}/common/WriteBuffer.cpp"
    "${HDFS3_SOURCE_DIR}/common/HWCrc32c.cpp"
    "${HDFS3_SOURCE_DIR}/common/MappedFileWrapper.cpp"
    "${HDFS3_SOURCE_DIR}/common/Hash.cpp"
    "${HDFS3_SOURCE_DIR}/common/SWCrc32c.cpp"
    "${HDFS3_SOURCE_DIR}/common/Thread.cpp"
    "${HDFS3_SOURCE_DIR}/common/IntelAsmCrc32c.cpp"
    ${PROTO_SOURCES}
)

if (ARCH_AMD64 AND NOT APPLE)
    find_program(YASM_PATH NAMES yasm)
    if (NOT YASM_PATH)
        message(FATAL_ERROR "Please install the Yasm assembler to build ClickHouse with Hadoop Distributed File System (HDFS) support")
    endif ()
    add_custom_command(
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/crc_iscsi_v_pcl.o
        COMMAND ${YASM_PATH} -f x64 -f elf64 -X gnu -g dwarf2 -D LINUX -o ${CMAKE_CURRENT_BINARY_DIR}/crc_iscsi_v_pcl.o ${HDFS3_SOURCE_DIR}/common/crc_iscsi_v_pcl.asm
        COMMENT "Translating Intel CRC assembly to object code")
    LIST(APPEND SRCS ${CMAKE_CURRENT_BINARY_DIR}/crc_iscsi_v_pcl.o)
endif ()

# old kernels (< 3.17) doesn't have SYS_getrandom. Always use POSIX implementation to have better compatibility
set_source_files_properties("${HDFS3_SOURCE_DIR}/rpc/RpcClient.cpp" PROPERTIES COMPILE_FLAGS "-DBOOST_UUID_RANDOM_PROVIDER_FORCE_POSIX=1")

# target
add_library(_hdfs3 ${SRCS})

add_dependencies(_hdfs3 protoc)
target_compile_options(_hdfs3 PRIVATE
    -include "${ClickHouse_SOURCE_DIR}/contrib/google-protobuf/src/google/protobuf/stubs/port.h"
)

if (ENABLE_CLICKHOUSE_BENCHMARK)
    message(STATUS "enable perf checksum")
    target_include_directories(_hdfs3 PUBLIC ${HDFS3_SOURCE_DIR})
    target_include_directories(_hdfs3 PUBLIC ${HDFS3_COMMON_DIR})
    target_include_directories(_hdfs3 PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
else ()
    target_include_directories(_hdfs3 PRIVATE ${HDFS3_SOURCE_DIR})
    target_include_directories(_hdfs3 PRIVATE ${HDFS3_COMMON_DIR})
    target_include_directories(_hdfs3 PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
endif ()

target_include_directories(_hdfs3 SYSTEM BEFORE INTERFACE "${ClickHouse_SOURCE_DIR}/contrib/libhdfs3/include")

target_link_libraries(_hdfs3 PRIVATE ch_contrib::gsasl)
if (WITH_KERBEROS)
    target_link_libraries(_hdfs3 PRIVATE ch_contrib::krb5)
endif()
target_link_libraries(_hdfs3 PRIVATE ch_contrib::libxml2)

# inherit from parent cmake
target_link_libraries(_hdfs3 PRIVATE ch_contrib::protobuf boost::headers_only)
if (TARGET OpenSSL::SSL)
    target_link_libraries(_hdfs3 PRIVATE OpenSSL::Crypto OpenSSL::SSL)
endif()

if (TARGET ch_contrib::isal)
    target_link_libraries(_hdfs3 PRIVATE ch_contrib::isal)
    add_definitions(-DHADOOP_ISAL_LIBRARY)
endif()

add_library(ch_contrib::hdfs ALIAS _hdfs3)

if (ENABLE_CLICKHOUSE_BENCHMARK)
     add_executable(perf_checksum ${HDFS3_SOURCE_DIR}/common/perf_checksum.cpp)
     target_link_libraries(perf_checksum PRIVATE ch_contrib::hdfs)
endif ()
