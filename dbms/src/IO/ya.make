LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
    contrib/libs/brotli/dec
    contrib/libs/brotli/enc
    contrib/libs/poco/NetSSL_OpenSSL
)

SRCS(
    AIOContextPool.cpp
    AIO.cpp
    BrotliReadBuffer.cpp
    BrotliWriteBuffer.cpp
    CascadeWriteBuffer.cpp
    CompressionMethod.cpp
    copyData.cpp
    createReadBufferFromFileBase.cpp
    createWriteBufferFromFileBase.cpp
    DoubleConverter.cpp
    HashingWriteBuffer.cpp
    # HDFSCommon.cpp
    HexWriteBuffer.cpp
    HTTPCommon.cpp
    LimitReadBuffer.cpp
    MemoryReadWriteBuffer.cpp
    MMapReadBufferFromFile.cpp
    MMapReadBufferFromFileDescriptor.cpp
    NullWriteBuffer.cpp
    parseDateTimeBestEffort.cpp
    PeekableReadBuffer.cpp
    Progress.cpp
    ReadBufferAIO.cpp
    ReadBufferFromFileBase.cpp
    ReadBufferFromFile.cpp
    ReadBufferFromFileDescriptor.cpp
    # ReadBufferFromHDFS.cpp
    ReadBufferFromIStream.cpp
    ReadBufferFromMemory.cpp
    ReadBufferFromPocoSocket.cpp
    # ReadBufferFromS3.cpp
    readFloatText.cpp
    ReadHelpers.cpp
    ReadWriteBufferFromHTTP.cpp
    # S3Common.cpp
    UseSSL.cpp
    WriteBufferAIO.cpp
    WriteBufferFromFileBase.cpp
    WriteBufferFromFile.cpp
    WriteBufferFromFileDescriptor.cpp
    WriteBufferFromFileDescriptorDiscardOnFailure.cpp
    # WriteBufferFromHDFS.cpp
    WriteBufferFromHTTP.cpp
    WriteBufferFromHTTPServerResponse.cpp
    WriteBufferFromOStream.cpp
    WriteBufferFromPocoSocket.cpp
    # WriteBufferFromS3.cpp
    WriteBufferFromTemporaryFile.cpp
    WriteBufferValidUTF8.cpp
    WriteHelpers.cpp
    ZlibDeflatingWriteBuffer.cpp
    ZlibInflatingReadBuffer.cpp
)

END()
