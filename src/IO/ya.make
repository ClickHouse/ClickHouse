# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

ADDINCL(
    contrib/libs/zstd/include
    contrib/restricted/fast_float/include
)

PEERDIR(
    clickhouse/src/Common
    contrib/libs/brotli/dec
    contrib/libs/brotli/enc
    contrib/libs/poco/NetSSL_OpenSSL
    contrib/libs/zstd
    contrib/restricted/fast_float
)


SRCS(
    AIO.cpp
    AIOContextPool.cpp
    BrotliReadBuffer.cpp
    BrotliWriteBuffer.cpp
    Bzip2ReadBuffer.cpp
    Bzip2WriteBuffer.cpp
    CascadeWriteBuffer.cpp
    CompressionMethod.cpp
    DoubleConverter.cpp
    FileEncryptionCommon.cpp
    HTTPChunkedReadBuffer.cpp
    HTTPCommon.cpp
    HashingWriteBuffer.cpp
    LZMADeflatingWriteBuffer.cpp
    LZMAInflatingReadBuffer.cpp
    LimitReadBuffer.cpp
    MMapReadBufferFromFile.cpp
    MMapReadBufferFromFileDescriptor.cpp
    MMapReadBufferFromFileWithCache.cpp
    MMappedFile.cpp
    MMappedFileDescriptor.cpp
    MemoryReadWriteBuffer.cpp
    MySQLBinlogEventReadBuffer.cpp
    MySQLPacketPayloadReadBuffer.cpp
    MySQLPacketPayloadWriteBuffer.cpp
    NullWriteBuffer.cpp
    OpenedFile.cpp
    PeekableReadBuffer.cpp
    Progress.cpp
    ReadBufferFromEncryptedFile.cpp
    ReadBufferFromFile.cpp
    ReadBufferFromFileBase.cpp
    ReadBufferFromFileDecorator.cpp
    ReadBufferFromFileDescriptor.cpp
    ReadBufferFromIStream.cpp
    ReadBufferFromMemory.cpp
    ReadBufferFromPocoSocket.cpp
    ReadHelpers.cpp
    SeekAvoidingReadBuffer.cpp
    TimeoutSetter.cpp
    UseSSL.cpp
    WriteBufferFromEncryptedFile.cpp
    WriteBufferFromFile.cpp
    WriteBufferFromFileBase.cpp
    WriteBufferFromFileDecorator.cpp
    WriteBufferFromFileDescriptor.cpp
    WriteBufferFromFileDescriptorDiscardOnFailure.cpp
    WriteBufferFromHTTP.cpp
    WriteBufferFromOStream.cpp
    WriteBufferFromPocoSocket.cpp
    WriteBufferFromTemporaryFile.cpp
    WriteBufferValidUTF8.cpp
    WriteHelpers.cpp
    ZlibDeflatingWriteBuffer.cpp
    ZlibInflatingReadBuffer.cpp
    ZstdDeflatingWriteBuffer.cpp
    ZstdInflatingReadBuffer.cpp
    copyData.cpp
    createReadBufferFromFileBase.cpp
    parseDateTimeBestEffort.cpp
    readFloatText.cpp

)

END()
