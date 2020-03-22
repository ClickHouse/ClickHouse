LIBRARY()

PEERDIR(
    clickhouse/base/common
)

SRCS(
    CascadeWriteBuffer.cpp
    CompressionMethod.cpp
    copyData.cpp
    DoubleConverter.cpp
    HTTPCommon.cpp
    LimitReadBuffer.cpp
    MemoryReadWriteBuffer.cpp
    MMapReadBufferFromFile.cpp
    parseDateTimeBestEffort.cpp
    Progress.cpp
    ReadBufferFromFile.cpp
    ReadBufferFromFileBase.cpp
    ReadBufferFromFileDescriptor.cpp
    ReadBufferFromIStream.cpp
    ReadBufferFromMemory.cpp
    ReadBufferFromPocoSocket.cpp
    readFloatText.cpp
    ReadHelpers.cpp
    UseSSL.cpp
    WriteBufferFromFile.cpp
    WriteBufferFromFileDescriptor.cpp
    WriteBufferFromFileDescriptorDiscardOnFailure.cpp
    WriteBufferFromHTTPServerResponse.cpp
    WriteBufferFromPocoSocket.cpp
    WriteBufferFromTemporaryFile.cpp
    WriteHelpers.cpp
)

END()
