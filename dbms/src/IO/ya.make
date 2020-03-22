LIBRARY()

PEERDIR(
    clickhouse/base/common
)

SRCS(
    ReadBufferFromIStream.cpp
    ReadBufferFromMemory.cpp
    UseSSL.cpp
    WriteBufferFromHTTPServerResponse.cpp
)

END()
