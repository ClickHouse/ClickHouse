LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
)

SRCS(
    CompressionFactory.cpp
    CompressedWriteBuffer.cpp
)

END()
