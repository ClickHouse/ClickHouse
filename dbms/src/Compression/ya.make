LIBRARY()

ADDINCL(
    contrib/libs/lz4
    contrib/libs/zstd
)

PEERDIR(
    clickhouse/dbms/src/Common
    contrib/libs/lz4
    contrib/libs/zstd
)

SRCS(
    CompressionCodecDelta.cpp
    CompressionCodecDoubleDelta.cpp
    CompressionCodecGorilla.cpp
    CompressionCodecLZ4.cpp
    CompressionCodecMultiple.cpp
    CompressionCodecNone.cpp
    CompressionCodecT64.cpp
    CompressionCodecZSTD.cpp
    CompressionFactory.cpp
    CompressedReadBuffer.cpp
    CompressedReadBufferBase.cpp
    CompressedReadBufferFromFile.cpp
    CompressedWriteBuffer.cpp
    ICompressionCodec.cpp
    LZ4_decompress_faster.cpp
)

END()
