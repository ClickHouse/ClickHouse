# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
LIBRARY()

ADDINCL(
    contrib/libs/lz4
    contrib/libs/zstd
)

PEERDIR(
    clickhouse/src/Common
    contrib/libs/lz4
    contrib/libs/zstd
)

SRCS(
    CachedCompressedReadBuffer.cpp
    CompressedReadBufferBase.cpp
    CompressedReadBuffer.cpp
    CompressedReadBufferFromFile.cpp
    CompressedWriteBuffer.cpp
    CompressionCodecDelta.cpp
    CompressionCodecDoubleDelta.cpp
    CompressionCodecGorilla.cpp
    CompressionCodecLZ4.cpp
    CompressionCodecMultiple.cpp
    CompressionCodecNone.cpp
    CompressionCodecT64.cpp
    CompressionCodecZSTD.cpp
    CompressionFactory.cpp
    ICompressionCodec.cpp
    LZ4_decompress_faster.cpp

)

END()
