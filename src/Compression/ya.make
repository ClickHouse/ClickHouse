# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

ADDINCL(
    contrib/libs/lz4
    contrib/libs/zstd/include
)

PEERDIR(
    clickhouse/src/Common
    contrib/libs/lz4
    contrib/libs/zstd
)


SRCS(
    CachedCompressedReadBuffer.cpp
    CheckingCompressedReadBuffer.cpp
    CompressedReadBuffer.cpp
    CompressedReadBufferBase.cpp
    CompressedReadBufferFromFile.cpp
    CompressedWriteBuffer.cpp
    CompressionCodecDelta.cpp
    CompressionCodecDoubleDelta.cpp
    CompressionCodecEncrypted.cpp
    CompressionCodecGorilla.cpp
    CompressionCodecLZ4.cpp
    CompressionCodecMultiple.cpp
    CompressionCodecNone.cpp
    CompressionCodecT64.cpp
    CompressionCodecZSTD.cpp
    CompressionFactory.cpp
    CompressionFactoryAdditions.cpp
    ICompressionCodec.cpp
    LZ4_decompress_faster.cpp
    fuzzers/compressed_buffer_fuzzer.cpp
    fuzzers/delta_decompress_fuzzer.cpp
    fuzzers/double_delta_decompress_fuzzer.cpp
    fuzzers/encrypted_decompress_fuzzer.cpp
    fuzzers/lz4_decompress_fuzzer.cpp
    getCompressionCodecForFile.cpp

)

END()
