#include <Compression/CompressionCodecGDeflate.h>
#include <Common/HashTable/Hash.h>
#include <base/types.h>

#include <gtest/gtest.h>

#include <memory>
#include <vector>

using namespace DB;

/// A page that is compressible for its first `prefix` bytes and random after them compresses to about the most a GDeflate page can.
TEST(GDeflateCodec, CompressedChunkFitsReserve)
{
    constexpr UInt32 source_size = CompressionCodecGDeflate::chunk_bytes;
    std::vector<char> source(source_size);
    std::vector<char> decompressed(source_size);

    for (const int level : {1, 6, 12})
    {
        const CompressionCodecGDeflate codec(level);
        const UInt32 reserve = codec.getCompressedReserveSize(source_size);

        for (size_t prefix = 0; prefix < 256; ++prefix)
        {
            for (size_t i = 0; i < source_size; ++i)
                source[i] = static_cast<char>(i < prefix ? i % 7 : static_cast<UInt8>(intHash64(i)));

            /// Exactly `reserve` bytes, so that a sanitizer reports a write past it.
            auto compressed = std::make_unique<char[]>(reserve);
            UInt32 compressed_size = 0;
            ASSERT_NO_THROW(compressed_size = codec.compress(source.data(), source_size, compressed.get()))
                << "level " << level << ", prefix " << prefix;
            ASSERT_LE(compressed_size, reserve) << "level " << level << ", prefix " << prefix;

            ASSERT_EQ(codec.decompress(compressed.get(), compressed_size, decompressed.data()), source_size)
                << "level " << level << ", prefix " << prefix;
            ASSERT_EQ(source, decompressed) << "level " << level << ", prefix " << prefix;
        }
    }
}
