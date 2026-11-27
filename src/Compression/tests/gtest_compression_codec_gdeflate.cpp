#include <Compression/CompressionCodecGDeflate.h>
#include <Compression/CompressionInfo.h>
#include <Common/Exception.h>
#include <base/unaligned.h>

#include <gtest/gtest.h>

#include <cstring>
#include <limits>
#include <random>
#include <vector>

using namespace DB;

namespace
{

/// A block of one chunk holding `page`, in a buffer of exactly its size, so that ASan reports a read past it.
std::vector<char> makeBlock(const char * page, UInt32 page_size, UInt32 uncompressed_size)
{
    const UInt32 header_size = 9;
    const UInt32 payload_size = 2 * sizeof(UInt32) + page_size;
    std::vector<char> block(header_size + payload_size);
    block[0] = static_cast<char>(CompressionMethodByte::GDeflate);
    unalignedStoreLittleEndian<UInt32>(&block[1], header_size + payload_size);
    unalignedStoreLittleEndian<UInt32>(&block[5], uncompressed_size);
    unalignedStoreLittleEndian<UInt32>(&block[header_size], 1);
    unalignedStoreLittleEndian<UInt32>(&block[header_size + sizeof(UInt32)], page_size);
    if (page_size)
        memcpy(&block[header_size + 2 * sizeof(UInt32)], page, page_size);
    return block;
}

/// Decompresses `block` of a malformed page. GDeflate has no checksum of its own (the checksum of the compressed
/// block catches a corrupt page), so the page may expand to anything or be refused; it must only stay in its bounds.
void decompressMalformed(const CompressionCodecGDeflate & codec, const std::vector<char> & block, UInt32 uncompressed_size)
{
    std::vector<char> out(uncompressed_size);
    try
    {
        codec.decompress(block.data(), static_cast<UInt32>(block.size()), out.data());
    }
    catch (const Exception &) // NOLINT(bugprone-empty-catch)
    {
    }
}

}

/// A valid page expands to its data; truncated, corrupt and random pages are not read past their end.
TEST(CompressionCodecGDeflate, MalformedPages)
{
    const CompressionCodecGDeflate codec(CompressionCodecGDeflate::DEFAULT_LEVEL);
    std::mt19937 rng(42); /// NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp) deterministic seed, so a failure is reproducible

    for (UInt32 size : {1u, 100u, 1000u, 65536u})
    {
        std::vector<char> raw(size);
        for (UInt32 i = 0; i < size; ++i)
            raw[i] = static_cast<char>(i < size / 2 ? i % 7 : rng());

        std::vector<char> compressed(codec.getCompressedReserveSize(size));
        compressed.resize(codec.compress(raw.data(), size, compressed.data()));
        const UInt32 header_size = 9;
        const char * page = &compressed[header_size + 2 * sizeof(UInt32)];
        const UInt32 page_size = unalignedLoadLittleEndian<UInt32>(&compressed[header_size + sizeof(UInt32)]);

        const auto block = makeBlock(page, page_size, size);
        std::vector<char> out(size);
        codec.decompress(block.data(), static_cast<UInt32>(block.size()), out.data());
        EXPECT_EQ(out, raw);

        for (UInt32 cut = 1; cut <= page_size; ++cut)
            decompressMalformed(codec, makeBlock(page, page_size - cut, size), size);

        std::vector<char> corrupt(page, page + page_size);
        for (size_t i = 0; i < 1000; ++i)
        {
            std::vector<char> bad = corrupt;
            bad[rng() % page_size] ^= static_cast<char>(1 << (rng() % 8));
            decompressMalformed(codec, makeBlock(bad.data(), page_size, size), size);
        }
    }

    std::vector<char> garbage(300);
    for (UInt32 size = 0; size < garbage.size(); ++size)
        for (size_t i = 0; i < 20; ++i)
        {
            for (auto & c : garbage)
                c = static_cast<char>(rng());
            decompressMalformed(codec, makeBlock(garbage.data(), size, 1000), 1000);
        }
}

/// The reserve for a block is the bound of every page of it, which exceeds 32 bits for the largest blocks.
TEST(CompressionCodecGDeflate, CompressedReserveSizeFitsInUInt32)
{
    const CompressionCodecGDeflate codec(CompressionCodecGDeflate::DEFAULT_LEVEL);
    EXPECT_GT(codec.getCompressedReserveSize(1u << 30), 1u << 30);
    EXPECT_THROW((void)codec.getCompressedReserveSize(std::numeric_limits<UInt32>::max()), Exception);
}
