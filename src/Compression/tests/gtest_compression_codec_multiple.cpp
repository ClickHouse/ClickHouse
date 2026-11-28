#include <Compression/CompressionCodecMultiple.h>
#include <Compression/CompressionFactory.h>

#include <gtest/gtest.h>

#include <cstring>
#include <initializer_list>
#include <memory>
#include <random>
#include <utility>
#include <vector>

using namespace DB;

/// Check `Multiple` decodes correctly without requiring extra space in the caller’s buffers
TEST(CompressionCodecMultiple, ExactBuffersAndTails)
{
    CompressionCodecMultiple decoder;
    ASSERT_EQ(decoder.getAdditionalSizeAtTheEndOfBuffer(), 0);
    /// Reuse one decoder across changing chains and sizes.
    for (size_t size : {1uz, 15uz, 16uz, 17uz, 65535uz, 65536uz, 65537uz})
    {
        char value = 0;
        for (const auto & methods :
             {std::initializer_list<const char *>{"NONE", "NONE"},
              {"LZ4", "ZSTD"},
              {"ZSTD", "LZ4"},
              {"LZ4", "LZ4"},
              {"LZ4", "LZ4", "ZSTD"}})
        {
            Codecs codecs;
            for (const char * method : methods)
                codecs.push_back(CompressionCodecFactory::instance().get(method, {}));
            CompressionCodecMultiple encoder(std::move(codecs));
            std::vector<char> raw(size, ++value);
            std::vector<char> compressed(encoder.getCompressedReserveSize(static_cast<UInt32>(size)));
            compressed.resize(encoder.compress(raw.data(), static_cast<UInt32>(size), compressed.data()));

            /// Nested codecs (i.e. `LZ4`) may read or write beyond the data when using wide memory accesses.
            /// `Multiple` supplies padded temporary buffers for those codecs and copies only the actual data.
            /// Thus, callers do not need extra bytes after either input or output.
            /// Exact allocations let ASan detect accesses beyond these caller-provided buffers.
            auto input = std::make_unique<char[]>(compressed.size());
            auto output = std::make_unique<char[]>(raw.size());
            memcpy(input.get(), compressed.data(), compressed.size());

            ASSERT_EQ(decoder.decompress(input.get(), static_cast<UInt32>(compressed.size()), output.get()), raw.size());
            EXPECT_EQ(memcmp(output.get(), raw.data(), raw.size()), 0);
            EXPECT_EQ(memcmp(input.get(), compressed.data(), compressed.size()), 0);
        }
    }
}

/// Resuming a chain after any number of completed stages writes the bytes `compress` writes.
TEST(CompressionCodecMultiple, CompressRemainingStagesMatchesCompress)
{
    auto & factory = CompressionCodecFactory::instance();
    const auto first = factory.get("LZ4", {});
    CompressionCodecMultiple chain(Codecs{first, factory.get("ZSTD", {})});

    for (const UInt32 size : {1u, 40003u})
    {
        /// Low-entropy bytes, so both stages have something to compress.
        std::mt19937 rng(size);
        std::vector<char> bytes(size);
        for (auto & byte : bytes)
            byte = static_cast<char>(rng() % 16);

        std::vector<char> whole(chain.getCompressedReserveSize(size));
        const UInt32 whole_size = chain.compress(bytes.data(), size, whole.data());

        std::vector<char> first_stage(first->getCompressedReserveSize(size));
        const UInt32 first_stage_size = first->compress(bytes.data(), size, first_stage.data());

        const auto check = [&](size_t completed_stages, const char * input, UInt32 input_size)
        {
            SCOPED_TRACE(testing::Message() << "size " << size << ", completed stages " << completed_stages);
            std::vector<char> resumed(chain.getCompressedReserveSize(size));
            const UInt32 resumed_size = chain.compressRemainingStages(completed_stages, input, input_size, size, resumed.data());
            ASSERT_EQ(resumed_size, whole_size);
            EXPECT_EQ(memcmp(resumed.data(), whole.data(), whole_size), 0);
        };

        check(0, bytes.data(), size);
        check(1, first_stage.data(), first_stage_size);

        const UInt32 frame_size = ICompressionCodec::getHeaderSize() + 3;
        check(2, whole.data() + frame_size, whole_size - frame_size);
    }
}
