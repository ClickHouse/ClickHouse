#include <Compression/CompressionCodecMultiple.h>
#include <Compression/CompressionFactory.h>

#include <gtest/gtest.h>

#include <cstring>
#include <initializer_list>
#include <memory>
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
