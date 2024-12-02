#include <random>
#include <gtest/gtest.h>

#include <Formats/MarkInCompressedFile.h>

using namespace DB;

TEST(Marks, Compression)
{
    std::random_device dev;
    std::mt19937 rng(dev());

    auto gen = [&](size_t count, size_t max_x_increment, size_t max_y_increment)
    {
        size_t x = 0, y = 0;
        PODArray<MarkInCompressedFile> plain(count);
        for (int i = 0; i < count; ++i)
        {
            x += rng() % (max_x_increment + 1);
            y += rng() % (max_y_increment + 1);
            plain[i] = MarkInCompressedFile{.offset_in_compressed_file = x, .offset_in_decompressed_block = y};
        }
        return plain;
    };

    auto test = [](const PODArray<MarkInCompressedFile> & plain, size_t max_bits_per_mark)
    {
        PODArray<MarkInCompressedFile> copy;
        copy.assign(plain); // paranoid in case next line mutates it

        MarksInCompressedFile marks(copy);
        for (size_t i = 0; i < plain.size(); ++i)
            ASSERT_EQ(marks.get(i), plain[i]);

        EXPECT_LE((marks.approximateMemoryUsage() - sizeof(MarksInCompressedFile)) * 8, plain.size() * max_bits_per_mark);
    };

    // Typical.
    test(gen(10000, 1'000'000, 0), 30);

    // Completely random 64-bit values.
    test(gen(10000, UINT64_MAX - 1, UINT64_MAX - 1), 130);

    // All zeros.
    test(gen(10000, 0, 0), 2);

    // Short.
    test(gen(10, 1000, 1000), 65);

    // Empty.
    test(gen(0, 0, 0), 0);
}
