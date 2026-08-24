#include <random>
#include <string>
#include <vector>
#include <gtest/gtest.h>

#include <Formats/MarkInCompressedFile.h>

using namespace DB;

TEST(Marks, Compression)
{
    std::random_device dev;
    std::mt19937 rng(dev());

    auto gen = [&](size_t count, size_t max_x_increment, size_t max_y_increment)
    {
        size_t x = 0;
        size_t y = 0;
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

        auto marks = MarksInCompressedFile::create(copy);
        for (size_t i = 0; i < plain.size(); ++i)
            ASSERT_EQ(marks->get(i), plain[i]);

        EXPECT_LE((marks->approximateMemoryUsage() - sizeof(MarksInCompressedFile)) * 8, plain.size() * max_bits_per_mark);
    };

    {
        SCOPED_TRACE("Typical");
        test(gen(10000, 1'000'000, 0), 30);
    }


    {
        SCOPED_TRACE("Completely random 64-bit values");
        test(gen(10000, UINT64_MAX - 1, UINT64_MAX - 1), 130);
    }

    {
        SCOPED_TRACE("All zeros");
        test(gen(10000, 0, 0), 2);
    }

    {
        SCOPED_TRACE("Short");
        test(gen(10, 1000, 1000), 65);
    }

    {
        SCOPED_TRACE("Empty");
        test(gen(0, 0, 0), 0);
    }
}

namespace
{

MarkInCompressedFile mark(size_t x, size_t y = 0)
{
    return MarkInCompressedFile{.offset_in_compressed_file = x, .offset_in_decompressed_block = y};
}

/// What MergeTreeReaderStream::hasAtMostNDistinctMarks counts by scanning: groups
/// of consecutive equal marks, saturated the same way.
size_t referenceDistinctMarksCapped(const PODArray<MarkInCompressedFile> & plain)
{
    size_t count = 0;
    MarkInCompressedFile last{UINT64_MAX, UINT64_MAX};
    for (const auto & m : plain)
    {
        if (m != last)
        {
            last = m;
            if (count < MarksInCompressedFile::DISTINCT_MARKS_CAP)
                ++count;
        }
    }
    return count;
}

}

TEST(Marks, DistinctMarksCapped)
{
    const size_t block = MarksInCompressedFile::MARKS_PER_BLOCK;

    /// Chunk sizes that straddle MARKS_PER_BLOCK so the count must survive both
    /// block flushes and the builder's internal buffering of partial chunks.
    const std::vector<size_t> chunk_sizes{1, 7, 255, 256, 257, 300, 511, 513};

    auto check = [&](const PODArray<MarkInCompressedFile> & plain, size_t expected)
    {
        const size_t reference = referenceDistinctMarksCapped(plain);
        ASSERT_EQ(reference, expected) << "test case itself is wrong";

        {
            SCOPED_TRACE("create");
            auto marks = MarksInCompressedFile::create(plain);
            ASSERT_EQ(marks->getNumDistinctMarksCapped(), expected);
            /// The value must describe the marks that were actually stored.
            ASSERT_EQ(marks->getNumberOfMarks(), plain.size());
            for (size_t i = 0; i < plain.size(); ++i)
                ASSERT_EQ(marks->get(i), plain[i]) << "at " << i;
        }

        for (size_t chunk : chunk_sizes)
        {
            SCOPED_TRACE("addMarks chunk=" + std::to_string(chunk));
            MarksInCompressedFile::Builder builder(plain.size());
            size_t fed = 0;
            while (fed < plain.size())
            {
                size_t n = std::min(chunk, plain.size() - fed);
                builder.addMarks(plain.data() + fed, n);
                fed += n;
            }
            auto marks = builder.finish();
            ASSERT_EQ(marks->getNumDistinctMarksCapped(), expected);
            /// The value must describe the marks that were actually stored.
            ASSERT_EQ(marks->getNumberOfMarks(), plain.size());
            for (size_t i = 0; i < plain.size(); ++i)
                ASSERT_EQ(marks->get(i), plain[i]) << "at " << i;
        }
    };

    auto repeat = [](MarkInCompressedFile m, size_t n)
    {
        PODArray<MarkInCompressedFile> out(n);
        for (size_t i = 0; i < n; ++i)
            out[i] = m;
        return out;
    };

    {
        SCOPED_TRACE("Empty");
        check(PODArray<MarkInCompressedFile>{}, 0);
    }

    {
        SCOPED_TRACE("Single mark");
        check(repeat(mark(42), 1), 1);
    }

    {
        SCOPED_TRACE("All identical, spanning many blocks");
        check(repeat(mark(7, 3), 4 * block + 13), 1);
    }

    {
        SCOPED_TRACE("Two positions, transition inside a block");
        auto plain = repeat(mark(0), block + 10);
        for (size_t i = 5; i < plain.size(); ++i)
            plain[i] = mark(100);
        check(plain, 2);
    }

    {
        SCOPED_TRACE("Two positions, transition exactly on a block boundary");
        auto plain = repeat(mark(0), 3 * block);
        for (size_t i = block; i < plain.size(); ++i)
            plain[i] = mark(100);
        check(plain, 2);
    }

    {
        SCOPED_TRACE("Single dictionary with final mark: last mark differs");
        auto plain = repeat(mark(500), 2 * block + 1);
        plain[plain.size() - 1] = mark(900, 17);
        check(plain, 2);
    }

    {
        SCOPED_TRACE("Three positions saturates the cap");
        auto plain = repeat(mark(0), 3 * block);
        for (size_t i = block; i < 2 * block; ++i)
            plain[i] = mark(10);
        for (size_t i = 2 * block; i < plain.size(); ++i)
            plain[i] = mark(20);
        check(plain, MarksInCompressedFile::DISTINCT_MARKS_CAP);
    }

    {
        SCOPED_TRACE("Every mark distinct saturates the cap");
        PODArray<MarkInCompressedFile> plain(2 * block + 5);
        for (size_t i = 0; i < plain.size(); ++i)
            plain[i] = mark(i * 13, i % 4);
        check(plain, MarksInCompressedFile::DISTINCT_MARKS_CAP);
    }

    {
        SCOPED_TRACE("Repeats are not deduplicated across a gap");
        /// A, A, B, B, A: five marks, three groups.
        PODArray<MarkInCompressedFile> plain(5);
        plain[0] = mark(1);
        plain[1] = mark(1);
        plain[2] = mark(2);
        plain[3] = mark(2);
        plain[4] = mark(1);
        check(plain, 3);
    }

    {
        SCOPED_TRACE("Marks differing only in offset_in_decompressed_block");
        auto plain = repeat(mark(80, 0), block + 4);
        for (size_t i = block; i < plain.size(); ++i)
            plain[i] = mark(80, 64);
        check(plain, 2);
    }
}
