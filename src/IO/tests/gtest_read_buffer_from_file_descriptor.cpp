#include <gtest/gtest.h>

#include <base/types.h>
#include <Common/filesystemHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/AsynchronousReadBufferFromFile.h>
#include <IO/SynchronousReader.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Disks/IO/ThreadPoolReader.h>

#include <cstring>
#include <random>

using namespace DB;

/// Test that `rewind` fully resets the buffer state, including the `canceled` flag.
/// This covers the scenario in `AsynchronousMetrics` where `/proc` files are read
/// in a loop: if a read fails and cancels the buffer, the next rewind+read
/// cycle must work without hitting the `chassert` in `ReadBuffer::next`.
TEST(ReadBufferFromFileDescriptor, RewindResetsBufferState)
{
    auto tmp_file = createTemporaryFile("/tmp/");

    {
        WriteBufferFromFile out(tmp_file->path());
        writeString(std::string_view{"hello world\n"}, out);
        out.finalize();
    }

    ReadBufferFromFile buf(tmp_file->path());

    /// First read should work.
    String line;
    readStringUntilEOF(line, buf);
    ASSERT_FALSE(line.empty());

    /// Simulate what happens when `nextImpl` throws: manually cancel the buffer.
    buf.cancel();
    ASSERT_TRUE(buf.isCanceled());

    /// `rewind` must fully reset the buffer.
    buf.rewind();

    ASSERT_FALSE(buf.isCanceled()) << "rewind() must reset the canceled flag";
    ASSERT_EQ(buf.getPosition(), 0) << "rewind() must reset file position to zero";
    ASSERT_EQ(buf.available(), 0) << "rewind() must clear the working buffer";

    /// Reading after rewind must return the same content.
    String line2;
    readStringUntilEOF(line2, buf);
    ASSERT_EQ(line, line2);
}

/// Same test for the asynchronous variant of the buffer.
TEST(AsynchronousReadBufferFromFileDescriptor, RewindResetsBufferState)
{
    auto tmp_file = createTemporaryFile("/tmp/");

    {
        WriteBufferFromFile out(tmp_file->path());
        writeString(std::string_view{"hello world\n"}, out);
        out.finalize();
    }

    SynchronousReader reader;
    AsynchronousReadBufferFromFileWithDescriptorsCache buf(reader, {}, tmp_file->path());

    /// First read should work.
    String line;
    readStringUntilEOF(line, buf);
    ASSERT_FALSE(line.empty());

    /// Simulate what happens when `nextImpl` throws: manually cancel the buffer.
    buf.cancel();
    ASSERT_TRUE(buf.isCanceled());

    /// `rewind` must fully reset the buffer.
    buf.rewind();

    ASSERT_FALSE(buf.isCanceled()) << "rewind() must reset the canceled flag";
    ASSERT_EQ(buf.getPosition(), 0) << "rewind() must reset file position to zero";
    ASSERT_EQ(buf.available(), 0) << "rewind() must clear the working buffer";

    /// Reading after rewind must return the same content.
    String line2;
    readStringUntilEOF(line2, buf);
    ASSERT_EQ(line, line2);
}

namespace
{

/// Random reads, prefetches and seeks on the buffer, each checked against the file content at the position a model tracks.
void checkReadBigAgainstModel(bool with_seeks)
{
    std::mt19937_64 rng(42);
    std::string data(3 * 1024 * 1024 + 123, '\0');
    for (auto & c : data)
        c = static_cast<char>(rng());

    auto tmp_file = createTemporaryFile("/tmp/");
    {
        WriteBufferFromFile out(tmp_file->path());
        out.write(data.data(), data.size());
        out.finalize();
    }

    SynchronousReader sync_reader;
    ThreadPoolReader pool_reader(4, 1000000);
    const std::vector<std::pair<std::string, IAsynchronousReader *>> readers{{"SynchronousReader", &sync_reader}, {"ThreadPoolReader", &pool_reader}};

    for (const auto & [reader_name, reader] : readers)
    for (size_t buf_size : {size_t{4097}, size_t{65536}, size_t{131072}})
    for (bool known_size : {true, false})
    {
        SCOPED_TRACE(fmt::format("{}, buffer size {}, file size {}", reader_name, buf_size, known_size ? "known" : "unknown"));

        std::optional<size_t> file_size;
        if (known_size)
            file_size = data.size();
        AsynchronousReadBufferFromFileWithDescriptorsCache buf(*reader, {}, tmp_file->path(), buf_size, -1, nullptr, 0, file_size);

        size_t pos = 0;
        std::string to;
        auto read_and_check = [&](size_t n, bool big)
        {
            to.assign(n, '\0');
            size_t got = big ? buf.readBig(to.data(), n) : buf.read(to.data(), n);
            ASSERT_EQ(got, std::min(n, data.size() - pos)) << "at position " << pos;
            ASSERT_EQ(0, memcmp(to.data(), data.data() + pos, got)) << "wrong bytes at position " << pos;
            pos += got;
        };

        for (size_t i = 0; i < 300; ++i)
        {
            size_t op = rng() % 20;
            if (!with_seeks)
                op = op % 10;

            if (op < 4)
            {
                ASSERT_NO_FATAL_FAILURE(read_and_check(rng() % (3 * buf_size + 1), true));
            }
            else if (op < 7)
            {
                ASSERT_NO_FATAL_FAILURE(read_and_check(rng() % 100, false));
            }
            else if (op < 10)
            {
                buf.prefetch(Priority{});
            }
            else if (op < 13)
            {
                size_t target = rng() % (data.size() + 1);
                ASSERT_EQ(buf.seek(target, SEEK_SET), static_cast<off_t>(target));
                pos = target;
            }
            else if (op < 17)
            {
                size_t back = std::min(pos, 2 * buf_size);
                size_t forward = std::min(data.size() - pos, 2 * buf_size);
                off_t shift = static_cast<off_t>(rng() % (back + forward + 1)) - static_cast<off_t>(back);
                ASSERT_EQ(buf.seek(shift, SEEK_CUR), static_cast<off_t>(pos) + shift);
                pos = static_cast<size_t>(static_cast<off_t>(pos) + shift);
            }
            else
            {
                /// A backward seek of less than one buffer right after a read that may have bypassed the buffer.
                ASSERT_NO_FATAL_FAILURE(read_and_check(2 * buf_size + rng() % buf_size, true));
                if (pos > 0)
                {
                    off_t shift = -static_cast<off_t>(1 + rng() % std::min(pos, buf_size - 1));
                    ASSERT_EQ(buf.seek(shift, SEEK_CUR), static_cast<off_t>(pos) + shift);
                    pos = static_cast<size_t>(static_cast<off_t>(pos) + shift);
                }
            }

            ASSERT_EQ(buf.getPosition(), static_cast<off_t>(pos)) << "after operation " << i;
            if (!with_seeks)
                ASSERT_EQ(buf.count(), pos) << "after operation " << i;
        }

        ASSERT_NO_FATAL_FAILURE(read_and_check(data.size() - pos + 10, true));
        ASSERT_TRUE(buf.eof());
    }
}

}

TEST(AsynchronousReadBufferFromFileDescriptor, ReadBigMatchesFileSequential)
{
    checkReadBigAgainstModel(false);
}

TEST(AsynchronousReadBufferFromFileDescriptor, ReadBigMatchesFileWithSeeks)
{
    checkReadBigAgainstModel(true);
}
