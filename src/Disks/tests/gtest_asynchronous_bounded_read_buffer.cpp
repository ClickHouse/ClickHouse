#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/TemporaryFile.h>
#include <array>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <thread>


using namespace DB;
namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event RemoteFSPrefetchedReads;
}

class AsynchronousBoundedReadBufferTest : public ::testing::TestWithParam<const char *>
{
public:
    AsynchronousBoundedReadBufferTest() { fs::create_directories(temp_folder.path()); }

    String makeTempFile(const String & contents)
    {
        String path = fmt::format("{}/{}", temp_folder.path(), counter);
        ++counter;

        WriteBufferFromFile out{path};
        out.write(contents.data(), contents.size());
        out.finalize();

        return path;
    }

private:
    Poco::TemporaryFile temp_folder;
    size_t counter = 0;
};

static String getAlphabetWithDigits()
{
    String contents;
    for (char c = 'a'; c <= 'z'; ++c)
        contents += c;
    for (char c = '0'; c <= '9'; ++c)
        contents += c;
    return contents;
}


TEST_F(AsynchronousBoundedReadBufferTest, setReadUntilPosition)
{
    String file_path = makeTempFile(getAlphabetWithDigits());
    ThreadPoolRemoteFSReader remote_fs_reader(4, 0);

    for (bool with_prefetch : {false, true})
    {
        AsynchronousBoundedReadBuffer read_buffer(
            createReadBufferFromFileBase(file_path, ReadSettings{}), remote_fs_reader,
            DBMS_DEFAULT_BUFFER_SIZE, /* min_bytes_for_seek */ 0,
            Priority{0}, /* page_cache_block_size */ 0, /* enable_prefetches_log */ false);
        read_buffer.setReadUntilPosition(20);

        auto try_read = [&](size_t count)
        {
            if (with_prefetch)
                read_buffer.prefetch(Priority{0});

            String str;
            str.resize(count);
            str.resize(read_buffer.read(str.data(), str.size()));
            return str;
        };

        EXPECT_EQ(try_read(15), "abcdefghijklmno");
        EXPECT_EQ(try_read(15), "pqrst");
        EXPECT_EQ(try_read(15), "");

        read_buffer.setReadUntilPosition(25);

        EXPECT_EQ(try_read(15), "uvwxy");
        EXPECT_EQ(try_read(15), "");

        read_buffer.setReadUntilEnd();

        EXPECT_EQ(try_read(15), "z0123456789");
        EXPECT_EQ(try_read(15), "");
    }
}

TEST_F(AsynchronousBoundedReadBufferTest, concurrentReadBigAtWithPrefetch)
{
    /// readBigAt may be called concurrently (e.g. by ParallelReadBuffer), possibly while an initial
    /// prefetch is in flight. Concurrent calls used to race on consuming prefetch_future.

    String contents;
    contents.reserve(100000);
    for (size_t i = 0; i < 100000; ++i)
        contents += static_cast<char>('a' + i % 26);

    const String file_path = makeTempFile(contents);
    ThreadPoolRemoteFSReader remote_fs_reader(4, 0);

    constexpr size_t num_threads = 4;
    constexpr size_t num_iterations = 500;
    /// Smaller than the file, so the prefetch is usually still in flight when the reads run.
    constexpr size_t buffer_size = 16384;

    const auto prefetched_reads_before = ProfileEvents::global_counters[ProfileEvents::RemoteFSPrefetchedReads];

    for (size_t iteration = 0; iteration < num_iterations; ++iteration)
    {
        AsynchronousBoundedReadBuffer read_buffer(
            createReadBufferFromFileBase(file_path, ReadSettings{}), remote_fs_reader,
            buffer_size, /* min_bytes_for_seek */ 0,
            Priority{0}, /* page_cache_block_size */ 0, /* enable_prefetches_log */ false);

        read_buffer.prefetch(Priority{0});

        std::atomic<size_t> ready{0};
        std::array<String, num_threads> errors;
        std::vector<std::thread> threads;

        for (size_t t = 0; t < num_threads; ++t)
        {
            threads.emplace_back([&, t]
            {
                /// Barrier, to maximize the chance that the readBigAt calls overlap.
                ready.fetch_add(1);
                while (ready.load() < num_threads)
                    ;

                try
                {
                    /// Threads read different, partially overlapping ranges.
                    const size_t offset = (t < 2) ? 1000 * (t + 1) : 20000 * t;
                    const size_t count = 30000;
                    String buf(count, 0);
                    size_t total = 0;
                    while (total < count)
                    {
                        size_t read = read_buffer.readBigAt(buf.data() + total, count - total, offset + total, nullptr);
                        if (read == 0)
                            break;
                        total += read;
                    }
                    if (total != count)
                        errors[t] = fmt::format("short read: {} instead of {}", total, count);
                    else if (memcmp(buf.data(), contents.data() + offset, count) != 0)
                        errors[t] = "read data does not match file contents";
                }
                catch (...)
                {
                    errors[t] = getCurrentExceptionMessage(true);
                }
            });
        }

        for (auto & thread : threads)
            thread.join();

        for (size_t t = 0; t < num_threads; ++t)
            ASSERT_EQ(errors[t], "") << "thread " << t << ", iteration " << iteration;
    }

    /// The prefetched data is retained after being consumed, so in every iteration both threads
    /// whose ranges start inside it must have been served from it, not only the consuming one.
    const auto prefetched_reads = ProfileEvents::global_counters[ProfileEvents::RemoteFSPrefetchedReads] - prefetched_reads_before;
    EXPECT_GE(prefetched_reads, 2 * num_iterations);
}

TEST_F(AsynchronousBoundedReadBufferTest, readBigAtFromRetainedPrefetch)
{
    String contents;
    contents.reserve(100000);
    for (size_t i = 0; i < 100000; ++i)
        contents += static_cast<char>('a' + i % 26);

    const String file_path = makeTempFile(contents);
    ThreadPoolRemoteFSReader remote_fs_reader(4, 0);

    /// Smaller than the file, so the prefetch covers only its prefix.
    constexpr size_t buffer_size = 16384;

    AsynchronousBoundedReadBuffer read_buffer(
        createReadBufferFromFileBase(file_path, ReadSettings{}), remote_fs_reader,
        buffer_size, /* min_bytes_for_seek */ 0,
        Priority{0}, /* page_cache_block_size */ 0, /* enable_prefetches_log */ false);

    read_buffer.prefetch(Priority{0});

    const auto prefetched_reads_before = ProfileEvents::global_counters[ProfileEvents::RemoteFSPrefetchedReads];
    auto prefetched_reads = [&] { return ProfileEvents::global_counters[ProfileEvents::RemoteFSPrefetchedReads] - prefetched_reads_before; };

    auto read_at = [&](size_t offset, size_t count)
    {
        String buf(count, 0);
        size_t total = 0;
        while (total < count)
        {
            size_t read = read_buffer.readBigAt(buf.data() + total, count - total, offset + total, nullptr);
            if (read == 0)
                break;
            total += read;
        }
        EXPECT_EQ(total, count);
        EXPECT_EQ(buf, contents.substr(offset, count));
    };

    /// A read past the prefetched range consumes the prefetch, but retains its data.
    read_at(50000, 10000);
    EXPECT_EQ(prefetched_reads(), 0);

    /// Reads inside the prefetched range are served from the retained data, each of them.
    read_at(0, 10000);
    EXPECT_EQ(prefetched_reads(), 1);
    read_at(5000, 5000);
    EXPECT_EQ(prefetched_reads(), 2);

    /// A read crossing the end of the prefetched range: the head is served from the retained
    /// data, the suffix is read directly.
    read_at(10000, 20000);
    EXPECT_EQ(prefetched_reads(), 3);
}
