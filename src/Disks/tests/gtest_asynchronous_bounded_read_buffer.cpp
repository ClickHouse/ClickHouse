#include <gtest/gtest.h>

#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/TemporaryFile.h>
#include <atomic>
#include <filesystem>
#include <thread>
#include <vector>


using namespace DB;
namespace fs = std::filesystem;

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

/// Regression test for https://github.com/ClickHouse/ClickHouse/issues/109678
/// readBigAt() is safe for concurrent use (the Parquet RandomRead path calls it from several
/// fast-pool threads at once). When a sequential prefetch is in flight, the racing threads used
/// to consume the shared prefetch_future without synchronization: one thread would move it out and
/// get() it, another would get() the moved-from future and dereference a null shared state -> SIGSEGV.
/// The threads must serialize on draining the prefetch and every reader must observe the correct data.
TEST_F(AsynchronousBoundedReadBufferTest, concurrentReadBigAtWithPrefetch)
{
    const String contents = getAlphabetWithDigits(); /// 36 bytes: a..z0..9
    String file_path = makeTempFile(contents);
    ThreadPoolRemoteFSReader remote_fs_reader(8, 0);

    for (int iteration = 0; iteration < 200; ++iteration)
    {
        AsynchronousBoundedReadBuffer read_buffer(
            createReadBufferFromFileBase(file_path, ReadSettings{}), remote_fs_reader,
            DBMS_DEFAULT_BUFFER_SIZE, /* min_bytes_for_seek */ 0,
            Priority{0}, /* page_cache_block_size */ 0, /* enable_prefetches_log */ false);

        ASSERT_TRUE(read_buffer.supportsReadAt());

        /// Start a sequential prefetch that covers the whole (small) file, then race several
        /// readBigAt() calls against it, matching Prefetcher::readSync's RandomRead branch.
        read_buffer.prefetch(Priority{0});

        constexpr size_t num_threads = 8;
        std::atomic<size_t> ready{0};
        std::atomic<bool> go{false};
        std::vector<std::thread> threads;
        std::atomic<bool> all_ok{true};
        threads.reserve(num_threads);
        for (size_t t = 0; t < num_threads; ++t)
        {
            threads.emplace_back([&, t]()
            {
                const size_t range_begin = t % contents.size();
                const size_t n = contents.size() - range_begin;
                String out;
                out.resize(n);

                ready.fetch_add(1);
                while (!go.load())
                    ; /// spin so all threads hit readBigAt as close together as possible

                size_t read = read_buffer.readBigAt(out.data(), n, range_begin, nullptr);
                if (read != n || out != contents.substr(range_begin))
                    all_ok.store(false);
            });
        }

        while (ready.load() != num_threads)
            ;
        go.store(true);

        for (auto & thread : threads)
            thread.join();

        EXPECT_TRUE(all_ok.load()) << "concurrent readBigAt returned wrong data at iteration " << iteration;
    }
}

/// readBigAt() for a range that starts at or past the estimated prefetch end must not wait on
/// prefetch_mutex: it reads straight from impl. This races such out-of-prefetch reads against an
/// in-flight prefetch and checks every reader observes the correct data (correctness of the
/// mutex-skipping narrowing requested in the PR review).
TEST_F(AsynchronousBoundedReadBufferTest, concurrentOutOfPrefetchReadBigAt)
{
    String contents;
    for (size_t i = 0; i < 100000; ++i)
        contents += static_cast<char>('a' + (i % 26));
    String file_path = makeTempFile(contents);
    ThreadPoolRemoteFSReader remote_fs_reader(8, 0);

    for (int iteration = 0; iteration < 50; ++iteration)
    {
        AsynchronousBoundedReadBuffer read_buffer(
            createReadBufferFromFileBase(file_path, ReadSettings{}), remote_fs_reader,
            /* buffer_size */ 4096, /* min_bytes_for_seek */ 0,
            Priority{0}, /* page_cache_block_size */ 0, /* enable_prefetches_log */ false);

        ASSERT_TRUE(read_buffer.supportsReadAt());

        /// Prefetch covers only [0, 4096); the readers below all start well past that, so they must
        /// skip the mutex and read directly from impl while the prefetch is still in flight.
        read_buffer.prefetch(Priority{0});

        constexpr size_t num_threads = 8;
        std::atomic<size_t> ready{0};
        std::atomic<bool> go{false};
        std::atomic<bool> all_ok{true};
        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        for (size_t t = 0; t < num_threads; ++t)
        {
            threads.emplace_back([&, t]()
            {
                const size_t range_begin = 8192 + t * 4096;
                const size_t n = 4096;
                String out;
                out.resize(n);

                ready.fetch_add(1);
                while (!go.load())
                    ;

                size_t read = read_buffer.readBigAt(out.data(), n, range_begin, nullptr);
                if (read != n || out != contents.substr(range_begin, n))
                    all_ok.store(false);
            });
        }

        while (ready.load() != num_threads)
            ;
        go.store(true);

        for (auto & thread : threads)
            thread.join();

        EXPECT_TRUE(all_ok.load()) << "out-of-prefetch readBigAt returned wrong data at iteration " << iteration;
    }
}
