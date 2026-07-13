#include <gtest/gtest.h>

#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/TemporaryFile.h>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <mutex>
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

/// Races several out-of-prefetch positioned reads against an in-flight prefetch (prefetch covers only
/// [0, 4096), readers start well past that). Such reads skip prefetch_mutex and read directly from impl
/// while the prefetch is in flight. Checks every reader observes the correct data.
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

        /// Prefetch covers only [0, 4096); the readers below all start well past that, so they skip
        /// the mutex and read directly from impl while the prefetch is still in flight.
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

/// Once no prefetch is in flight, concurrent readBigAt() takes the lock-free fast path. This drains
/// the initial prefetch (a nextImpl() read), then races several readBigAt() calls and checks they all
/// return correct data, exercising the fast path that skips prefetch_mutex.
TEST_F(AsynchronousBoundedReadBufferTest, concurrentReadBigAtNoPrefetchInFlight)
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

        /// Issue then fully drain the prefetch so no prefetch is in flight when the readBigAt storm hits.
        read_buffer.prefetch(Priority{0});
        char c = 0;
        ASSERT_EQ(read_buffer.read(&c, 1), 1u);

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
                const size_t range_begin = t * 4096;
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

        EXPECT_TRUE(all_ok.load()) << "no-prefetch readBigAt returned wrong data at iteration " << iteration;
    }
}

/// Regression for the second half of issue #109678: an overlapping-range readBigAt() drains the shared
/// prefetch_future, while a concurrent sequential next() also consumes it. Before the fix, next() consumed
/// prefetch_future without prefetch_mutex, so it could race the readBigAt() drain and both get() the same
/// (moved-from) future -> null shared state -> SIGSEGV. Every prefetch-consumption path now takes
/// prefetch_mutex, so the two serialize; the sequential reader must still observe the file from the start.
TEST_F(AsynchronousBoundedReadBufferTest, overlappingReadBigAtRacesSequentialNext)
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

        /// Prefetch covers the whole small file, so the readBigAt() below overlaps it and drains it.
        read_buffer.prefetch(Priority{0});

        std::atomic<bool> go{false};
        std::atomic<bool> all_ok{true};

        /// Positional reader: overlapping range -> takes prefetch_mutex and drains prefetch_future.
        std::thread positional([&]
        {
            String out;
            out.resize(contents.size());
            while (!go.load())
                ;
            size_t read = read_buffer.readBigAt(out.data(), out.size(), 0, nullptr);
            if (read != contents.size() || out != contents)
                all_ok.store(false);
        });

        /// Sequential reader: read() -> nextImpl() also consumes prefetch_future (now under the same mutex).
        std::thread sequential([&]
        {
            String out;
            out.resize(contents.size());
            while (!go.load())
                ;
            out.resize(read_buffer.read(out.data(), out.size()));
            if (out != contents.substr(0, out.size()))
                all_ok.store(false);
        });

        go.store(true);
        positional.join();
        sequential.join();

        EXPECT_TRUE(all_ok.load()) << "overlapping readBigAt vs sequential next returned wrong data at iteration " << iteration;
    }
}

/// Models a lazily initialized remote reader (like ReadBufferFromAzureBlobStorage, whose backend client
/// is created inside initialize() from nextImpl()). Its nextImpl() parks on a latch so a prefetch can be
/// held in flight while a racing out-of-prefetch readBigAt() runs. readBigAt() is self-contained (uses
/// only request-local state and the immutable contents), as the SeekableReadBuffer contract requires, so
/// it is correct even while next() runs concurrently.
class LazyInitFakeReadBuffer : public DB::ReadBufferFromFileBase
{
public:
    LazyInitFakeReadBuffer(String contents_, std::atomic<bool> & release_next_)
        : ReadBufferFromFileBase(0, nullptr, 0, contents_.size())
        , contents(std::move(contents_))
        , release_next(release_next_)
    {
    }

    String getFileName() const override { return "lazy_init_fake"; }

    off_t getPosition() override { return file_offset_of_buffer_end - available(); }

    off_t seek(off_t offset, int) override
    {
        file_offset_of_buffer_end = offset;
        resetWorkingBuffer();
        return offset;
    }

    bool supportsReadAt() override { return true; }

    /// Self-contained positional read: request-local, touches no member the sequential path mutates.
    size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> &) const override
    {
        const size_t available_bytes = range_begin < contents.size() ? contents.size() - range_begin : 0;
        const size_t to_copy = std::min(n, available_bytes);
        memcpy(to, contents.data() + range_begin, to_copy);
        return to_copy;
    }

private:
    bool nextImpl() override
    {
        /// Background prefetch read. Park on the latch to hold the prefetch in flight while the racing
        /// readBigAt() runs.
        {
            std::unique_lock lock(mutex);
            cv.wait(lock, [&] { return release_next.load(); });
        }

        if (file_offset_of_buffer_end >= contents.size())
            return false;

        const size_t n = std::min(internal_buffer.size(), contents.size() - file_offset_of_buffer_end);
        memcpy(internal_buffer.begin(), contents.data() + file_offset_of_buffer_end, n);
        working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + n);
        pos = working_buffer.begin();
        file_offset_of_buffer_end += n;
        return n != 0;
    }

    const String contents;
    size_t file_offset_of_buffer_end = 0;
    std::atomic<bool> & release_next;
    std::mutex mutex;
    std::condition_variable cv;

public:
    /// Release the parked background next(). Sets the predicate under the wait() mutex to avoid a lost wakeup.
    void wakeNext()
    {
        {
            std::lock_guard lock(mutex);
            release_next.store(true);
        }
        cv.notify_all();
    }
};

/// An out-of-prefetch readBigAt() must take the lock-free range-skip path and NOT drain the prefetch.
/// Proven deterministically: the prefetch's next() is parked on a latch released only AFTER readBigAt()
/// returns, so if readBigAt() had instead taken prefetch_mutex to drain the future it would block on the
/// parked next(); the before_prefetch_drain_for_test hook (fired at the drain point) records that and
/// releases next() to avoid a hang. Its returning without the hook firing proves it skipped the mutex.
TEST_F(AsynchronousBoundedReadBufferTest, outOfPrefetchReadBigAtSkipsMutex)
{
    String contents;
    for (size_t i = 0; i < 100000; ++i)
        contents += static_cast<char>('a' + (i % 26));

    ThreadPoolRemoteFSReader remote_fs_reader(8, 0);

    for (int iteration = 0; iteration < 200; ++iteration)
    {
        std::atomic<bool> release_next{false};

        auto impl = std::make_unique<LazyInitFakeReadBuffer>(contents, release_next);
        auto * impl_raw = impl.get();

        AsynchronousBoundedReadBuffer read_buffer(
            std::move(impl), remote_fs_reader,
            /* buffer_size */ 4096, /* min_bytes_for_seek */ 0,
            Priority{0}, /* page_cache_block_size */ 0, /* enable_prefetches_log */ false);

        /// If readBigAt() reaches the drain point it took prefetch_mutex (which it must not for an
        /// out-of-prefetch read). Record it and release next() so the drain does not hang the test.
        std::atomic<bool> drained_under_mutex{false};
        read_buffer.before_prefetch_drain_for_test = [&] { drained_under_mutex.store(true); impl_raw->wakeNext(); };

        ASSERT_TRUE(read_buffer.supportsReadAt());

        /// Prefetch covers [0, 4096); its background next() parks on the latch, keeping it in flight.
        read_buffer.prefetch(Priority{0});

        /// Out-of-prefetch read (range 8192): must complete via the lock-free path while next() is parked.
        String out;
        out.resize(4096);
        size_t read = read_buffer.readBigAt(out.data(), out.size(), 8192, nullptr);

        EXPECT_FALSE(drained_under_mutex.load())
            << "out-of-prefetch readBigAt took prefetch_mutex (iteration " << iteration << ")";
        EXPECT_EQ(read, 4096u);
        EXPECT_EQ(out, contents.substr(8192, 4096)) << "iteration " << iteration;

        /// Release the parked prefetch and drain it so the buffer destructs cleanly.
        impl_raw->wakeNext();
        char c = 0;
        ASSERT_EQ(read_buffer.read(&c, 1), 1u);
    }
}
