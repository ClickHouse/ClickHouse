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

/// Races several out-of-prefetch positioned reads against an in-flight prefetch (prefetch covers
/// only [0, 4096), readers start well past that). While the prefetch is in flight these reads still
/// serialize on prefetch_mutex, because the prefetch's background impl->next() runs against impl and
/// readBigAt() may not run in parallel with next() (SeekableReadBuffer contract; lazy backends such as
/// Azure would otherwise null-deref). This checks every reader observes the correct data regardless of
/// which side of the prefetch range it targets.
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

/// A backend whose readBigAt() is NOT safe against a concurrent sequential read, modelling the lazily
/// initialized remote readers (e.g. ReadBufferFromAzureBlobStorage creates its blob_client inside
/// initialize() from nextImpl(); readBigAt() dereferences it). The reader is created uninitialized;
/// initialization happens inside the FIRST nextImpl(), which blocks on a latch to keep the prefetch
/// "in flight" while a racing out-of-prefetch readBigAt() runs. If readBigAt() is called before
/// nextImpl() has initialized the backend, it records a violation instead of null-dereferencing -- this
/// is exactly the forbidden overlap the prefetch_estimated_end guard must prevent. The local pread
/// backend used by the other tests can never surface this because both its paths are positional.
class LazyInitFakeReadBuffer : public DB::ReadBufferFromFileBase
{
public:
    LazyInitFakeReadBuffer(String contents_, std::atomic<bool> & violation_, std::atomic<bool> & release_next_)
        : ReadBufferFromFileBase(0, nullptr, 0, contents_.size())
        , contents(std::move(contents_))
        , violation(violation_)
        , release_next(release_next_)
    {
    }

    String getFileName() const override { return "lazy_init_fake"; }

    off_t getPosition() override { return file_offset_of_buffer_end - available(); }

    off_t seek(off_t offset, int) override
    {
        /// Called from submit()/execute() before the read; must not touch the lazy backend.
        file_offset_of_buffer_end = offset;
        resetWorkingBuffer();
        return offset;
    }

    bool supportsReadAt() override { return true; }

    /// Positional read. Unsafe against a concurrent next(): if the backend was not initialized yet
    /// (init happens inside nextImpl()), record a violation, mirroring an Azure blob_client null-deref.
    size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> &) const override
    {
        if (!initialized.load(std::memory_order_acquire))
            violation.store(true);

        const size_t available_bytes = range_begin < contents.size() ? contents.size() - range_begin : 0;
        const size_t to_copy = std::min(n, available_bytes);
        memcpy(to, contents.data() + range_begin, to_copy);
        return to_copy;
    }

private:
    bool nextImpl() override
    {
        /// The background prefetch read. Block on the latch to keep the prefetch in flight while the
        /// racing readBigAt() runs, then "initialize" the backend (the moment Azure creates blob_client).
        {
            std::unique_lock lock(mutex);
            cv.wait(lock, [&] { return release_next.load(); });
        }
        initialized.store(true, std::memory_order_release);

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
    std::atomic<bool> & violation;
    std::atomic<bool> & release_next;
    std::atomic<bool> initialized{false};
    std::mutex mutex;
    std::condition_variable cv;

public:
    /// Let the blocked background next() proceed. Sets the predicate under the same mutex used by
    /// wait() to avoid a lost wakeup.
    void wakeNext()
    {
        {
            std::lock_guard lock(mutex);
            release_next.store(true);
        }
        cv.notify_all();
    }
};

/// Directly targets the SeekableReadBuffer contract the prefetch_estimated_end guard enforces: an
/// out-of-prefetch readBigAt() must NOT run in parallel with the in-flight prefetch's background
/// next() on the same impl. Uses a backend whose readBigAt() is unsafe before the (lazy) init done
/// inside next() -- the pre-fix range-based skip would let the out-of-prefetch read hit the fast path
/// and observe the uninitialized backend (violation=true); the fixed no-prefetch-in-flight-only skip
/// keeps it under prefetch_mutex until the prefetch drains, so it never overlaps next().
TEST_F(AsynchronousBoundedReadBufferTest, outOfPrefetchReadBigAtDoesNotRaceInFlightNext)
{
    String contents;
    for (size_t i = 0; i < 100000; ++i)
        contents += static_cast<char>('a' + (i % 26));

    ThreadPoolRemoteFSReader remote_fs_reader(8, 0);

    for (int iteration = 0; iteration < 200; ++iteration)
    {
        std::atomic<bool> violation{false};
        std::atomic<bool> release_next{false};

        auto impl = std::make_unique<LazyInitFakeReadBuffer>(contents, violation, release_next);
        auto * impl_raw = impl.get();

        AsynchronousBoundedReadBuffer read_buffer(
            std::move(impl), remote_fs_reader,
            /* buffer_size */ 4096, /* min_bytes_for_seek */ 0,
            Priority{0}, /* page_cache_block_size */ 0, /* enable_prefetches_log */ false);

        ASSERT_TRUE(read_buffer.supportsReadAt());

        /// Start a prefetch covering [0, 4096). Its background next() blocks in the latch, so the
        /// prefetch stays in flight (and the backend uninitialized) while the reader below races it.
        read_buffer.prefetch(Priority{0});

        /// Read well past the prefetch range: the pre-fix range-based skip would take the lock-free
        /// fast path here and call impl->readBigAt() while next() is still pending -> violation.
        std::atomic<bool> reader_done{false};
        std::thread reader([&]
        {
            String out;
            out.resize(4096);
            read_buffer.readBigAt(out.data(), out.size(), 8192, nullptr);
            reader_done.store(true);
        });

        /// Give the racing reader time to reach (and, under the fixed code, block on) readBigAt while
        /// the prefetch is still in flight, before we let the background next() proceed.
        std::this_thread::sleep_for(std::chrono::milliseconds(2));

        impl_raw->wakeNext();

        reader.join();

        EXPECT_FALSE(violation.load())
            << "out-of-prefetch readBigAt ran while the in-flight prefetch's next() had not initialized "
               "the backend (iteration " << iteration << ")";
    }
}
