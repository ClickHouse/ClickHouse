#include <Common/SharedMemoryRegion.h>
#include <Common/Exception.h>
#include <base/defines.h>

#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <limits>
#include <mutex>
#include <string>
#include <thread>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <gtest/gtest.h>

using namespace DB;

/// SharedMemoryRegion relies on `mkostemp`/`posix_fallocate` and is Linux-only: its constructor
/// throws on other platforms (see `SharedMemoryRegion::checkSupported`). Gate the whole suite so
/// it neither fails nor pins resources on non-Linux builds of unit_tests_dbms.
#if defined(OS_LINUX)

namespace
{
std::string tempDir()
{
    return std::filesystem::temp_directory_path().string();
}
}

TEST(SharedMemoryRegion, CreateReadWrite)
{
    SharedMemoryRegion region(tempDir(), 4096);

    EXPECT_EQ(region.size(), 4096u);
    EXPECT_NE(region.data(), nullptr);
    EXPECT_FALSE(region.path().empty());
    EXPECT_TRUE(std::filesystem::exists(region.path()));

    const std::string payload = "hello shared memory";
    memcpy(region.data(), payload.data(), payload.size());
    EXPECT_EQ(std::string(region.data(), payload.size()), payload);
}

/// The backing descriptor must be close-on-exec: otherwise a concurrent fork+exec on another
/// thread could leak this fd (holding the mapped UDF data and pinning tmpfs storage after unlink)
/// into an unrelated child process. The internal fd is not exposed, but while the region is alive
/// its file stays linked at path(), so we can locate the descriptor via /proc/self/fd and verify
/// its FD_CLOEXEC flag. This guards the atomic mkostemp(O_CLOEXEC) creation against regression.
TEST(SharedMemoryRegion, BackingDescriptorIsCloseOnExec)
{
    SharedMemoryRegion region(tempDir(), 4096);

    bool found = false;
    for (const auto & entry : std::filesystem::directory_iterator("/proc/self/fd"))
    {
        std::error_code ec;
        const auto target = std::filesystem::read_symlink(entry.path(), ec);
        if (ec || target.string() != region.path())
            continue;

        const int fd = std::stoi(entry.path().filename().string());
        const int flags = ::fcntl(fd, F_GETFD);
        ASSERT_NE(flags, -1);
        EXPECT_TRUE(flags & FD_CLOEXEC) << "backing fd for " << region.path() << " is not close-on-exec";
        found = true;
    }
    EXPECT_TRUE(found) << "did not find the region's backing descriptor under /proc/self/fd";
}

TEST(SharedMemoryRegion, SizeZeroThrows)
{
    EXPECT_THROW(SharedMemoryRegion(tempDir(), 0), DB::Exception);
}

/// A size that does not fit into a signed off_t must be rejected instead of overflowing ftruncate
/// (and, at the consumer level, the Int64 memory-tracker charge).
TEST(SharedMemoryRegion, OversizedThrows)
{
    const size_t too_large = static_cast<size_t>(std::numeric_limits<off_t>::max()) + 1;
    EXPECT_THROW(SharedMemoryRegion(tempDir(), too_large), DB::Exception);
}

TEST(SharedMemoryRegion, UnlinkOnDestroy)
{
    std::string path;
    {
        SharedMemoryRegion region(tempDir(), 1024);
        path = region.path();
        EXPECT_TRUE(std::filesystem::exists(path));
    }
    EXPECT_FALSE(std::filesystem::exists(path));
}

/// The whole point of MAP_SHARED: a second, independent mapping of the same file (as the child
/// process does) observes writes made through the region, and vice versa.
TEST(SharedMemoryRegion, SharedAcrossMappings)
{
    SharedMemoryRegion region(tempDir(), 4096);

    int fd = ::open(region.path().c_str(), O_RDWR);
    ASSERT_NE(fd, -1);
    void * other = ::mmap(nullptr, region.size(), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ASSERT_NE(other, MAP_FAILED);
    ::close(fd);

    auto * other_data = static_cast<char *>(other);

    /// Server -> child direction.
    const std::string in = "input-from-server";
    memcpy(region.data(), in.data(), in.size());
    EXPECT_EQ(std::string(other_data, in.size()), in);

    /// Child -> server direction (written after the input, as the protocol does).
    const std::string out = "output-from-child";
    memcpy(other_data + 2048, out.data(), out.size());
    EXPECT_EQ(std::string(region.data() + 2048, out.size()), out);

    ::munmap(other, region.size());
}

/// Growing the region enlarges the backing file, preserves the previously written bytes, keeps the
/// same path, and makes the larger size visible to a freshly opened second mapping (as the child
/// does on its next request).
TEST(SharedMemoryRegion, GrowPreservesDataAndEnlargesFile)
{
    SharedMemoryRegion region(tempDir(), 1024);
    const std::string path = region.path();

    const std::string payload = "payload-before-growth";
    memcpy(region.data(), payload.data(), payload.size());

    region.grow(8192);
    EXPECT_EQ(region.size(), 8192u);
    EXPECT_EQ(region.path(), path);
    EXPECT_EQ(std::string(region.data(), payload.size()), payload);

    /// The on-disk file (and hence a second mapping) reflects the new size.
    int fd = ::open(path.c_str(), O_RDWR);
    ASSERT_NE(fd, -1);
    struct stat st{};
    ASSERT_EQ(::fstat(fd, &st), 0);
    EXPECT_EQ(static_cast<size_t>(st.st_size), 8192u);

    void * other = ::mmap(nullptr, region.size(), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ASSERT_NE(other, MAP_FAILED);
    ::close(fd);
    EXPECT_EQ(std::string(static_cast<char *>(other), payload.size()), payload);
    ::munmap(other, region.size());
}

TEST(SharedMemoryRegion, GrowToSmallerOrEqualThrows)
{
    SharedMemoryRegion region(tempDir(), 4096);
    EXPECT_THROW(region.grow(4096), DB::Exception);
    EXPECT_THROW(region.grow(1024), DB::Exception);
    EXPECT_EQ(region.size(), 4096u);
}

/// When `grow` enlarges the backing file with `ftruncate` but reserving the new range fails, it
/// must roll the file size back; otherwise a pooled region left with a larger tmpfs file than
/// `region.size` would leak unaccounted memory across borrows. We force the
/// `ftruncate`-succeeds/reservation-fails path by growing to a size that a sparse tmpfs file
/// accepts but cannot back with pages. This needs tmpfs (`/dev/shm`); skip where it is not
/// available.
TEST(SharedMemoryRegion, GrowRollsBackFileSizeOnReserveFailure)
{
    const std::string shm_dir = "/dev/shm";
    if (!std::filesystem::exists(shm_dir))
        GTEST_SKIP() << "/dev/shm not available";

    SharedMemoryRegion region(shm_dir, 4096);
    const std::string path = region.path();
    region.data()[0] = 'Z';

    /// ftruncate to this size succeeds on tmpfs (sparse), but mmap of ~1 EiB cannot be reserved.
    const size_t huge = static_cast<size_t>(1) << 60;
    EXPECT_THROW(region.grow(huge), DB::Exception);

    /// The object is unchanged: same size, data intact and still readable.
    EXPECT_EQ(region.size(), 4096u);
    EXPECT_EQ(region.data()[0], 'Z');

    /// The backing file must have been rolled back to the old size, not left enlarged.
    struct stat st{};
    ASSERT_EQ(::stat(path.c_str(), &st), 0);
    EXPECT_EQ(static_cast<size_t>(st.st_size), 4096u);
}

/// Models the server<->child ping-pong within one process: a producer writes an "input" area and
/// hands off; a consumer reads it and writes an "output" area; the producer reads the output. The
/// handoff is fully synchronized, so the access is race-free (a clean target for ThreadSanitizer).
TEST(SharedMemoryRegion, SynchronizedHandoff)
{
    SharedMemoryRegion region(tempDir(), 4096);

    std::mutex mutex;
    std::condition_variable cv;
    int turn = 0; /// 0 = producer writes input, 1 = consumer writes output, 2 = done

    const std::string request = "request-payload";
    const std::string response = "response-payload";

    std::thread consumer([&]
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return turn == 1; });

        EXPECT_EQ(std::string(region.data(), request.size()), request);
        memcpy(region.data() + 2048, response.data(), response.size());

        turn = 2;
        cv.notify_all();
    });

    {
        std::unique_lock lock(mutex);
        memcpy(region.data(), request.data(), request.size());
        turn = 1;
        cv.notify_all();
        cv.wait(lock, [&] { return turn == 2; });
    }

    consumer.join();
    EXPECT_EQ(std::string(region.data() + 2048, response.size()), response);
}

#endif
