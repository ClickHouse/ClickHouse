#include <Common/SharedMemoryRegion.h>
#include <Common/Exception.h>
#include <Common/getRandomASCIIString.h>
#include <base/defines.h>
#include <base/scope_guard.h>

#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <limits>
#include <mutex>
#include <string>
#include <thread>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <gtest/gtest.h>

using namespace DB;

/// SharedMemoryRegion relies on `O_TMPFILE`/`posix_fallocate` and is Linux-only: its constructor
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
/// into an unrelated child process. The internal fd is not exposed, so we walk /proc/self/fd and
/// match descriptors by the inode of the region's file - the fd was opened as an unnamed
/// `O_TMPFILE`, so its /proc symlink still points at the anonymous name, not at path(). This
/// guards the atomic open(O_TMPFILE | O_CLOEXEC) creation against regression.
TEST(SharedMemoryRegion, BackingDescriptorIsCloseOnExec)
{
    SharedMemoryRegion region(tempDir(), 4096);

    struct stat region_stat;
    ASSERT_EQ(::stat(region.path().c_str(), &region_stat), 0);

    bool found = false;
    for (const auto & entry : std::filesystem::directory_iterator("/proc/self/fd"))
    {
        const int fd = std::stoi(entry.path().filename().string());

        struct stat fd_stat;
        if (::fstat(fd, &fd_stat) != 0 || fd_stat.st_dev != region_stat.st_dev || fd_stat.st_ino != region_stat.st_ino)
            continue;

        const int flags = ::fcntl(fd, F_GETFD);
        ASSERT_NE(flags, -1);
        EXPECT_TRUE(flags & FD_CLOEXEC) << "backing fd for " << region.path() << " is not close-on-exec";
        found = true;
    }
    EXPECT_TRUE(found) << "did not find the region's backing descriptor under /proc/self/fd";
}

/// A server that dies without running destructors (SIGKILL, an OOM kill) leaves its region files
/// behind, and their pages stay committed until the file is unlinked. Creating a region in the same
/// directory must reclaim them - and must leave live regions, which hold an flock, alone.
TEST(SharedMemoryRegion, ReclaimsLeftoverRegionFiles)
{
    /// A fresh directory keeps the test isolated from other region creation in this process.
    const std::string directory
        = tempDir() + "/clickhouse_shm_leftovers_" + std::to_string(::getpid()) + "_" + getRandomASCIIString(16);
    std::filesystem::create_directories(directory);
    SCOPE_EXIT({ std::filesystem::remove_all(directory); });
    ASSERT_EQ(::chmod(directory.c_str(), 0700), 0);

    /// Nobody holds a lock on this one, exactly like a file left by a process that is gone.
    const std::string leftover = directory + "/clickhouse_udf_shm_leftover";
    {
        int fd = ::open(leftover.c_str(), O_CREAT | O_RDWR, 0600);
        ASSERT_NE(fd, -1);
        ASSERT_EQ(::ftruncate(fd, 4096), 0);
        ::close(fd);
    }

    /// This one is locked, exactly like a region whose owner is still running: it must survive.
    const std::string locked = directory + "/clickhouse_udf_shm_locked";
    int locked_fd = ::open(locked.c_str(), O_CREAT | O_RDWR, 0600);
    ASSERT_NE(locked_fd, -1);
    SCOPE_EXIT({ ::close(locked_fd); });
    ASSERT_EQ(::flock(locked_fd, LOCK_EX | LOCK_NB), 0);

    /// A file that is not a region must not be touched, locked or not.
    const std::string unrelated = directory + "/not_a_region";
    {
        int fd = ::open(unrelated.c_str(), O_CREAT | O_RDWR, 0600);
        ASSERT_NE(fd, -1);
        ::close(fd);
    }

    /// A matching prefix is not sufficient evidence that an entry belongs to this mechanism.
    const std::string unrelated_matching_file = directory + "/clickhouse_udf_shm_unrelated";
    {
        int fd = ::open(unrelated_matching_file.c_str(), O_CREAT | O_RDWR, 0644);
        ASSERT_NE(fd, -1);
        ::close(fd);
    }

    const std::string matching_fifo = directory + "/clickhouse_udf_shm_fifo";
    ASSERT_EQ(::mkfifo(matching_fifo.c_str(), 0600), 0);

    SharedMemoryRegion region(directory, 4096);

    EXPECT_FALSE(std::filesystem::exists(leftover));
    EXPECT_TRUE(std::filesystem::exists(locked));
    EXPECT_TRUE(std::filesystem::exists(unrelated));
    EXPECT_TRUE(std::filesystem::exists(unrelated_matching_file));
    EXPECT_TRUE(std::filesystem::exists(matching_fifo));
    EXPECT_TRUE(std::filesystem::exists(region.path()));

    /// A burst of region creations scans this directory only once. Without throttling, N
    /// non-pooled UDF calls would each scan the N live region files and perform O(N^2) work.
    const std::string later_leftover = directory + "/clickhouse_udf_shm_later_leftover";
    {
        int fd = ::open(later_leftover.c_str(), O_CREAT | O_RDWR, 0600);
        ASSERT_NE(fd, -1);
        ::close(fd);
    }

    SharedMemoryRegion another_region(directory, 4096);
    EXPECT_TRUE(std::filesystem::exists(later_leftover));
}

TEST(SharedMemoryRegion, SizeZeroThrows)
{
    EXPECT_THROW(SharedMemoryRegion(tempDir(), 0), DB::Exception);
}

TEST(SharedMemoryRegion, UnsupportedDirectoryRejectedDuringConfigurationValidation)
{
    /// `procfs` cannot host an `O_TMPFILE`; the same probe is used while loading UDF configuration.
    EXPECT_THROW(SharedMemoryRegion::checkSupported("/proc"), DB::Exception);
}

TEST(SharedMemoryRegion, SharedDirectoryWithoutStickyBitRejected)
{
    const std::string directory
        = tempDir() + "/clickhouse_shm_unsafe_permissions_" + std::to_string(::getpid()) + "_" + getRandomASCIIString(16);
    std::filesystem::create_directories(directory);
    SCOPE_EXIT({ std::filesystem::remove_all(directory); });
    ASSERT_EQ(::chmod(directory.c_str(), 0777), 0);

    const std::string candidate = directory + "/clickhouse_udf_shm_must_survive";
    {
        int fd = ::open(candidate.c_str(), O_CREAT | O_RDWR, 0600);
        ASSERT_NE(fd, -1);
        ::close(fd);
    }

    EXPECT_THROW(SharedMemoryRegion::checkSupported(directory), DB::Exception);
    EXPECT_THROW(SharedMemoryRegion(directory, 4096), DB::Exception);
    EXPECT_TRUE(std::filesystem::exists(candidate));
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

/// `shrink` gives the backing pages of a region back after a borrow grew it for one outsized
/// chunk. The file must actually become smaller (that is what releases the tmpfs memory), the
/// path must stay the same, and the surviving prefix must still be readable through both the
/// region and a mapping made by another process.
TEST(SharedMemoryRegion, ShrinkReleasesBackingFileAndKeepsPrefix)
{
    SharedMemoryRegion region(tempDir(), 8192);
    const std::string path = region.path();

    const std::string payload = "payload-before-shrink";
    memcpy(region.data(), payload.data(), payload.size());

    region.shrink(1024);
    EXPECT_EQ(region.size(), 1024u);
    EXPECT_EQ(region.path(), path);
    EXPECT_EQ(std::string(region.data(), payload.size()), payload);

    struct stat st{};
    ASSERT_EQ(::stat(path.c_str(), &st), 0);
    EXPECT_EQ(static_cast<size_t>(st.st_size), 1024u);

    int fd = ::open(path.c_str(), O_RDWR);
    ASSERT_NE(fd, -1);
    void * other = ::mmap(nullptr, region.size(), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ASSERT_NE(other, MAP_FAILED);
    ::close(fd);
    EXPECT_EQ(std::string(static_cast<char *>(other), payload.size()), payload);
    ::munmap(other, region.size());

    /// A shrunk region can be grown again: this is what the next pool borrow does.
    region.grow(4096);
    EXPECT_EQ(region.size(), 4096u);
    EXPECT_EQ(std::string(region.data(), payload.size()), payload);
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
