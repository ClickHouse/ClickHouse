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
#include <string_view>
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
/// Mirrors the name the implementation builds: the effective uid is part of it, so that a shared
/// parent like `/dev/shm` can hold one of these per OS user.
std::string regionDirectoryName()
{
    return ".clickhouse-udf-shared-memory-" + std::to_string(::geteuid());
}

/// A directory the regions can actually live in. They need `O_TMPFILE` and `posix_fallocate`, which
/// not every filesystem provides: `/dev/shm` is tmpfs, always provides both, and is where the feature
/// puts its regions by default, so prefer it and fall back to the system temporary directory only
/// where there is no `/dev/shm` at all.
std::string regionDir()
{
    static const std::string dir = std::filesystem::is_directory("/dev/shm")
        ? std::string("/dev/shm")
        : std::filesystem::temp_directory_path().string();
    return dir;
}
}

TEST(SharedMemoryRegion, CreateReadWrite)
{
    SharedMemoryRegion region(regionDir(), 4096);

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
    SharedMemoryRegion region(regionDir(), 4096);

    struct stat region_stat{};
    ASSERT_EQ(::stat(region.path().c_str(), &region_stat), 0);

    bool found = false;
    for (const auto & entry : std::filesystem::directory_iterator("/proc/self/fd"))
    {
        const int fd = std::stoi(entry.path().filename().string());

        struct stat fd_stat{};
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
        = regionDir() + "/clickhouse_shm_leftovers_" + std::to_string(::getpid()) + "_" + getRandomASCIIString(16);
    std::filesystem::create_directories(directory);
    SCOPE_EXIT({ std::filesystem::remove_all(directory); });
    ASSERT_EQ(::chmod(directory.c_str(), 0700), 0);

    const std::string private_directory = directory + "/" + regionDirectoryName();
    std::filesystem::create_directories(private_directory);
    ASSERT_EQ(::chmod(private_directory.c_str(), 0700), 0);

    /// Nobody holds a lock on this one, exactly like a file left by a process that is gone.
    const std::string leftover = private_directory + "/clickhouse_udf_shm_leftover";
    {
        int fd = ::open(leftover.c_str(), O_CREAT | O_RDWR, 0600);
        ASSERT_NE(fd, -1);
        ASSERT_EQ(::ftruncate(fd, 4096), 0);
        ::close(fd);
    }

    /// This one is locked, exactly like a region whose owner is still running: it must survive.
    const std::string locked = private_directory + "/clickhouse_udf_shm_locked";
    int locked_fd = ::open(locked.c_str(), O_CREAT | O_RDWR, 0600);
    ASSERT_NE(locked_fd, -1);
    SCOPE_EXIT({ ::close(locked_fd); });
    ASSERT_EQ(::flock(locked_fd, LOCK_EX | LOCK_NB), 0);

    /// A file that is not a region must not be touched, locked or not.
    const std::string unrelated = private_directory + "/not_a_region";
    {
        int fd = ::open(unrelated.c_str(), O_CREAT | O_RDWR, 0600);
        ASSERT_NE(fd, -1);
        ::close(fd);
    }

    /// A matching prefix is not sufficient evidence that an entry belongs to this mechanism.
    const std::string unrelated_matching_file = private_directory + "/clickhouse_udf_shm_unrelated";
    {
        int fd = ::open(unrelated_matching_file.c_str(), O_CREAT | O_RDWR, 0644);
        ASSERT_NE(fd, -1);
        ::close(fd);
    }

    const std::string matching_fifo = private_directory + "/clickhouse_udf_shm_fifo";
    ASSERT_EQ(::mkfifo(matching_fifo.c_str(), 0600), 0);

    /// Even a same-user 0600 file with the region prefix is unrelated when it lives directly in
    /// the configured parent directory. The stale sweep must be confined to its private namespace.
    const std::string matching_file_outside_namespace = directory + "/clickhouse_udf_shm_unrelated_0600";
    {
        int fd = ::open(matching_file_outside_namespace.c_str(), O_CREAT | O_RDWR, 0600);
        ASSERT_NE(fd, -1);
        ::close(fd);
    }

    SharedMemoryRegion region(directory, 4096);

    EXPECT_FALSE(std::filesystem::exists(leftover));
    EXPECT_TRUE(std::filesystem::exists(locked));
    EXPECT_TRUE(std::filesystem::exists(unrelated));
    EXPECT_TRUE(std::filesystem::exists(unrelated_matching_file));
    EXPECT_TRUE(std::filesystem::exists(matching_fifo));
    EXPECT_TRUE(std::filesystem::exists(matching_file_outside_namespace));
    EXPECT_TRUE(std::filesystem::exists(region.path()));
    EXPECT_EQ(std::filesystem::path(region.path()).parent_path(), std::filesystem::path(private_directory));

    /// A burst of region creations scans this directory only once. Without throttling, N
    /// non-pooled UDF calls would each scan the N live region files and perform O(N^2) work.
    const std::string later_leftover = private_directory + "/clickhouse_udf_shm_later_leftover";
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
    EXPECT_THROW(SharedMemoryRegion(regionDir(), 0), DB::Exception);
}

TEST(SharedMemoryRegion, UnsupportedDirectoryRejectedDuringConfigurationValidation)
{
    /// `procfs` cannot host an `O_TMPFILE`; the same probe is used while loading UDF configuration.
    EXPECT_THROW(SharedMemoryRegion::checkSupported("/proc"), DB::Exception);
}

TEST(SharedMemoryRegion, SharedDirectoryWithoutStickyBitRejected)
{
    const std::string directory
        = regionDir() + "/clickhouse_shm_unsafe_permissions_" + std::to_string(::getpid()) + "_" + getRandomASCIIString(16);
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
    EXPECT_THROW(SharedMemoryRegion(regionDir(), too_large), DB::Exception);
}

TEST(SharedMemoryRegion, UnlinkOnDestroy)
{
    std::string path;
    {
        SharedMemoryRegion region(regionDir(), 1024);
        path = region.path();
        EXPECT_TRUE(std::filesystem::exists(path));
    }
    EXPECT_FALSE(std::filesystem::exists(path));
}

/// The transport puts the input into the region through the descriptor rather than through the
/// mapping, and takes the output out the same way. Both directions have to be visible to a second,
/// independently opened mapping of the same file - which is what the command has.
TEST(SharedMemoryRegion, WriteAndReadBackingFileAreVisibleToAnotherMapping)
{
    SharedMemoryRegion region(regionDir(), 4096);

    int fd = ::open(region.path().c_str(), O_RDWR);
    ASSERT_NE(fd, -1);
    void * other = ::mmap(nullptr, region.size(), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ASSERT_NE(other, MAP_FAILED);
    ::close(fd);

    auto * other_data = static_cast<char *>(other);

    /// Server -> command, the way `WriteBufferToSharedMemoryRegion` does it.
    const std::string in = "input-from-server";
    region.writeBackingFile(in.data(), 0, in.size());
    EXPECT_EQ(std::string(other_data, in.size()), in);

    /// Command -> server: the command writes through its mapping, the server copies it out.
    const std::string out = "output-from-child";
    memcpy(other_data + 2048, out.data(), out.size());

    std::string read_back(out.size(), '\0');
    region.readBackingFile(read_back.data(), 2048, out.size());
    EXPECT_EQ(read_back, out);

    ::munmap(other, region.size());
}

/// The bound is the region's own size, and it is checked against what the region claims rather than
/// against the file - the file is the command's to change, so asking it would be asking the very
/// thing that cannot be trusted.
TEST(SharedMemoryRegion, WriteBackingFileRejectsWritesPastTheRegion)
{
    SharedMemoryRegion region(regionDir(), 4096);

    const std::string payload(64, 'x');
    EXPECT_THROW(region.writeBackingFile(payload.data(), 4096 - 32, payload.size()), DB::Exception);
    EXPECT_THROW(region.writeBackingFile(payload.data(), 8192, payload.size()), DB::Exception);

    /// And the last byte that does fit is still allowed.
    EXPECT_NO_THROW(region.writeBackingFile(payload.data(), 4096 - payload.size(), payload.size()));
}

/// Why the input goes through the descriptor at all. The command holds the region open for writing
/// and can shorten it at any moment, including between the server's check that the file is whole
/// and the server's own store. Through the mapping that store lands on a page the file no longer
/// backs, and the resulting `SIGBUS` cannot be caught: it would take the whole server down, and
/// this test process with it. A `pwrite` of the same range simply extends the file again.
TEST(SharedMemoryRegion, WriteBackingFileSurvivesTheFileBeingTruncatedUnderIt)
{
    SharedMemoryRegion region(regionDir(), 4096);

    /// What the command can do to the file it was handed - here at the worst possible moment.
    int fd = ::open(region.path().c_str(), O_RDWR);
    ASSERT_NE(fd, -1);
    ASSERT_EQ(::ftruncate(fd, 0), 0);
    ::close(fd);

    const std::string payload = "written-after-the-file-was-truncated";
    EXPECT_NO_THROW(region.writeBackingFile(payload.data(), 0, payload.size()));

    std::string read_back(payload.size(), '\0');
    EXPECT_NO_THROW(region.readBackingFile(read_back.data(), 0, payload.size()));
    EXPECT_EQ(read_back, payload);

    /// The region still knows what it is charged for; only the file changed under it, which is what
    /// `backingFileState` is there to report.
    EXPECT_EQ(region.size(), 4096u);
    EXPECT_EQ(region.backingFileState().size, payload.size());
}

/// What a pooled region is charged for while it sits idle has to be what the `tmpfs` is really
/// holding. The command holds its region open for writing, so the file can be longer than the
/// region believes - by its own doing, or because a `grow` whose rollback failed left it that way -
/// and those extra pages would otherwise be held by nobody and counted by nobody for as long as the
/// worker stayed idle. Putting the file back is what gives them up.
TEST(SharedMemoryRegion, ReconcileGivesBackPagesTheFileGrewBy)
{
    SharedMemoryRegion region(regionDir(), 4096);
    const std::string path = region.path();

    const std::string payload = "kept across the reconcile";
    region.writeBackingFile(payload.data(), 0, payload.size());

    /// What the command can do to the file it was handed.
    int fd = ::open(path.c_str(), O_RDWR);
    ASSERT_NE(fd, -1);
    ASSERT_EQ(::ftruncate(fd, 65536), 0);
    ::close(fd);

    EXPECT_EQ(region.reconcileBackingFileSize(), 4096u);

    struct stat st{};
    ASSERT_EQ(::stat(path.c_str(), &st), 0);
    EXPECT_EQ(static_cast<size_t>(st.st_size), 4096u) << "the pages the file grew by were not given back";

    /// The region itself is untouched, contents included.
    EXPECT_EQ(region.size(), 4096u);
    std::string read_back(payload.size(), '\0');
    region.readBackingFile(read_back.data(), 0, payload.size());
    EXPECT_EQ(read_back, payload);
}

/// The other direction is not this method's business. A file that got *shorter* is a hazard rather
/// than an accounting error - `backingFileState` reports it and the consumer discards the worker
/// over it - and extending it again here would paper over exactly that.
TEST(SharedMemoryRegion, ReconcileLeavesAShortenedFileAlone)
{
    SharedMemoryRegion region(regionDir(), 4096);
    const std::string path = region.path();

    int fd = ::open(path.c_str(), O_RDWR);
    ASSERT_NE(fd, -1);
    ASSERT_EQ(::ftruncate(fd, 1024), 0);
    ::close(fd);

    /// Charged for what it reserved, not for what is left of it.
    EXPECT_EQ(region.reconcileBackingFileSize(), 4096u);

    struct stat st{};
    ASSERT_EQ(::stat(path.c_str(), &st), 0);
    EXPECT_EQ(static_cast<size_t>(st.st_size), 1024u) << "a shortened file must be left for the integrity check to find";
    EXPECT_EQ(region.backingFileState().size, 1024u);
}

/// The ordinary case: nothing has touched the file, so nothing happens to it.
TEST(SharedMemoryRegion, ReconcileIsANoOpOnAnUntouchedRegion)
{
    SharedMemoryRegion region(regionDir(), 4096);

    EXPECT_EQ(region.reconcileBackingFileSize(), 4096u);
    EXPECT_EQ(region.backingFileState().size, 4096u);

    region.grow(8192);
    EXPECT_EQ(region.reconcileBackingFileSize(), 8192u);
    EXPECT_EQ(region.backingFileState().size, 8192u);
}

/// The whole point of MAP_SHARED: a second, independent mapping of the same file (as the child
/// process does) observes writes made through the region, and vice versa.
TEST(SharedMemoryRegion, SharedAcrossMappings)
{
    SharedMemoryRegion region(regionDir(), 4096);

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
    SharedMemoryRegion region(regionDir(), 1024);
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
    SharedMemoryRegion region(regionDir(), 4096);
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
    SharedMemoryRegion region(regionDir(), 8192);
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
    SharedMemoryRegion region(regionDir(), 4096);

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

/// The configuration check is the first thing to touch the region directory, and after an unclean
/// shutdown it is the first thing to find it full of files nobody owns any more. It has to reclaim
/// them itself: it probes the directory by creating a file there, so leftovers that filled the
/// filesystem would fail the probe, the function would never load - and since creating a region is
/// what would have swept them away, nothing else ever would either. One crash would leave the
/// feature unusable until the directory was cleaned by hand.
TEST(SharedMemoryRegion, ConfigurationCheckReclaimsLeftoverRegionFiles)
{
    /// A directory of its own, so this is the first call to reclaim in it: the sweep runs at most
    /// once a minute per directory, and a shared one might have been swept by another test already.
    const std::string directory
        = regionDir() + "/clickhouse_shm_check_leftovers_" + std::to_string(::getpid()) + "_" + getRandomASCIIString(16);
    std::filesystem::create_directories(directory);
    SCOPE_EXIT({ std::filesystem::remove_all(directory); });
    ASSERT_EQ(::chmod(directory.c_str(), 0700), 0);

    const std::string private_directory = directory + "/" + regionDirectoryName();
    std::filesystem::create_directories(private_directory);
    ASSERT_EQ(::chmod(private_directory.c_str(), 0700), 0);

    /// Unlocked and unowned, exactly like a file left behind by a process that is gone.
    const std::string leftover = private_directory + "/clickhouse_udf_shm_leftover";
    {
        int fd = ::open(leftover.c_str(), O_CREAT | O_RDWR, 0600);
        ASSERT_NE(fd, -1);
        ASSERT_EQ(::ftruncate(fd, 4096), 0);
        ::close(fd);
    }

    /// And one that is still owned, which must survive being looked at.
    const std::string locked = private_directory + "/clickhouse_udf_shm_locked";
    int locked_fd = ::open(locked.c_str(), O_CREAT | O_RDWR, 0600);
    ASSERT_NE(locked_fd, -1);
    SCOPE_EXIT({ ::close(locked_fd); });
    ASSERT_EQ(::flock(locked_fd, LOCK_EX | LOCK_NB), 0);

    ASSERT_NO_THROW(SharedMemoryRegion::checkSupported(directory));

    EXPECT_FALSE(std::filesystem::exists(leftover));
    EXPECT_TRUE(std::filesystem::exists(locked));
}

#endif
