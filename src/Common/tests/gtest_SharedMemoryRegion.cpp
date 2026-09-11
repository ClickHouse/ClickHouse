#include <Common/SharedMemoryRegion.h>
#include <Common/Exception.h>
#include <base/defines.h>

#include <condition_variable>
#include <cstring>
#include <limits>
#include <mutex>
#include <string>
#include <thread>

#include <csignal>
#include <fstream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>

#include <gtest/gtest.h>

using namespace DB;

/// SharedMemoryRegion relies on a sealed `memfd` and is Linux-only: its constructor throws on
/// other platforms (see `SharedMemoryRegion::checkSupported`). Gate the whole suite so it neither
/// fails nor pins resources on non-Linux builds of unit_tests_dbms.
#if defined(OS_LINUX)

namespace
{

/// A second mapping of the region, made the way the command makes it: from the descriptor, of the
/// whole file. Reading `st_size` first is also how the command learns how large the file is.
struct OtherMapping
{
    char * data = nullptr;
    size_t size = 0;

    explicit OtherMapping(int fd)
    {
        struct stat st{};
        if (0 != ::fstat(fd, &st))
            throw std::runtime_error("fstat failed");
        size = static_cast<size_t>(st.st_size);
        void * buf = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (buf == MAP_FAILED)
            throw std::runtime_error("mmap failed");
        data = static_cast<char *>(buf);
    }

    ~OtherMapping()
    {
        ::munmap(data, size);
    }
};

}

TEST(SharedMemoryRegion, CreateReadWrite)
{
    SharedMemoryRegion region(4096);

    EXPECT_EQ(region.size(), 4096u);
    EXPECT_NE(region.data(), nullptr);
    EXPECT_NE(region.fd(), -1);

    const std::string payload = "hello shared memory";
    memcpy(region.data(), payload.data(), payload.size());
    EXPECT_EQ(std::string(region.data(), payload.size()), payload);
}

TEST(SharedMemoryRegion, SupportProbePasses)
{
    EXPECT_NO_THROW(SharedMemoryRegion::checkSupported());
}

/// The descriptor must be close-on-exec: otherwise a concurrent fork+exec on another thread would
/// carry it - and the mapped UDF data behind it - into an unrelated child. The one child that is
/// meant to have it gets an explicit `dup2` copy, which is what clears the flag.
TEST(SharedMemoryRegion, DescriptorIsCloseOnExec)
{
    SharedMemoryRegion region(4096);

    const int flags = ::fcntl(region.fd(), F_GETFD);
    ASSERT_NE(flags, -1);
    EXPECT_TRUE(flags & FD_CLOEXEC);
}

/// The seals are the whole safety argument of the class: the command holds a writable descriptor,
/// and the only thing standing between it and a `SIGBUS` in the server is that the kernel refuses
/// to let it shrink the file. Check that shrinking is refused, that the seal set cannot be
/// changed, and that growing - which the server needs - is still allowed.
TEST(SharedMemoryRegion, IsSealedAgainstShrinkingButNotGrowth)
{
    SharedMemoryRegion region(8192);

    const int seals = ::fcntl(region.fd(), F_GET_SEALS);
    ASSERT_NE(seals, -1);
    EXPECT_TRUE(seals & F_SEAL_SHRINK);
    EXPECT_TRUE(seals & F_SEAL_SEAL);
    EXPECT_FALSE(seals & F_SEAL_GROW);
    EXPECT_FALSE(seals & F_SEAL_WRITE);

    /// What a hostile or buggy command could try through its inherited descriptor.
    EXPECT_EQ(::ftruncate(region.fd(), 4096), -1);
    EXPECT_EQ(errno, EPERM);
    EXPECT_EQ(::fcntl(region.fd(), F_ADD_SEALS, F_SEAL_GROW), -1);
    EXPECT_EQ(errno, EPERM);
    EXPECT_EQ(::fcntl(region.fd(), F_ADD_SEALS, F_SEAL_WRITE), -1);
    EXPECT_EQ(errno, EPERM);

    struct stat st{};
    ASSERT_EQ(::fstat(region.fd(), &st), 0);
    EXPECT_EQ(static_cast<size_t>(st.st_size), 8192u);

    /// Still readable and writable through the mapping after the refused attempts.
    memcpy(region.data() + 8000, "tail", 4);
    EXPECT_EQ(std::string(region.data() + 8000, 4), "tail");

    region.grow(16384);
    EXPECT_EQ(region.size(), 16384u);
    ASSERT_EQ(::fstat(region.fd(), &st), 0);
    EXPECT_EQ(static_cast<size_t>(st.st_size), 16384u);
}

/// The pages are committed up front, so the region is a fully backed file from the start: the
/// server writes into it without page faults that could fail later, and the command sees the
/// same size as the server.
TEST(SharedMemoryRegion, PagesAreCommittedAtCreation)
{
    SharedMemoryRegion region(1 << 20);

    struct stat st{};
    ASSERT_EQ(::fstat(region.fd(), &st), 0);
    EXPECT_EQ(static_cast<size_t>(st.st_size), static_cast<size_t>(1 << 20));
    EXPECT_GE(static_cast<size_t>(st.st_blocks) * 512, static_cast<size_t>(1 << 20));
}

TEST(SharedMemoryRegion, SizeZeroThrows)
{
    EXPECT_THROW(SharedMemoryRegion region(0), DB::Exception);
}

TEST(SharedMemoryRegion, OversizedThrows)
{
    const size_t oversized = std::numeric_limits<size_t>::max();
    EXPECT_THROW(SharedMemoryRegion region(oversized), DB::Exception);
}

/// The descriptor is the only handle: once the region is destroyed there is nothing left of it.
TEST(SharedMemoryRegion, ClosesDescriptorOnDestroy)
{
    int fd = -1;
    {
        SharedMemoryRegion region(4096);
        fd = region.fd();
        EXPECT_NE(::fcntl(fd, F_GETFD), -1);
    }
    EXPECT_EQ(::fcntl(fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
}

TEST(SharedMemoryRegion, PathForChildFd)
{
    EXPECT_EQ(SharedMemoryRegion::pathForChildFd(3), "/proc/self/fd/3");
    EXPECT_EQ(SharedMemoryRegion::pathForChildFd(4), "/proc/self/fd/4");
}

/// The region can be opened again through `/proc/self/fd/N` by the process that holds the
/// descriptor - which is exactly what the command does with the path it receives.
TEST(SharedMemoryRegion, OpenableThroughProcSelfFd)
{
    SharedMemoryRegion region(4096);

    const std::string payload = "through-proc";
    memcpy(region.data(), payload.data(), payload.size());

    int fd = ::open(SharedMemoryRegion::pathForChildFd(region.fd()).c_str(), O_RDWR);
    ASSERT_NE(fd, -1);
    {
        OtherMapping other(fd);
        EXPECT_EQ(other.size, 4096u);
        EXPECT_EQ(std::string(other.data, payload.size()), payload);
    }
    ::close(fd);
}

/// Both directions of the protocol go through two mappings of the same file: the server writes
/// input, the command writes output where the server reads it in place.
TEST(SharedMemoryRegion, SharedAcrossMappings)
{
    SharedMemoryRegion region(4096);
    OtherMapping other(region.fd());

    /// Server -> child direction.
    const std::string in = "input-from-server";
    memcpy(region.data(), in.data(), in.size());
    EXPECT_EQ(std::string(other.data, in.size()), in);

    /// Child -> server direction (written after the input, as the protocol does).
    const std::string out = "output-from-child";
    memcpy(other.data + 2048, out.data(), out.size());
    EXPECT_EQ(std::string(region.data() + 2048, out.size()), out);
}

/// Growing the region enlarges the file, preserves the previously written bytes, keeps the same
/// descriptor, and makes the larger size visible to a fresh mapping made from that descriptor
/// (as the command makes one on its next request).
TEST(SharedMemoryRegion, GrowPreservesDataAndEnlargesFile)
{
    SharedMemoryRegion region(1024);
    const int fd = region.fd();

    const std::string payload = "payload-before-growth";
    memcpy(region.data(), payload.data(), payload.size());

    region.grow(8192);
    EXPECT_EQ(region.size(), 8192u);
    EXPECT_EQ(region.backingSize(), 8192u);
    EXPECT_EQ(region.fd(), fd);
    EXPECT_EQ(std::string(region.data(), payload.size()), payload);

    OtherMapping other(fd);
    EXPECT_EQ(other.size, 8192u);
    EXPECT_EQ(std::string(other.data, payload.size()), payload);
}

TEST(SharedMemoryRegion, GrowToSmallerOrEqualThrows)
{
    SharedMemoryRegion region(4096);
    EXPECT_THROW(region.grow(4096), DB::Exception);
    EXPECT_THROW(region.grow(1024), DB::Exception);
    EXPECT_EQ(region.size(), 4096u);
}

/// A growth that cannot be backed must leave the region exactly as it was: same size, same
/// mapping, same contents. `posix_fallocate` undoes itself on failure, and the mapping is only
/// replaced once the storage is there.
///
/// The failure is provoked with `RLIMIT_FSIZE`, which the kernel checks before it commits a single
/// page (`EFBIG`). An impossibly large size would not do: the internal mount behind a `memfd` has
/// no size limit, so the kernel would keep allocating pages until the machine ran out of them.
/// The kernel also raises `SIGXFSZ` along with `EFBIG`, whose default action ends the process, so
/// it is ignored for the duration.
TEST(SharedMemoryRegion, FailedGrowLeavesRegionIntact)
{
    SharedMemoryRegion region(4096);
    char * data_before = region.data();
    const std::string payload = "survives-a-failed-growth";
    memcpy(region.data(), payload.data(), payload.size());

    /// Capture the enumerator outside of any macro: glibc defines `RLIMIT_FSIZE` as a
    /// self-referential macro, which trips -Wdisabled-macro-expansion when re-scanned inside
    /// gtest's macros.
    const auto fsize_resource = RLIMIT_FSIZE;

    struct rlimit old_limit{};
    ASSERT_EQ(0, ::getrlimit(fsize_resource, &old_limit));
    auto old_sigxfsz = std::signal(SIGXFSZ, SIG_IGN);

    /// Restore the process-wide state on every exit path, including a fatal ASSERT_* failure, which
    /// returns from the function rather than throwing.
    struct StateGuard
    {
        decltype(fsize_resource) resource;
        struct rlimit limit;
        decltype(old_sigxfsz) handler;

        ~StateGuard()
        {
            EXPECT_EQ(0, ::setrlimit(resource, &limit));
            (void)std::signal(SIGXFSZ, handler);
        }
    } guard{fsize_resource, old_limit, old_sigxfsz};

    struct rlimit limit = old_limit;
    limit.rlim_cur = 8192;
    ASSERT_EQ(0, ::setrlimit(fsize_resource, &limit));

    EXPECT_THROW(region.grow(16384), DB::Exception);

    EXPECT_EQ(region.size(), 4096u);
    EXPECT_EQ(region.backingSize(), 4096u);
    EXPECT_EQ(region.data(), data_before);
    EXPECT_EQ(std::string(region.data(), payload.size()), payload);

    struct stat st{};
    ASSERT_EQ(::fstat(region.fd(), &st), 0);
    EXPECT_EQ(static_cast<size_t>(st.st_size), 4096u);
}

/// The other half of a failed growth: the pages were committed, and then the replacement mapping
/// could not be made. The file is sealed against shrinking, so those pages cannot be given back;
/// the region must report them as its cost (`backingSize`) while keeping the mapping - `size`,
/// `data`, the bytes - exactly as it was. The next growth to that size then only maps.
///
/// The mapping is refused through `RLIMIT_AS`: the address-space limit stops `mmap` but not
/// `posix_fallocate`, which allocates file pages and no address space. Not under the sanitizers,
/// which keep the address space to themselves and fail in their own ways under such a limit.
#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER) && !defined(MEMORY_SANITIZER)
TEST(SharedMemoryRegion, GrowThatCommitsButCannotMapKeepsMappingAndReportsBackingSize)
{
    SharedMemoryRegion region(4096);
    char * data_before = region.data();
    const std::string payload = "survives-a-failed-remap";
    memcpy(region.data(), payload.data(), payload.size());

    const auto as_resource = RLIMIT_AS;

    struct rlimit old_limit{};
    ASSERT_EQ(0, ::getrlimit(as_resource, &old_limit));

    struct StateGuard
    {
        decltype(as_resource) resource;
        struct rlimit limit;

        ~StateGuard()
        {
            EXPECT_EQ(0, ::setrlimit(resource, &limit));
        }
    } guard{as_resource, old_limit};

    /// Cap the address space just above what the process uses now, so that a mapping of many
    /// megabytes is refused while everything already mapped stays valid. The `posix_fallocate`
    /// before it is not subject to the limit: it allocates file pages, not address space.
    size_t vm_size_pages = 0;
    {
        std::ifstream statm("/proc/self/statm");
        ASSERT_TRUE(statm >> vm_size_pages);
    }
    /// Small enough that reserving it cannot fail on a constrained machine, large enough that a
    /// mapping of it is refused under the limit set below, which leaves a fraction of it free.
    const size_t page_size = static_cast<size_t>(::sysconf(_SC_PAGESIZE));
    const size_t new_size = 16 << 20;

    struct rlimit limit = old_limit;
    limit.rlim_cur = vm_size_pages * page_size + (new_size / 4);
    ASSERT_EQ(0, ::setrlimit(as_resource, &limit));

    EXPECT_THROW(region.grow(new_size), DB::Exception);

    /// The mapping is untouched ...
    EXPECT_EQ(region.size(), 4096u);
    EXPECT_EQ(region.data(), data_before);
    EXPECT_EQ(std::string(region.data(), payload.size()), payload);

    /// ... but the file is not, and the region says so.
    EXPECT_EQ(region.backingSize(), new_size);
    struct stat st{};
    ASSERT_EQ(::fstat(region.fd(), &st), 0);
    EXPECT_EQ(static_cast<size_t>(st.st_size), new_size);

    /// With the limit lifted, growing to the committed size maps without committing anything more.
    ASSERT_EQ(0, ::setrlimit(as_resource, &old_limit));
    region.grow(new_size);
    EXPECT_EQ(region.size(), new_size);
    EXPECT_EQ(region.backingSize(), new_size);
    EXPECT_EQ(std::string(region.data(), payload.size()), payload);
}
#endif

/// The seals stop the command from shrinking the file, not from extending it. Pages it adds that
/// way are not the server's doing, but they are the server's cost, so the region reports them once
/// asked to look (`refreshBackingSize`) - and a later growth of the server's own must commit the
/// tail the command left sparse before mapping it, or writing into it could fail under memory
/// pressure with the one signal the whole design exists to avoid.
TEST(SharedMemoryRegion, CommandExtendingTheFileIsSeenAndItsTailIsCommittedOnGrowth)
{
    SharedMemoryRegion region(4096);

    /// What a command could do through its inherited descriptor: `ftruncate` up is not sealed.
    ASSERT_EQ(::ftruncate(region.fd(), 65536), 0);

    /// The cached figure does not know; the re-read does. The mapping is untouched either way.
    EXPECT_EQ(region.backingSize(), 4096u);
    EXPECT_EQ(region.refreshBackingSize(), 65536u);
    EXPECT_EQ(region.backingSize(), 65536u);
    EXPECT_EQ(region.size(), 4096u);

    /// The tail the command added is sparse ...
    struct stat st{};
    ASSERT_EQ(::fstat(region.fd(), &st), 0);
    EXPECT_LT(static_cast<size_t>(st.st_blocks) * 512, 65536u);

    /// ... and a growth into it commits it before mapping it, even though the file is already
    /// long enough.
    region.grow(65536);
    EXPECT_EQ(region.size(), 65536u);
    ASSERT_EQ(::fstat(region.fd(), &st), 0);
    EXPECT_GE(static_cast<size_t>(st.st_blocks) * 512, 65536u);
    memset(region.data() + 60000, 'x', 100);
    EXPECT_EQ(std::string(region.data() + 60000, 3), "xxx");
}

TEST(SharedMemoryRegion, SynchronizedHandoff)
{
    SharedMemoryRegion region(4096);

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
