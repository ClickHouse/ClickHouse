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

#include <linux/falloc.h>

/// The sealing constants of the kernel ABI, for a libc whose headers predate them (the same
/// fallback as in SharedMemoryRegion.cpp, which explains it).
#if !defined(F_ADD_SEALS)
#    define F_ADD_SEALS 1033
#endif
#if !defined(F_GET_SEALS)
#    define F_GET_SEALS 1034
#endif
#if !defined(F_SEAL_SEAL)
#    define F_SEAL_SEAL 0x0001
#endif
#if !defined(F_SEAL_SHRINK)
#    define F_SEAL_SHRINK 0x0002
#endif
#if !defined(F_SEAL_GROW)
#    define F_SEAL_GROW 0x0004
#endif
#if !defined(F_SEAL_WRITE)
#    define F_SEAL_WRITE 0x0008
#endif

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
    /// In pages, not in bytes: this test counts the pages the file holds, and what a page is here
    /// is the unit the kernel backs the region in - 4 KiB, 64 KiB on some kernels, or a transparent
    /// huge page where `shmem` is backed with those. A region of a page and a file of sixteen
    /// written as 4096 and 65536 would be one page and one page on a 64 KiB kernel, and the "the
    /// tail is sparse" check below would have nothing left to be true about.
    const size_t page = SharedMemoryRegion::roundUpToPages(1);
    const size_t grown = 16 * page;

    SharedMemoryRegion region(page);

    /// What a command could do through its inherited descriptor: `ftruncate` up is not sealed.
    ASSERT_EQ(::ftruncate(region.fd(), static_cast<off_t>(grown)), 0);

    /// The cached figure does not know; the re-read does. The mapping is untouched either way.
    EXPECT_EQ(region.backingSize(), page);
    EXPECT_EQ(region.refreshBackingSize(), grown);
    EXPECT_EQ(region.backingSize(), grown);
    EXPECT_EQ(region.size(), page);

    /// The tail the command added is sparse ...
    struct stat st{};
    ASSERT_EQ(::fstat(region.fd(), &st), 0);
    EXPECT_LT(static_cast<size_t>(st.st_blocks) * 512, grown);

    /// ... and a growth into it commits it before mapping it, even though the file is already
    /// long enough.
    region.grow(grown);
    EXPECT_EQ(region.size(), grown);
    ASSERT_EQ(::fstat(region.fd(), &st), 0);
    EXPECT_GE(static_cast<size_t>(st.st_blocks) * 512, grown);
    memset(region.data() + grown - 100, 'x', 100);
    EXPECT_EQ(std::string(region.data() + grown - 100, 3), "xxx");
}

/// The seal stops the command from making the file shorter; it does not stop it from freeing pages
/// inside it. The region survives that: a punched page reads as zeros and takes a write like any
/// other - an allocation, not a `SIGBUS`. What is lost is the reservation, and that is all: this
/// test pins the boundary of what the seal promises, not a repair (see the class comment for why
/// there is none).
TEST(SharedMemoryRegion, HolePunchedByTheCommandIsNotASigbus)
{
    /// Whole pages of whatever size this kernel backs the region in - a hole of less than a page,
    /// or one that is not aligned to a page, frees nothing and would leave this test proving
    /// nothing.
    const size_t page = SharedMemoryRegion::roundUpToPages(1);
    const size_t size = 16 * page;
    SharedMemoryRegion region(size);
    memset(region.data(), 'x', size);

    /// What a command could do through its inherited descriptor.
    ASSERT_EQ(
        ::fallocate(
            region.fd(),
            FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
            static_cast<off_t>(page),
            static_cast<off_t>(8 * page)),
        0);

    struct stat st{};
    ASSERT_EQ(::fstat(region.fd(), &st), 0);
    EXPECT_EQ(static_cast<size_t>(st.st_size), size);
    EXPECT_LT(static_cast<size_t>(st.st_blocks) * 512, size);

    /// The mapping is intact: the hole reads as zeros, the rest as it was, and a write into the
    /// hole is an ordinary page allocation.
    EXPECT_EQ(region.data()[0], 'x');
    EXPECT_EQ(region.data()[page], '\0');
    EXPECT_EQ(region.data()[9 * page], 'x');
    region.data()[page] = 'y';
    EXPECT_EQ(region.data()[page], 'y');
    EXPECT_EQ(region.refreshBackingSize(), size);
}

/// The seals stop the file getting shorter, not pages being committed past its end: `fallocate`
/// with `FALLOC_FL_KEEP_SIZE` beyond EOF allocates pages that the length never shows. The
/// footprint is what a cap or a charge has to go by, and it sees them.
TEST(SharedMemoryRegion, PagesCommittedPastTheEndOfTheFileShowInTheFootprintButNotInTheLength)
{
    /// Whole pages, so that the footprint of the file is its length: a size that is not a multiple
    /// of what the kernel backs the region in is rounded up in the footprint and nowhere else.
    const size_t size = 16 * SharedMemoryRegion::roundUpToPages(1);
    SharedMemoryRegion region(size);
    EXPECT_EQ(region.refreshFootprint(), size);

    /// What a command could do through its inherited descriptor.
    ASSERT_EQ(::fallocate(region.fd(), FALLOC_FL_KEEP_SIZE, static_cast<off_t>(size), static_cast<off_t>(2 * size)), 0);

    EXPECT_EQ(region.refreshBackingSize(), size);
    EXPECT_EQ(region.size(), size);
    EXPECT_GE(region.refreshFootprint(), 3 * size);
    EXPECT_EQ(region.footprint(), region.refreshFootprint());

    /// And the other way round: a sparse tail is length without pages, and the footprint is the
    /// length then.
    ASSERT_EQ(::ftruncate(region.fd(), static_cast<off_t>(8 * size)), 0);
    EXPECT_EQ(region.refreshBackingSize(), 8 * size);
    EXPECT_EQ(region.refreshFootprint(), 8 * size);

    /// A growth into pages that are already there adds nothing to the footprint.
    region.grow(4 * size);
    EXPECT_EQ(region.footprint(), 8 * size);
}

/// The footprint says how many pages the file holds, not where: a file the command stretched to
/// the cap without committing a page, plus a cap's worth of pages it committed past the end, has
/// the footprint of one cap and would have two once the server commits the file up to its length
/// - which mapping it whole does. What the server itself has committed (`reservedSize`) is what
/// the cost of that fill is bounded from, and the cap check counts that cost in.
TEST(SharedMemoryRegion, SparseLengthAndPagesPastTheEndAreCountedTogetherAgainstTheCap)
{
    const size_t page = SharedMemoryRegion::roundUpToPages(1);
    const size_t cap = 4 * page;
    SharedMemoryRegion region(page);
    EXPECT_EQ(region.reservedSize(), page);
    EXPECT_FALSE(region.isOverTheCap(cap));

    /// The command stretches the file to the cap - length without pages - and commits as many
    /// pages past the end.
    ASSERT_EQ(::ftruncate(region.fd(), cap), 0);
    ASSERT_EQ(::fallocate(region.fd(), FALLOC_FL_KEEP_SIZE, cap, cap), 0);

    /// By the length and by the pages alone the file is at the cap, not over it.
    EXPECT_EQ(region.refreshFootprint(), cap + page);
    EXPECT_EQ(region.backingSize(), cap);
    EXPECT_EQ(region.reservedSize(), page);
    /// But committing it up to its length would add three pages, and that is over the cap.
    EXPECT_EQ(region.fillCostUpTo(cap), cap - page);
    EXPECT_TRUE(region.isOverTheCap(cap));
    EXPECT_FALSE(region.isOverTheCap(2 * cap));

    /// And that is what the fill does: the pages past the end stay, and the sparse ones are
    /// committed on top of them.
    region.grow(cap);
    EXPECT_EQ(region.reservedSize(), cap);
    EXPECT_EQ(region.refreshFootprint(), 2 * cap);
    EXPECT_EQ(region.fillCostUpTo(cap), 0u);
}

/// A file holds whole pages, so a region of a few bytes has the footprint of a page - and that is
/// what it is compared with, so that its own size, rounded the same way, is not a cap it is over.
TEST(SharedMemoryRegion, FootprintIsInWholePages)
{
    SharedMemoryRegion region(16);
    const size_t page = SharedMemoryRegion::roundUpToPages(1);
    EXPECT_GE(page, 4096u);
    EXPECT_EQ(region.footprint(), page);
    EXPECT_EQ(region.refreshFootprint(), page);
    EXPECT_EQ(region.refreshBackingSize(), 16u);
    EXPECT_EQ(SharedMemoryRegion::roundUpToPages(16), page);
    EXPECT_EQ(SharedMemoryRegion::roundUpToPages(page), page);
    EXPECT_EQ(SharedMemoryRegion::roundUpToPages(page + 1), 2 * page);
    EXPECT_EQ(SharedMemoryRegion::roundUpToPages(0), 0u);
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
