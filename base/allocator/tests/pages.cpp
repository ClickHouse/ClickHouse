/// Tests of `Pages` (pages.c, extent_mmap.c) and the default extent hooks (ehooks.c): boot-time probes against what
/// the kernel reports, mapping (hint, alignment), purging, commit/decommit under the detected overcommit mode, guards.

#include <allocator/ExtentHooks.h>
#include <allocator/Pages.h>

#include "Test.h"

#include <cstdio>
#include <cstring>
#include <initializer_list>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

using namespace jemalloc;

namespace
{

void bootOnce()
{
    static bool booted = false;
    if (!booted)
    {
        REQUIRE(!pages::boot());
        booted = true;
    }
}

/// The permissions ("rw-p") and the name of the mapping containing `addr` from /proc/self/maps.
bool findMapping(const void * addr, char * permissions, char * name, size_t name_size)
{
    FILE * f = std::fopen("/proc/self/maps", "r");
    if (!f)
        return false;
    char line[1024];
    bool found = false;
    while (std::fgets(line, sizeof(line), f))
    {
        unsigned long begin;
        unsigned long end;
        char p[8];
        int consumed = 0;
        if (std::sscanf(line, "%lx-%lx %7s %*s %*s %*s%n", &begin, &end, p, &consumed) < 3)
            continue;
        uintptr_t a = reinterpret_cast<uintptr_t>(addr);
        if (a >= begin && a < end)
        {
            std::strcpy(permissions, p);
            const char * rest = line + consumed;
            while (*rest == ' ')
                ++rest;
            std::snprintf(name, name_size, "%s", rest);
            size_t len = std::strlen(name);
            if (len && name[len - 1] == '\n')
                name[len - 1] = 0;
            found = true;
            break;
        }
    }
    std::fclose(f);
    return found;
}

char readFirstByte(const char * path)
{
    int fd = ::open(path, O_RDONLY);
    if (fd < 0)
        return 0;
    char c = 0;
    if (::read(fd, &c, 1) != 1)
        c = 0;
    ::close(fd);
    return c;
}

bool allZero(const void * p, size_t size)
{
    const unsigned char * c = static_cast<const unsigned char *>(p);
    for (size_t i = 0; i < size; ++i)
        if (c[i])
            return false;
    return true;
}

}

TEST(Pages, BootProbes)
{
    bootOnce();
    CHECK_EQ(os_page, static_cast<size_t>(sysconf(_SC_PAGESIZE)));
    CHECK_LE(os_page, PAGE);

    char overcommit = readFirstByte("/proc/sys/vm/overcommit_memory");
    bool expected_overcommit = overcommit == '0' || overcommit == '1';
    CHECK_EQ(pages::osOvercommits(), expected_overcommit);
    int expected_flags = MAP_PRIVATE | MAP_ANON | (expected_overcommit ? MAP_NORESERVE : 0);
    CHECK_EQ(pages::mmapFlags(), expected_flags);

    /// Real hardware (not QEMU user mode): MADV_DONTNEED zeroes, MADV_FREE is supported.
    CHECK(!options.trust_madvise);
    CHECK(!pages::madviseDontNeedZerosIsFaulty());
    CHECK(pages::canPurgeLazyRuntime());

    /// THP mode from sysfs: the bracketed word.
    char buf[64] = {};
    int fd = ::open("/sys/kernel/mm/transparent_hugepage/enabled", O_RDONLY);
    if (fd >= 0)
    {
        ssize_t n = ::read(fd, buf, sizeof(buf) - 1);
        ::close(fd);
        CHECK_GT(n, 0);
        if (std::strcmp(buf, "always [madvise] never\n") == 0)
            CHECK_EQ(init_system_transparent_huge_pages_mode, SystemTransparentHugePagesMode::Madvise);
        else if (std::strcmp(buf, "[always] madvise never\n") == 0)
            CHECK_EQ(init_system_transparent_huge_pages_mode, SystemTransparentHugePagesMode::Always);
        else if (std::strcmp(buf, "always madvise [never]\n") == 0)
            CHECK_EQ(init_system_transparent_huge_pages_mode, SystemTransparentHugePagesMode::Never);
        else
            CHECK_EQ(init_system_transparent_huge_pages_mode, SystemTransparentHugePagesMode::NotSupported);
    }
    else
        CHECK_EQ(init_system_transparent_huge_pages_mode, SystemTransparentHugePagesMode::NotSupported);
    if (init_system_transparent_huge_pages_mode != SystemTransparentHugePagesMode::NotSupported)
        CHECK_EQ(options.transparent_huge_pages, TransparentHugePagesMode::DoNothing);
    else
        CHECK_EQ(options.transparent_huge_pages, TransparentHugePagesMode::NotSupported);

    CHECK_STREQ(transparent_huge_pages_mode_names[0], "default");
    CHECK_STREQ(transparent_huge_pages_mode_names[3], "not supported");
    CHECK_STREQ(system_transparent_huge_pages_mode_names[0], "madvise");
    CHECK_STREQ(metadata_transparent_huge_pages_mode_names[2], "always");
    CHECK(options.retain == config::retain);
}

TEST(Pages, MapAlignedAndHint)
{
    bootOnce();
    for (size_t alignment : {PAGE, size_t(2) << 20, size_t(16) << 20})
    {
        for (int i = 0; i < 8; ++i)
        {
            bool commit = true;
            size_t size = PAGE * (1 + i);
            void * p = pages::map(nullptr, size, alignment, &commit);
            REQUIRE(p != nullptr);
            CHECK_EQ(reinterpret_cast<uintptr_t>(p) % alignment, 0u);
            CHECK(commit);
            CHECK(allZero(p, size));
            memset(p, 1, size);

            char permissions[8];
            char name[256];
            REQUIRE(findMapping(p, permissions, name, sizeof(name)));
            CHECK_STREQ(permissions, "rw-p");
            /// With `CONFIG_ANON_VMA_NAME` the mapping is named; otherwise prctl fails with EINVAL and it is not.
            if (name[0])
                CHECK_STREQ(name, pages::osOvercommits() ? "[anon:jemalloc_pg_overcommit]" : "[anon:jemalloc_pg]");
            pages::unmap(p, size);
        }
    }

    /// A hint that is free is honored exactly; a hint that is taken fails without replacing the existing mapping.
    bool commit = true;
    void * p = pages::map(nullptr, 4 * PAGE, PAGE, &commit);
    REQUIRE(p != nullptr);
    memset(p, 7, 4 * PAGE);
    void * q = pages::map(p, 4 * PAGE, PAGE, &commit);
    CHECK(q == nullptr);
    CHECK_EQ(static_cast<unsigned char *>(p)[0], 7u);
    pages::unmap(p, 4 * PAGE);
    q = pages::map(p, 4 * PAGE, PAGE, &commit);
    CHECK(q == p);
    CHECK(allZero(q, 4 * PAGE));
    pages::unmap(q, 4 * PAGE);
}

TEST(Pages, Purge)
{
    bootOnce();
    bool commit = true;
    size_t size = 8 * PAGE;
    char * p = static_cast<char *>(pages::map(nullptr, size, PAGE, &commit));
    REQUIRE(p != nullptr);

    memset(p, 'x', size);
    CHECK(!pages::purgeForced(p + PAGE, 2 * PAGE));
    CHECK(allZero(p + PAGE, 2 * PAGE));
    CHECK_EQ(p[0], 'x');
    CHECK_EQ(p[3 * PAGE], 'x');

    /// `MADV_FREE` succeeds; the content is unspecified afterwards.
    CHECK(!pages::purgeLazy(p, size));

    /// Default hooks: zeroing a range uses MADV_DONTNEED (not memset).
    memset(p, 'y', size);
    extentHooksDefaultZeroImpl(p, size);
    CHECK(allZero(p, size));
    memset(p, 'z', size);
    CHECK(!extentHooksDefaultPurgeForcedImpl(p, PAGE, PAGE));
    CHECK(allZero(p + PAGE, PAGE));
    CHECK_EQ(p[0], 'z');
    CHECK(!extentHooksDefaultPurgeLazyImpl(p, 0, PAGE));

    CHECK(!pages::dontDump(p, size));
    CHECK(!pages::doDump(p, size));
    CHECK(pages::collapse(p, size));

    pages::unmap(p, size);
}

TEST(Pages, CommitDecommit)
{
    bootOnce();
    size_t size = 4 * PAGE;
    bool commit = false;
    char * p = static_cast<char *>(pages::map(nullptr, size, PAGE, &commit));
    REQUIRE(p != nullptr);
    char permissions[8];
    char name[256];
    REQUIRE(findMapping(p, permissions, name, sizeof(name)));

    if (pages::osOvercommits())
    {
        /// Overcommit: memory is always committed, and commit/decommit are errors (no-ops).
        CHECK(commit);
        CHECK_STREQ(permissions, "rw-p");
        memset(p, 1, size);
        CHECK(pages::decommit(p, size));
        CHECK(pages::commit(p, size));
        CHECK_EQ(p[0], 1);
        CHECK(extentHooksDefaultDecommitImpl(p, 0, size));
        CHECK(extentHooksDefaultCommitImpl(p, 0, size));
    }
    else
    {
        CHECK(!commit);
        CHECK_STREQ(permissions, "---p");
        CHECK(!pages::commit(p, size));
        memset(p, 1, size);
        CHECK(!pages::decommit(p, size));
        REQUIRE(findMapping(p, permissions, name, sizeof(name)));
        CHECK_STREQ(permissions, "---p");
        CHECK(!pages::commit(p, size));
        CHECK(allZero(p, size));
    }
    pages::unmap(p, size);
}

TEST(Pages, Guards)
{
    bootOnce();
    size_t size = 6 * PAGE;
    bool commit = true;
    char * p = static_cast<char *>(pages::map(nullptr, size, PAGE, &commit));
    REQUIRE(p != nullptr);
    char permissions[8];
    char name[256];

    char * head = p + PAGE;
    char * tail = p + 4 * PAGE;
    pages::markGuards(head, tail);
    REQUIRE(findMapping(head, permissions, name, sizeof(name)));
    CHECK_STREQ(permissions, "---p");
    REQUIRE(findMapping(tail, permissions, name, sizeof(name)));
    CHECK_STREQ(permissions, "---p");
    REQUIRE(findMapping(p + 2 * PAGE, permissions, name, sizeof(name)));
    CHECK_STREQ(permissions, "rw-p");

    pages::unmarkGuards(head, tail);
    REQUIRE(findMapping(head, permissions, name, sizeof(name)));
    CHECK_STREQ(permissions, "rw-p");
    REQUIRE(findMapping(tail, permissions, name, sizeof(name)));
    CHECK_STREQ(permissions, "rw-p");
    head[0] = 1;
    tail[0] = 1;

    pages::markGuards(nullptr, tail);
    REQUIRE(findMapping(tail, permissions, name, sizeof(name)));
    CHECK_STREQ(permissions, "---p");
    pages::unmarkGuards(nullptr, tail);
    REQUIRE(findMapping(tail, permissions, name, sizeof(name)));
    CHECK_STREQ(permissions, "rw-p");

    pages::unmap(p, size);
}

TEST(Pages, ExtentMmap)
{
    bootOnce();
    bool zero = false;
    bool commit = true;
    size_t size = 4 * PAGE;
    void * p = extentAllocMmap(nullptr, size, 2 << 20, &zero, &commit);
    REQUIRE(p != nullptr);
    CHECK(zero);
    CHECK_EQ(reinterpret_cast<uintptr_t>(p) % (2 << 20), 0u);

    /// With retain (Linux), dalloc fails and the memory stays mapped.
    CHECK_EQ(extentDeallocateMmap(p, size), options.retain);
    if (options.retain)
    {
        static_cast<char *>(p)[0] = 1;
        pages::unmap(p, size);
    }

    /// Without commit (and without overcommit), the memory is not zeroed by contract.
    zero = false;
    commit = false;
    p = extentAllocMmap(nullptr, size, PAGE, &zero, &commit);
    REQUIRE(p != nullptr);
    CHECK_EQ(zero, commit);
    CHECK_EQ(commit, pages::osOvercommits());
    pages::unmap(p, size);
}

TEST(ExtentHooks, Default)
{
    bootOnce();
    ExtentHooks extent_hooks;
    extent_hooks.init(const_cast<extent_hooks_t *>(&extent_hooks_default_extent_hooks), 7);
    CHECK_EQ(extent_hooks.idxGet(), 7u);
    CHECK(extent_hooks.areDefault());
    CHECK_EQ(extent_hooks.deallocateWillFail(), options.retain);
    CHECK(!extent_hooks.splitWillFail());
    CHECK(!extent_hooks.mergeWillFail());
    CHECK(!extent_hooks.guardWillFail());

    bool zero = false;
    bool commit = true;
    char * p = static_cast<char *>(extent_hooks.alloc(nullptr, nullptr, 8 * PAGE, PAGE, &zero, &commit));
    REQUIRE(p != nullptr);
    CHECK(zero);
    CHECK(!extent_hooks.split(nullptr, p, 8 * PAGE, 4 * PAGE, 4 * PAGE, true));
    CHECK(!extent_hooks.merge(nullptr, p, 4 * PAGE, p + 4 * PAGE, 4 * PAGE, true));
    memset(p, 3, 8 * PAGE);
    extent_hooks.zero(nullptr, p, 8 * PAGE);
    CHECK(allZero(p, 8 * PAGE));
    CHECK(!extent_hooks.guard(nullptr, p, p + 7 * PAGE));
    CHECK(!extent_hooks.unguard(nullptr, p, p + 7 * PAGE));
    CHECK_EQ(extent_hooks.deallocate(nullptr, p, 8 * PAGE, true), options.retain);
    if (options.retain)
        extent_hooks.destroy(nullptr, p, 8 * PAGE, true);

    /// The public table.
    const extent_hooks_t & table = extent_hooks_default_extent_hooks;
    REQUIRE(
        table.alloc && table.dalloc && table.destroy && table.commit && table.decommit && table.purge_lazy && table.purge_forced
        && table.split && table.merge);
    extent_hooks_t * h = const_cast<extent_hooks_t *>(&table);
    zero = false;
    commit = true;
    /// The alignment is rounded up to PAGE.
    p = static_cast<char *>(table.alloc(h, nullptr, 2 * PAGE, 1, &zero, &commit, 0));
    REQUIRE(p != nullptr);
    CHECK_EQ(reinterpret_cast<uintptr_t>(p) % PAGE, 0u);
    CHECK(!table.purge_forced(h, p, 2 * PAGE, 0, PAGE, 0));
    CHECK(!table.purge_lazy(h, p, 2 * PAGE, 0, PAGE, 0));
    CHECK(!table.split(h, p, 2 * PAGE, PAGE, PAGE, true, 0));
    CHECK(!table.merge(h, p, PAGE, p + PAGE, PAGE, true, 0));
    CHECK_EQ(table.dalloc(h, p, 2 * PAGE, true, 0), options.retain);
    if (options.retain)
        table.destroy(h, p, 2 * PAGE, true, 0);

    CHECK_STREQ(sbrk_precedence_names[2], "secondary");
}
