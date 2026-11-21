#include <gtest/gtest.h>

#include <Common/HashTable/HashTableAllocator.h>
#include <base/getPageSize.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>

/// `CLICKHOUSE_HASH_TABLE_HUGE_PAGES=1` makes `HashTableAllocator` map the buffers of at least one huge page directly
/// with `mmap`, bypassing jemalloc. The variable is read once per process, so the lifecycle of these mappings
/// is checked in a fresh process.

namespace
{

/// The transparent huge page size if the kernel supports them and `Allocator` would use them, otherwise 0.
/// Mirrors the conditions of `readHugePageSizeForHashTables`.
size_t transparentHugePageSize()
{
    std::string enabled;
    if (!std::getline(std::ifstream("/sys/kernel/mm/transparent_hugepage/enabled"), enabled) || enabled.contains("[never]"))
        return 0;
    size_t size = 0;
    if (!(std::ifstream("/sys/kernel/mm/transparent_hugepage/hpage_pmd_size") >> size))
        return 0;
    if (size < 2 * 1024 * 1024 || size > 32 * 1024 * 1024 || (size & (size - 1)) != 0)
        return 0;
    return size;
}

/// A failed gtest assertion in the child would not reach the parent, which sees only the exit code.
void require(bool condition, const std::string & what)
{
    if (!condition)
    {
        std::cerr << "failed: " << what << '\n';
        std::_Exit(1);
    }
}

size_t roundUpToPageSize(size_t size)
{
    const size_t page_size = static_cast<size_t>(::getPageSize());
    return (size + page_size - 1) / page_size * page_size;
}

UInt8 pattern(size_t i)
{
    return static_cast<UInt8>(i * 131 + 7) | 1;
}

void fill(void * buf, size_t size)
{
    auto * bytes = static_cast<UInt8 *>(buf);
    for (size_t i = 0; i < size; ++i)
        bytes[i] = pattern(i);
}

bool isFilled(const void * buf, size_t size)
{
    const auto * bytes = static_cast<const UInt8 *>(buf);
    for (size_t i = 0; i < size; ++i)
        if (bytes[i] != pattern(i))
            return false;
    return true;
}

bool isZero(const void * buf, size_t begin, size_t end)
{
    const auto * bytes = static_cast<const UInt8 *>(buf);
    for (size_t i = begin; i < end; ++i)
        if (bytes[i] != 0)
            return false;
    return true;
}

[[noreturn]] void checkDirectlyMappedBuffers(size_t huge_page_size)
{
    setenv("CLICKHOUSE_HASH_TABLE_HUGE_PAGES", "1", 1); // NOLINT(concurrency-mt-unsafe)

    HashTableAllocator allocator;
    const size_t initial = getAllocatorDirectlyMappedBytes();
    auto mapped = [&] { return getAllocatorDirectlyMappedBytes() - initial; };

    /// Not a multiple of the page size, to check the rounding of the mapping length.
    const size_t size1 = 2 * huge_page_size + 100;
    void * buf = allocator.alloc(size1);
    require(mapped() == roundUpToPageSize(size1), "a buffer of a few huge pages is mapped directly");
    require(reinterpret_cast<uintptr_t>(buf) % huge_page_size == 0, "the mapping is aligned to the huge page size");
    require(isZero(buf, 0, size1), "a new mapping is zero-filled");
    fill(buf, size1);

    /// Growth from a mapping to a mapping.
    const size_t size2 = 4 * huge_page_size;
    buf = allocator.realloc(buf, size1, size2);
    require(mapped() == size2, "the grown buffer replaces the old mapping");
    require(reinterpret_cast<uintptr_t>(buf) % huge_page_size == 0, "the grown mapping is aligned");
    require(isFilled(buf, size1), "growth preserves the bytes");
    require(isZero(buf, size1, size2), "growth zero-fills the extension");

    /// Shrinkage below the threshold moves the buffer to jemalloc.
    const size_t size3 = huge_page_size / 4;
    buf = allocator.realloc(buf, size2, size3);
    require(mapped() == 0, "a small buffer is not mapped directly");
    require(isFilled(buf, size3), "shrinkage preserves the bytes");

    /// Growth from jemalloc back to a mapping.
    const size_t size4 = 3 * huge_page_size;
    buf = allocator.realloc(buf, size3, size4);
    require(mapped() == size4, "a buffer that grows past the threshold is mapped directly");
    require(isFilled(buf, size3), "growth past the threshold preserves the bytes");
    require(isZero(buf, size3, size4), "growth past the threshold zero-fills the extension");

    allocator.free(buf, size4);
    require(getAllocatorDirectlyMappedBytes() == initial, "free releases the mapping");

    std::_Exit(0);
}

}

TEST(AllocatorHugePagesDeathTest, DirectlyMappedBufferLifecycle)
{
    const size_t huge_page_size = transparentHugePageSize();
    if (!huge_page_size)
        GTEST_SKIP() << "transparent huge pages are not available";

    /// The `threadsafe` style re-executes the test binary, so the variable is read for the first time in the child.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EXIT(checkDirectlyMappedBuffers(huge_page_size), ::testing::ExitedWithCode(0), ".*");
}
