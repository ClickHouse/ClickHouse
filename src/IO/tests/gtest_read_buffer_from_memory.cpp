#include <gtest/gtest.h>

#include <IO/ReadBufferFromMemory.h>

#include <string_view>

using namespace DB;

/// An empty file materialized into an OWNED in-memory buffer must construct without undefined
/// behaviour: std::memcpy's pointer arguments are __attribute__((nonnull)), so memcpy(dst, nullptr, 0)
/// -- which an empty std::string_view (data() == nullptr) produces -- is UB that the asan_ubsan lane
/// aborts on (STID 5930-5afa, PR #2073). The buffer must construct and be immediately at EOF.
TEST(ReadBufferFromMemoryFileBase, EmptyOwnedBufferConstructsWithoutUB)
{
    /// ReadBufferFromMemoryFileBase's constructor is protected; ReadBufferFromOwnMemoryFile is the
    /// public concrete class that always passes owns_memory=true, exercising the guarded memcpy path.
    ReadBufferFromOwnMemoryFile buf("empty", std::string_view{});
    EXPECT_TRUE(buf.eof());
}
