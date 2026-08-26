#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobUploadPool.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <mutex>

using namespace DB::Cas;

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

namespace
{

/// Mirrors `gtest_cas_part_manifest_format.cpp`'s inlined assertion helper rather than pulling in
/// `Disks/tests/cas_test_helpers.h` for one tiny check.
template <typename F>
void expectThrowsCode(int expected_code, F && fn)
{
    try
    {
        fn();
        FAIL() << "expected DB::Exception";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), expected_code);
    }
}

/// The pattern stage-1 T5's fan-out fixtures reuse: lazily bring the server-wide pool up on first
/// need. Deliberately NOT torn down between calls -- `initializeBlobUploadPool` is once-only for
/// the lifetime of the binary via this helper, matching how the real server wires it once at
/// startup. Tests that need to exercise the raw init/shutdown lifecycle contract itself (this file)
/// call `initializeBlobUploadPool`/`shutdownBlobUploadPool` directly instead of through this helper.
void ensureBlobUploadPoolForTest(size_t size)
{
    static std::once_flag once;
    std::call_once(once, [size] { initializeBlobUploadPool(size); });
}

}

/// `blobUploadPool()` on an uninitialized pool throws `LOGICAL_ERROR`, which aborts the whole
/// process in debug/sanitizer builds instead of behaving like a catchable exception (see
/// `handle_error_code` in `Common/Exception.cpp`) -- `CASBlobUploadPoolDeathTest` below proves the
/// abort positively in those builds instead, following `gtest_cas_gc_state_format.cpp`'s pattern.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASBlobUploadPool, GetterThrowsBeforeInit)
{
    shutdownBlobUploadPool();
    EXPECT_FALSE(blobUploadPoolInitializedForTest());
    expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [] { blobUploadPool(); });
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASBlobUploadPoolDeathTest, GetterAbortsBeforeInit)
{
    shutdownBlobUploadPool();
    ASSERT_FALSE(blobUploadPoolInitializedForTest());
    EXPECT_DEATH({ (void)blobUploadPool(); }, "");
}
#endif

TEST(CASBlobUploadPool, InitZeroRejected)
{
    shutdownBlobUploadPool();
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [] { initializeBlobUploadPool(0); });
    /// A rejected init must not leave the pool half-initialized.
    EXPECT_FALSE(blobUploadPoolInitializedForTest());
    shutdownBlobUploadPool();
}

TEST(CASBlobUploadPool, InitThenGetWorks)
{
    shutdownBlobUploadPool();
    initializeBlobUploadPool(4);
    EXPECT_TRUE(blobUploadPoolInitializedForTest());

    std::atomic<int> ran{0};
    blobUploadPool().scheduleOrThrowOnError([&ran] { ++ran; });
    blobUploadPool().wait();
    EXPECT_EQ(ran.load(), 1);

    shutdownBlobUploadPool();
}

/// Same debug/sanitizer-abort caveat as `GetterThrowsBeforeInit` above: the second
/// `initializeBlobUploadPool` call throws `LOGICAL_ERROR`, which aborts under
/// `DEBUG_OR_SANITIZER_BUILD`.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASBlobUploadPool, DoubleInitThrows)
{
    shutdownBlobUploadPool();
    initializeBlobUploadPool(2);
    expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [] { initializeBlobUploadPool(2); });
    shutdownBlobUploadPool();
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASBlobUploadPoolDeathTest, DoubleInitAborts)
{
    shutdownBlobUploadPool();
    initializeBlobUploadPool(2);
    EXPECT_DEATH({ (void)initializeBlobUploadPool(2); }, "");
    shutdownBlobUploadPool();
}
#endif

TEST(CASBlobUploadPool, ShutdownIdempotent)
{
    /// Idempotent even when never initialized.
    shutdownBlobUploadPool();
    shutdownBlobUploadPool();
    EXPECT_FALSE(blobUploadPoolInitializedForTest());

    initializeBlobUploadPool(3);
    shutdownBlobUploadPool();
    /// Idempotent after a real init + shutdown too.
    shutdownBlobUploadPool();
    EXPECT_FALSE(blobUploadPoolInitializedForTest());
}

TEST(CASBlobUploadPool, EnsureForTestHelperLazilyInitializes)
{
    shutdownBlobUploadPool();
    EXPECT_FALSE(blobUploadPoolInitializedForTest());

    ensureBlobUploadPoolForTest(4);
    EXPECT_TRUE(blobUploadPoolInitializedForTest());

    /// Idempotent: a pool already up must not throw on a repeated call.
    ensureBlobUploadPoolForTest(4);
    EXPECT_TRUE(blobUploadPoolInitializedForTest());

    shutdownBlobUploadPool();
}
