#include <gtest/gtest.h>
#include <Disks/tests/cas_test_helpers.h>

/// Stage-1 §1: `ContentAddressedTransaction::uploadPendingBlobs` fans out on the server-wide blob
/// upload pool, whose getter is fail-loud (throws `LOGICAL_ERROR` -- an ABORT under a sanitizer build --
/// if the pool was never initialized). Any CA test that commits a transaction with a pending blob would
/// therefore abort the whole `unit_tests_dbms` process if the pool happened to be down.
///
/// This listener brings the pool up before EVERY test, so the pool is always initialized at the start of
/// a test body regardless of link/run order. It is deliberately a before-each hook (not a one-shot
/// `Environment::SetUp`): the raw-lifecycle suite in `gtest_cas_blob_upload_pool.cpp` shuts the pool
/// down inside its own bodies, and those tests explicitly re-establish whatever pool state they assert
/// on as their FIRST action, so re-ensuring the pool before them is harmless.
///
/// It ALSO shuts the pool down once, in `OnTestProgramEnd` (the last gtest event, fired from inside
/// `RUN_ALL_TESTS` before `gtest_main`'s exit `SCOPE_EXIT`). This is mandatory, not cosmetic: the blob
/// upload pool is a `ThreadFromGlobalPool`-backed `ThreadPool`, so its idle workers occupy GlobalThreadPool
/// std::threads. `gtest_main` shuts the GlobalThreadPool down at process exit by JOINING those std::threads
/// -- but a lingering blob-pool worker never returns until the blob pool itself is destroyed, so leaving
/// the pool up at exit deadlocks the whole binary (main joins a std::thread that is running a blob-pool
/// worker that waits for the blob pool to shut down). Draining it here, before `RUN_ALL_TESTS` returns,
/// releases those std::threads first.
namespace
{

class BlobUploadPoolEnsuringListener : public ::testing::EmptyTestEventListener
{
public:
    void OnTestStart(const ::testing::TestInfo &) override
    {
        DB::Cas::tests::ensureBlobUploadPoolForTest();
    }

    void OnTestProgramEnd(const ::testing::UnitTest &) override
    {
        /// Release the pool's GlobalThreadPool-backed workers BEFORE `gtest_main` joins the GlobalThreadPool
        /// at exit (see the class comment) -- otherwise the binary deadlocks at exit. Idempotent.
        DB::Cas::shutdownBlobUploadPool();
    }
};

const bool registered_blob_upload_pool_listener = []
{
    ::testing::UnitTest::GetInstance()->listeners().Append(new BlobUploadPoolEnsuringListener);
    return true;
}();

}
