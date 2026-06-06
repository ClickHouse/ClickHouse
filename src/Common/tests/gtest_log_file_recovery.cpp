#include <gtest/gtest.h>

#include <Poco/Exception.h>
#include <Poco/LogFile.h>

#include <csignal>
#include <filesystem>
#include <fstream>
#include <string>

#include <sys/resource.h>

/** Regression test for recovery of Poco::LogFile after a failed write.
  *
  * When the log disk becomes full (or any write fails for another reason), the underlying
  * std::ofstream / Poco::FileOutputStream raises its error flags (failbit / badbit). Before the fix,
  * LogFileImpl threw an exception but never cleared these flags, so every subsequent operation —
  * including LogFileImpl::sizeImpl, which is called on every rotation check — kept failing forever,
  * even after disk space was freed. The only recovery was a server restart.
  *
  * The contract verified here: once a failed write leaves the stream in an error state, LogFile must
  * recover after the underlying condition is resolved — size() must not keep throwing, and a later
  * write must actually reach the file.
  *
  * The failure is induced deterministically by lowering RLIMIT_FSIZE so that further writes fail with
  * EFBIG, which is equivalent (from the stream's point of view) to a disk-full condition. SIGXFSZ is
  * ignored for the duration so the process is not terminated when the limit is exceeded.
  */
TEST(LogFileRecovery, RecoversAfterFailedWrite)
{
    const std::string path = (std::filesystem::temp_directory_path()
        / ("gtest_log_file_recovery_" + std::to_string(::getpid()) + ".log")).string();
    std::filesystem::remove(path);

    /// Capture the enumerator outside of any macro: glibc defines `RLIMIT_FSIZE` as a
    /// self-referential macro, which trips -Wdisabled-macro-expansion when re-scanned inside
    /// gtest's ASSERT_*/EXPECT_* macros.
    const auto fsize_resource = RLIMIT_FSIZE;

    struct rlimit old_limit{};
    ASSERT_EQ(0, getrlimit(fsize_resource, &old_limit));
    auto old_sigxfsz = std::signal(SIGXFSZ, SIG_IGN);

    /// Restore process-wide state (file-size limit, SIGXFSZ handler) on every exit path, including a
    /// fatal ASSERT_* failure, which returns from the test rather than throwing. Leaving the lowered
    /// limit or the ignored signal in place would corrupt unrelated tests in the same process.
    struct StateGuard
    {
        decltype(fsize_resource) resource;
        struct rlimit limit;
        decltype(old_sigxfsz) handler;
        std::string file;

        ~StateGuard()
        {
            EXPECT_EQ(0, setrlimit(resource, &limit));
            (void)std::signal(SIGXFSZ, handler);
            std::filesystem::remove(file);
        }
    } guard{fsize_resource, old_limit, old_sigxfsz, path};

    Poco::LogFile log_file(path);

    /// A normal write succeeds.
    ASSERT_NO_THROW(log_file.write("before disk full\n", true));

    /// Constrain the maximum file size so that subsequent writes fail with EFBIG,
    /// emulating a disk-full condition.
    struct rlimit small_limit = old_limit;
    small_limit.rlim_cur = 4096;
    ASSERT_EQ(0, setrlimit(fsize_resource, &small_limit));

    /// Writing past the limit must throw. The stream is now in an error state; without the fix
    /// the error flags would remain set forever.
    bool threw = false;
    for (size_t i = 0; i < 1024 && !threw; ++i)
    {
        try
        {
            log_file.write(std::string(1024, 'x') + "\n", true);
        }
        catch (const Poco::Exception &)
        {
            threw = true;
        }
    }
    ASSERT_TRUE(threw) << "Expected a write past RLIMIT_FSIZE to fail";

    /// Free up space again.
    ASSERT_EQ(0, setrlimit(fsize_resource, &old_limit));

    /// The bug: sizeImpl kept throwing on every rotation check because the error flags were
    /// never cleared. After the fix it must recover.
    EXPECT_NO_THROW(log_file.size());

    /// A later write must reach the file again.
    const std::string marker = "recovered after disk full\n";
    EXPECT_NO_THROW(log_file.write(marker, true));

    /// Verify the recovery marker actually landed on disk.
    std::ifstream in(path);
    std::string contents((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    EXPECT_NE(std::string::npos, contents.find("recovered after disk full"));
}
