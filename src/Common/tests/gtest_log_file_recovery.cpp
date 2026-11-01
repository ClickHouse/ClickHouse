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
  * LogFileImpl threw an exception but never cleared these flags, so every subsequent operation kept
  * failing forever, even after disk space was freed. The only recovery was a server restart.
  *
  * Concretely (see Poco::FileChannel::log): each log message first calls LogFile::size via
  * RotateBySizeStrategy::mustRotate, then LogFile::write. A failed write left the stream in an error
  * state; the *next* message's size() then threw FileException (uncaught, unlike the write path), so
  * the server spammed that exception on every log attempt and never reached write() again. Clearing
  * the error flags inside the write paths (writeImpl / writeBinaryImpl) means the stream is good
  * again before the next size() check, so logging recovers once the underlying condition is resolved.
  *
  * Both write paths are covered here because both are real recovery sites:
  *   - LogFile::write       -> writeImpl        (plain text logging),
  *   - LogFile::writeBinary -> writeBinaryImpl  (compressed logging: `logger_stream_compress` creates
  *                                               Poco::CompressedLogFile, whose write() emits compressed
  *                                               chunks through writeBinary).
  * Each case induces the failure and recovers through the same write kind, so removing the clear in
  * either writeImpl or writeBinaryImpl makes the corresponding test fail.
  *
  * The failure is induced deterministically by lowering RLIMIT_FSIZE so that further writes fail with
  * EFBIG, which is equivalent (from the stream's point of view) to a disk-full condition. SIGXFSZ is
  * ignored for the duration so the process is not terminated when the limit is exceeded.
  */

namespace
{

enum class WriteKind
{
    Text,
    Binary
};

void writePayload(Poco::LogFile & log_file, WriteKind kind, const std::string & payload, bool flush)
{
    if (kind == WriteKind::Text)
        log_file.write(payload, flush);
    else
        log_file.writeBinary(payload.data(), payload.size(), flush);
}

void runRecoveryScenario(WriteKind kind, const std::string & path_suffix)
{
    const std::string path = (std::filesystem::temp_directory_path()
        / ("gtest_log_file_recovery_" + path_suffix + "_" + std::to_string(::getpid()) + ".log")).string();
    std::filesystem::remove(path);

    /// Capture the enumerator outside of any macro: glibc defines `RLIMIT_FSIZE` as a
    /// self-referential macro, which trips -Wdisabled-macro-expansion when re-scanned inside
    /// gtest's ASSERT_*/EXPECT_* macros.
    const auto fsize_resource = RLIMIT_FSIZE;

    struct rlimit old_limit{};
    ASSERT_EQ(0, getrlimit(fsize_resource, &old_limit));
    auto old_sigxfsz = std::signal(SIGXFSZ, SIG_IGN);

    /// Restore process-wide state (file-size limit, SIGXFSZ handler) on every exit path, including a
    /// fatal ASSERT_* failure, which returns from the function rather than throwing. Leaving the lowered
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
    ASSERT_NO_THROW(writePayload(log_file, kind, "before disk full\n", true));

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
            writePayload(log_file, kind, std::string(1024, 'x') + "\n", true);
        }
        catch (const Poco::Exception &)
        {
            threw = true;
        }
    }
    ASSERT_TRUE(threw) << "Expected a write past RLIMIT_FSIZE to fail";

    /// Free up space again.
    ASSERT_EQ(0, setrlimit(fsize_resource, &old_limit));

    /// Mirror the production rotation check: size() must not keep throwing after recovery. The failing
    /// write above already cleared the stream, so this returns normally (this is exactly why the next
    /// log message's mustRotate -> size() no longer throws the spammed FileException).
    EXPECT_NO_THROW(log_file.size());

    /// A later write of the same kind must reach the file again.
    const std::string marker = "recovered after disk full";
    EXPECT_NO_THROW(writePayload(log_file, kind, marker + "\n", true));

    /// Verify the recovery marker actually landed on disk.
    std::ifstream in(path);
    std::string contents((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    EXPECT_NE(std::string::npos, contents.find(marker));
}

}

/// Recovery through the plain-text write path (LogFile::write -> writeImpl).
TEST(LogFileRecovery, RecoversAfterFailedWrite)
{
    runRecoveryScenario(WriteKind::Text, "text");
}

/// Recovery through the binary write path (LogFile::writeBinary -> writeBinaryImpl), which is what
/// compressed logging (Poco::CompressedLogFile, enabled by `logger_stream_compress`) uses.
TEST(LogFileRecovery, RecoversAfterFailedBinaryWrite)
{
    runRecoveryScenario(WriteKind::Binary, "binary");
}
