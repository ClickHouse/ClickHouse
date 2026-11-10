#include <gtest/gtest.h>

#include <Poco/AutoPtr.h>
#include <Poco/Exception.h>
#include <Poco/FileChannel.h>
#include <Poco/Message.h>

#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/resource.h>
#include <unistd.h>

/** Regression test for log rotation when the process has no free file descriptors.
  * https://github.com/ClickHouse/clickhouse-core-incidents/issues/1699
  *
  * `ArchiveStrategy::archive` takes ownership of the current `LogFile` and deletes it before it renames
  * the files and opens a new one. When the process has no free file descriptors (EMFILE), that open throws,
  * and the fallback `newLogFile` in `FileChannel` throws too. `FileChannel::_pFile` must not keep pointing
  * to the deleted `LogFile`: the next message calls `RotateBySizeStrategy::mustRotate` on it, which reads
  * freed memory (heap-use-after-free; in production a SIGSEGV in `LogFileImpl::sizeImpl`).
  *
  * The descriptor table is exhausted deterministically. The soft limit of `RLIMIT_NOFILE` is lowered to
  * `limit`, and `/dev/null` is opened until `open` fails with EMFILE. Then the limit is raised by one, so
  * the log file gets the descriptor `limit`, and lowered back. When `archive` closes that descriptor, its
  * number is not below the limit, so it cannot be reused. This is what happens in production, where other
  * threads take every freed descriptor at once.
  */

namespace
{

/// Capture the enumerator outside of any macro: glibc defines `RLIMIT_NOFILE` as a self-referential
/// macro, which trips -Wdisabled-macro-expansion when re-scanned inside gtest's ASSERT_*/EXPECT_* macros.
const auto nofile_resource = RLIMIT_NOFILE;

/// Owns the descriptors that fill the table and restores the original limit, on every exit path.
/// Leaving the table full or the limit lowered would break unrelated tests in the same process.
struct DescriptorTableGuard
{
    struct rlimit original_limit{};
    std::vector<int> descriptors;

    void release()
    {
        for (int fd : descriptors)
            ::close(fd);
        descriptors.clear();
        EXPECT_EQ(0, setrlimit(nofile_resource, &original_limit));
    }

    ~DescriptorTableGuard() { release(); }
};

int setSoftLimit(rlim_t value, const struct rlimit & original_limit)
{
    struct rlimit limit = original_limit;
    limit.rlim_cur = value;
    return setrlimit(nofile_resource, &limit);
}

/// Opens `/dev/null` until `open` fails. Returns the errno of the failed `open`.
int fillDescriptorTable(DescriptorTableGuard & guard)
{
    while (true)
    {
        int fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
        if (fd < 0)
            return errno;
        guard.descriptors.push_back(fd);
    }
}

Poco::Message makeMessage(const std::string & text)
{
    return Poco::Message("gtest_file_channel_rotation_failure", text, Poco::Message::PRIO_INFORMATION);
}

/// Logs one message and expects that it fails because the process has no free file descriptors.
void expectNoFreeDescriptors(Poco::FileChannel & channel, const std::string & text)
{
    try
    {
        channel.log(makeMessage(text));
        ADD_FAILURE() << "Expected EMFILE while logging: " << text;
    }
    catch (const Poco::FileException & e)
    {
        EXPECT_EQ(EMFILE, e.code()) << e.displayText();
    }
}

std::string readFile(const std::string & path)
{
    std::ifstream in(path);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

}

TEST(FileChannelRotation, NoFreeDescriptorsDuringRotation)
{
    const auto dir = std::filesystem::temp_directory_path() / ("gtest_file_channel_rotation_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    struct DirectoryGuard
    {
        std::filesystem::path path;
        ~DirectoryGuard()
        {
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }
    } directory_guard{dir};
    const std::string path = (dir / "server.log").string();

    Poco::AutoPtr<Poco::FileChannel> channel = new Poco::FileChannel(path);
    channel->setProperty(Poco::FileChannel::PROP_ROTATION, "1K");
    channel->setProperty(Poco::FileChannel::PROP_ARCHIVE, "number");
    channel->setProperty(Poco::FileChannel::PROP_COMPRESS, "false");

    DescriptorTableGuard guard;
    ASSERT_EQ(0, getrlimit(nofile_resource, &guard.original_limit));
    /// Keep `limit + 1` within the hard limit, and keep the table small so that filling it is fast.
    const rlim_t limit = std::min<rlim_t>({guard.original_limit.rlim_cur, guard.original_limit.rlim_max - 1, 1024});

    /// Fill every descriptor below `limit`, then allow exactly one more for the log file.
    ASSERT_EQ(0, setSoftLimit(limit, guard.original_limit));
    ASSERT_EQ(EMFILE, fillDescriptorTable(guard));
    ASSERT_EQ(0, setSoftLimit(limit + 1, guard.original_limit));
    ASSERT_NO_THROW(channel->open());
    ASSERT_EQ(0, setSoftLimit(limit, guard.original_limit));

    /// The file is now larger than the rotation size, so the next message rotates it.
    ASSERT_NO_THROW(channel->log(makeMessage(std::string(2048, 'x'))));

    /// `archive` deletes the current `LogFile` and renames the file, then fails to open a new one.
    expectNoFreeDescriptors(*channel, "rotation without free descriptors");
    EXPECT_TRUE(std::filesystem::exists(path + ".0"));

    /// After the failed rotation the channel has no current file. `size` returns 0 only then;
    /// with a dangling `_pFile` it reads the deleted `LogFile`.
    EXPECT_EQ(0u, channel->size());

    /// The channel must not use the deleted `LogFile`: it tries to open the file again and fails the same way.
    expectNoFreeDescriptors(*channel, "still no free descriptors");

    guard.release();

    /// Descriptors are free again, so logging recovers in a new file.
    const std::string marker = "logged after descriptors were released";
    ASSERT_NO_THROW(channel->log(makeMessage(marker)));
    channel->close();
    EXPECT_NE(std::string::npos, readFile(path).find(marker));
}
