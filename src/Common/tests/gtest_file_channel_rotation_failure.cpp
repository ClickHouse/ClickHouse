#include <gtest/gtest.h>

#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>

#include <Poco/AutoPtr.h>
#include <Poco/Exception.h>
#include <Poco/FileChannel.h>
#include <Poco/Message.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <cstring>
#include <ctime>
#include <string_view>

#include <fmt/format.h>

#include <fcntl.h>
#include <sys/resource.h>
#include <unistd.h>

#if defined(OS_LINUX)
#include <sys/inotify.h>
#endif

/// A failed log rotation must keep the current file open and lose no message.

namespace
{

/// glibc defines `RLIMIT_NOFILE` as a self-referential macro, which trips -Wdisabled-macro-expansion inside gtest macros.
const auto nofile_resource = RLIMIT_NOFILE;

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

/// Fills every descriptor below `limit` and allows exactly one more. Returns `limit`.
rlim_t leaveOneFreeDescriptor(DescriptorTableGuard & guard)
{
    EXPECT_EQ(0, getrlimit(nofile_resource, &guard.original_limit));
    const rlim_t limit = std::min<rlim_t>({guard.original_limit.rlim_cur, guard.original_limit.rlim_max - 1, 1024});
    EXPECT_EQ(0, setSoftLimit(limit, guard.original_limit));
    EXPECT_EQ(EMFILE, fillDescriptorTable(guard));
    EXPECT_EQ(0, setSoftLimit(limit + 1, guard.original_limit));
    return limit;
}

/// Counts creations, deletions and renames in a directory. Counts nothing outside Linux.
class DirectoryChanges
{
public:
    explicit DirectoryChanges([[maybe_unused]] const std::filesystem::path & dir)
    {
#if defined(OS_LINUX)
        fd = inotify_init1(IN_NONBLOCK | IN_CLOEXEC);
        EXPECT_GE(fd, 0);
        EXPECT_GE(inotify_add_watch(fd, dir.c_str(), IN_CREATE | IN_DELETE | IN_MOVE), 0);
#endif
    }

    ~DirectoryChanges()
    {
        if (fd >= 0)
            ::close(fd);
    }

    /// Returns the number of changes since the previous call.
    size_t take() const
    {
        size_t count = 0;
#if defined(OS_LINUX)
        char buffer[4096];
        ssize_t bytes = 0;
        while ((bytes = ::read(fd, buffer, sizeof(buffer))) > 0)
        {
            for (ssize_t offset = 0; offset < bytes; ++count)
            {
                inotify_event event{};
                std::memcpy(&event, buffer + offset, sizeof(event));
                offset += sizeof(event) + event.len;
            }
        }
#endif
        return count;
    }

private:
    int fd = -1;
};

Poco::Message makeMessage(const std::string & text)
{
    return Poco::Message("gtest_file_channel_rotation_failure", text, Poco::Message::PRIO_INFORMATION);
}

constexpr std::string_view rotation_failed = "Log file rotation failed";

/// The channel writes records formatted above it, for example JSON; it must not add lines of its own.
std::string jsonRecord(std::string_view text)
{
    return fmt::format("{{\"message\":\"{}\"}}", text);
}

bool allLinesAreJsonRecords(const std::string & text)
{
    std::istringstream in(text);
    for (std::string line; std::getline(in, line);)
        if (!line.empty() && (line.front() != '{' || line.back() != '}'))
            return false;
    return true;
}

/// Captures stderr until `take`. Declare it before `DescriptorTableGuard`: capturing needs free descriptors.
class StderrCapture
{
public:
    StderrCapture() { testing::internal::CaptureStderr(); }

    ~StderrCapture()
    {
        if (!taken)
            testing::internal::GetCapturedStderr();
    }

    std::string take()
    {
        taken = true;
        return testing::internal::GetCapturedStderr();
    }

private:
    bool taken = false;
};

size_t countOccurrences(std::string_view text, std::string_view needle)
{
    size_t count = 0;
    for (size_t pos = text.find(needle); pos != std::string_view::npos; pos = text.find(needle, pos + needle.size()))
        ++count;
    return count;
}

std::chrono::system_clock::time_point nextMinuteBoundary()
{
    return std::chrono::floor<std::chrono::minutes>(std::chrono::system_clock::now()) + std::chrono::minutes(1);
}

/// The `hh:mm` rotation time of `time` in UTC, the default of `FileChannel::PROP_TIMES`.
std::string utcHourMinute(std::chrono::system_clock::time_point time)
{
    const std::time_t seconds = std::chrono::system_clock::to_time_t(time);
    std::tm utc{};
    gmtime_r(&seconds, &utc);
    return fmt::format("{:02}:{:02}", utc.tm_hour, utc.tm_min);
}

std::string readFile(const std::string & path)
{
    std::ifstream in(path);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

std::string readGzipFile(const std::string & path)
{
    auto in = DB::wrapReadBufferWithCompressionMethod(std::make_unique<DB::ReadBufferFromFile>(path), DB::CompressionMethod::Gzip);
    std::string out;
    DB::readStringUntilEOF(out, *in);
    return out;
}

class FileChannelRotation : public ::testing::Test
{
protected:
    void SetUp() override
    {
        dir = std::filesystem::temp_directory_path() / ("gtest_file_channel_rotation_" + std::to_string(::getpid()));
        std::filesystem::remove_all(dir);
        std::filesystem::create_directories(dir);
        path = (dir / "server.log").string();
    }

    void TearDown() override
    {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    Poco::AutoPtr<Poco::FileChannel> makeChannel(const std::string & rotation, bool compress, bool rotate_on_open = false)
    {
        Poco::AutoPtr<Poco::FileChannel> channel = new Poco::FileChannel(path);
        if (!rotation.empty())
            channel->setProperty(Poco::FileChannel::PROP_ROTATION, rotation);
        channel->setProperty(Poco::FileChannel::PROP_ARCHIVE, "number");
        channel->setProperty(Poco::FileChannel::PROP_COMPRESS, compress ? "true" : "false");
        channel->setProperty(Poco::FileChannel::PROP_ROTATEONOPEN, rotate_on_open ? "true" : "false");
        return channel;
    }

    std::filesystem::path dir;
    std::string path;
};

}

TEST_F(FileChannelRotation, NoFreeDescriptorsDuringRotation)
{
    auto channel = makeChannel("1K", /* compress */ false);
    const std::string old_content = jsonRecord(std::string(2048, 'x'));
    DirectoryChanges changes(dir);

    StderrCapture errors;
    DescriptorTableGuard guard;
    const rlim_t limit = leaveOneFreeDescriptor(guard);
    ASSERT_NO_THROW(channel->open());
    ASSERT_EQ(0, setSoftLimit(limit, guard.original_limit));
    ASSERT_NO_THROW(channel->log(makeMessage(old_content)));
    changes.take();

    /// The new file cannot be opened: logging continues in the current file, and no message fails.
    for (const char * text : {"first message without descriptors", "second message without descriptors", "third message without descriptors"})
        ASSERT_NO_THROW(channel->log(makeMessage(jsonRecord(text))));
    /// A failed attempt renames nothing, so the current file never disappears.
    EXPECT_EQ(0u, changes.take());
    EXPECT_FALSE(std::filesystem::exists(path + ".0"));
    EXPECT_GT(channel->size(), old_content.size());

    guard.release();

    const std::string current = readFile(path);
    EXPECT_NE(std::string::npos, current.find("first message without descriptors"));
    EXPECT_NE(std::string::npos, current.find("third message without descriptors"));
    /// The failure is reported once to stderr, and the file keeps only the records.
    EXPECT_EQ(1u, countOccurrences(errors.take(), rotation_failed));
    EXPECT_EQ(0u, countOccurrences(current, rotation_failed));
    EXPECT_TRUE(allLinesAreJsonRecords(current));

    /// Descriptors are free again, so the next message completes the rotation.
    const std::string marker = jsonRecord("logged after descriptors were released");
    ASSERT_NO_THROW(channel->log(makeMessage(marker)));
    channel->close();

    const std::string archived = readFile(path + ".0");
    EXPECT_NE(std::string::npos, archived.find(old_content));
    EXPECT_NE(std::string::npos, archived.find("third message without descriptors"));
    EXPECT_EQ(std::string::npos, archived.find(marker));
    const std::string after = readFile(path);
    EXPECT_NE(std::string::npos, after.find(marker));
    EXPECT_TRUE(allLinesAreJsonRecords(after));
    EXPECT_FALSE(std::filesystem::exists(path + ".new.incomplete"));
}

TEST_F(FileChannelRotation, NoFreeDescriptorsDuringRotateOnOpen)
{
    {
        std::ofstream previous_run(path);
        previous_run << "previous run\n";
    }
    auto channel = makeChannel("", /* compress */ false, /* rotate_on_open */ true);

    /// The existing file takes the only free descriptor, so the new file cannot be opened.
    StderrCapture errors;
    DescriptorTableGuard guard;
    leaveOneFreeDescriptor(guard);
    /// The rotation fails, but the channel stays usable with the existing file.
    ASSERT_NO_THROW(channel->open());
    EXPECT_FALSE(std::filesystem::exists(path + ".0"));
    EXPECT_GT(channel->size(), 0u);

    guard.release();

    /// The pending rotation is done with the next message.
    ASSERT_NO_THROW(channel->log(makeMessage("logged after descriptors were released")));
    channel->close();
    EXPECT_NE(std::string::npos, readFile(path + ".0").find("previous run"));
    const std::string current = readFile(path);
    EXPECT_NE(std::string::npos, current.find("logged after descriptors were released"));
    EXPECT_EQ(0u, countOccurrences(current, rotation_failed));
    EXPECT_EQ(1u, countOccurrences(errors.take(), rotation_failed));
}

TEST_F(FileChannelRotation, ScheduledRotationRetriedAfterFailure)
{
    /// `RotateAtTimeStrategy` fires only on a minute boundary of the real clock, so this test waits for the next one.
    auto boundary = nextMinuteBoundary();
    if (boundary - std::chrono::system_clock::now() < std::chrono::seconds(5))
    {
        std::this_thread::sleep_until(boundary);
        boundary = nextMinuteBoundary();
    }
    auto channel = makeChannel(utcHourMinute(boundary), /* compress */ false);
    ASSERT_NO_THROW(channel->log(makeMessage("before the scheduled rotation")));
    std::this_thread::sleep_until(boundary + std::chrono::milliseconds(100));

    /// The schedule fires, but the new file cannot be opened.
    StderrCapture errors;
    DescriptorTableGuard guard;
    ASSERT_EQ(0, getrlimit(nofile_resource, &guard.original_limit));
    const rlim_t limit = std::min<rlim_t>({guard.original_limit.rlim_cur, guard.original_limit.rlim_max - 1, 1024});
    ASSERT_EQ(0, setSoftLimit(limit, guard.original_limit));
    ASSERT_EQ(EMFILE, fillDescriptorTable(guard));
    ASSERT_NO_THROW(channel->log(makeMessage("at the scheduled time")));
    EXPECT_FALSE(std::filesystem::exists(path + ".0"));

    guard.release();

    /// The schedule already moved to the next day, but the pending rotation must still happen now.
    ASSERT_NO_THROW(channel->log(makeMessage("after descriptors were released")));
    channel->close();
    const std::string archived = readFile(path + ".0");
    EXPECT_NE(std::string::npos, archived.find("before the scheduled rotation"));
    EXPECT_NE(std::string::npos, archived.find("at the scheduled time"));
    EXPECT_NE(std::string::npos, readFile(path).find("after descriptors were released"));
    EXPECT_EQ(1u, countOccurrences(errors.take(), rotation_failed));
}

TEST_F(FileChannelRotation, NoFreeDescriptorsDuringFirstIntervalCheck)
{
    {
        std::ofstream previous_run(path);
        previous_run << "previous run\n";
    }
    auto channel = makeChannel("daily", /* compress */ false);
    ASSERT_NO_THROW(channel->open());

    /// The first interval check reads the first line of the file, which needs one more descriptor.
    StderrCapture errors;
    DescriptorTableGuard guard;
    ASSERT_EQ(0, getrlimit(nofile_resource, &guard.original_limit));
    const rlim_t limit = std::min<rlim_t>({guard.original_limit.rlim_cur, guard.original_limit.rlim_max - 1, 1024});
    ASSERT_EQ(0, setSoftLimit(limit, guard.original_limit));
    ASSERT_EQ(EMFILE, fillDescriptorTable(guard));
    ASSERT_NO_THROW(channel->log(makeMessage("first message without descriptors")));
    ASSERT_NO_THROW(channel->log(makeMessage("second message without descriptors")));

    guard.release();
    EXPECT_EQ(1u, countOccurrences(errors.take(), rotation_failed));

    /// The check succeeds now; the file is younger than a day, so it is not rotated.
    ASSERT_NO_THROW(channel->log(makeMessage("logged after descriptors were released")));
    channel->close();
    EXPECT_FALSE(std::filesystem::exists(path + ".0"));
    const std::string current = readFile(path);
    for (const char * text : {"previous run", "first message without descriptors", "second message without descriptors", "logged after descriptors were released"})
        EXPECT_NE(std::string::npos, current.find(text)) << text;
}

TEST_F(FileChannelRotation, CurrentFileRemovedFromOutside)
{
    auto channel = makeChannel("1K", /* compress */ false);
    ASSERT_NO_THROW(channel->log(makeMessage(std::string(2048, 'x'))));
    std::filesystem::remove(path);

    /// There is nothing to archive, so the rotation starts a new file instead of writing into the removed one.
    ASSERT_NO_THROW(channel->log(makeMessage("logged after removal")));
    channel->close();
    EXPECT_FALSE(std::filesystem::exists(path + ".0"));
    EXPECT_NE(std::string::npos, readFile(path).find("logged after removal"));
}

TEST_F(FileChannelRotation, CompressArchivedFile)
{
    auto channel = makeChannel("1K", /* compress */ true);
    const std::string old_content(2048, 'x');
    ASSERT_NO_THROW(channel->log(makeMessage(old_content)));
    ASSERT_NO_THROW(channel->log(makeMessage("logged after rotation")));

    /// Compression runs in the background and removes the uncompressed file when it is done.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::filesystem::exists(path + ".0") && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    channel->close();

    ASSERT_FALSE(std::filesystem::exists(path + ".0"));
    const std::string archived = readGzipFile(path + ".0.gz");
    EXPECT_NE(std::string::npos, archived.find(old_content));
    EXPECT_EQ(std::string::npos, archived.find("logged after rotation"));
    EXPECT_NE(std::string::npos, readFile(path).find("logged after rotation"));
}
