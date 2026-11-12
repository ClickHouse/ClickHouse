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
#include <functional>
#include <iterator>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <cstring>

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

void expectNoFreeDescriptors(const std::function<void()> & action, const std::string & what)
{
    try
    {
        action();
        ADD_FAILURE() << "Expected EMFILE: " << what;
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
    const std::string old_content(2048, 'x');
    DirectoryChanges changes(dir);

    DescriptorTableGuard guard;
    const rlim_t limit = leaveOneFreeDescriptor(guard);
    ASSERT_NO_THROW(channel->open());
    ASSERT_EQ(0, setSoftLimit(limit, guard.original_limit));
    ASSERT_NO_THROW(channel->log(makeMessage(old_content)));
    changes.take();

    /// The new file cannot be opened: the message goes to the current file, and the error is reported.
    expectNoFreeDescriptors([&] { channel->log(makeMessage("first message without descriptors")); }, "rotation");
    expectNoFreeDescriptors([&] { channel->log(makeMessage("second message without descriptors")); }, "rotation");
    /// A failed attempt renames nothing, so the current file never disappears.
    EXPECT_EQ(0u, changes.take());
    EXPECT_FALSE(std::filesystem::exists(path + ".0"));
    EXPECT_GT(channel->size(), old_content.size());

    guard.release();

    const std::string current = readFile(path);
    EXPECT_NE(std::string::npos, current.find("first message without descriptors"));
    EXPECT_NE(std::string::npos, current.find("second message without descriptors"));

    /// Descriptors are free again, so the next message completes the rotation.
    const std::string marker = "logged after descriptors were released";
    ASSERT_NO_THROW(channel->log(makeMessage(marker)));
    channel->close();

    const std::string archived = readFile(path + ".0");
    EXPECT_NE(std::string::npos, archived.find(old_content));
    EXPECT_NE(std::string::npos, archived.find("second message without descriptors"));
    EXPECT_EQ(std::string::npos, archived.find(marker));
    EXPECT_NE(std::string::npos, readFile(path).find(marker));
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
    DescriptorTableGuard guard;
    leaveOneFreeDescriptor(guard);
    expectNoFreeDescriptors([&] { channel->open(); }, "rotation on open");
    EXPECT_FALSE(std::filesystem::exists(path + ".0"));
    EXPECT_GT(channel->size(), 0u);

    guard.release();

    ASSERT_NO_THROW(channel->log(makeMessage("logged after descriptors were released")));
    channel->close();
    const std::string current = readFile(path);
    EXPECT_NE(std::string::npos, current.find("previous run"));
    EXPECT_NE(std::string::npos, current.find("logged after descriptors were released"));
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
