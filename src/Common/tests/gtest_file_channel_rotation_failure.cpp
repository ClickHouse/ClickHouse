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

/// Log rotation must not leave `FileChannel` with a deleted `LogFile` when the process has no free file descriptors.

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

Poco::Message makeMessage(const std::string & text)
{
    return Poco::Message("gtest_file_channel_rotation_failure", text, Poco::Message::PRIO_INFORMATION);
}

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
    const rlim_t limit = std::min<rlim_t>({guard.original_limit.rlim_cur, guard.original_limit.rlim_max - 1, 1024});

    /// The log file gets descriptor `limit`, so closing it in `archive` frees no descriptor below the limit.
    ASSERT_EQ(0, setSoftLimit(limit, guard.original_limit));
    ASSERT_EQ(EMFILE, fillDescriptorTable(guard));
    ASSERT_EQ(0, setSoftLimit(limit + 1, guard.original_limit));
    ASSERT_NO_THROW(channel->open());
    ASSERT_EQ(0, setSoftLimit(limit, guard.original_limit));

    ASSERT_NO_THROW(channel->log(makeMessage(std::string(2048, 'x'))));

    expectNoFreeDescriptors(*channel, "rotation without free descriptors");
    EXPECT_TRUE(std::filesystem::exists(path + ".0"));
    EXPECT_EQ(0u, channel->size());
    expectNoFreeDescriptors(*channel, "still no free descriptors");

    guard.release();

    const std::string marker = "logged after descriptors were released";
    ASSERT_NO_THROW(channel->log(makeMessage(marker)));
    channel->close();
    EXPECT_NE(std::string::npos, readFile(path).find(marker));
}
