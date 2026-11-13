#include <gtest/gtest.h>

#include <Poco/AutoPtr.h>
#include <Poco/Exception.h>
#include <Poco/FileChannel.h>
#include <Poco/Message.h>

#include <lz4frame.h>

#include <csignal>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <random>
#include <string>
#include <vector>

#include <sys/resource.h>
#include <unistd.h>

/// A compressed log file that cannot grow, as on a full disk, must not terminate the process when it is rotated or closed.

namespace
{

/// glibc defines `RLIMIT_FSIZE` as a self-referential macro, which trips -Wdisabled-macro-expansion inside gtest macros.
const auto fsize_resource = RLIMIT_FSIZE;

using SignalHandler = void (*)(int);

/// Restores the process-wide file size limit and `SIGXFSZ` disposition on every exit path, including a fatal `ASSERT_*`.
struct TestEnvironmentGuard
{
    std::filesystem::path dir;
    struct rlimit original_limit;
    SignalHandler original_handler;

    ~TestEnvironmentGuard()
    {
        EXPECT_EQ(0, setrlimit(fsize_resource, &original_limit));
        (void)std::signal(SIGXFSZ, original_handler);
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }
};

std::filesystem::path makeTestDirectory(const std::string & name)
{
    auto dir = std::filesystem::temp_directory_path() / ("gtest_compressed_log_file_" + name + "_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

int setFileSizeLimit(rlim_t value, const struct rlimit & original_limit)
{
    struct rlimit limit = original_limit;
    limit.rlim_cur = value;
    return setrlimit(fsize_resource, &limit);
}

Poco::AutoPtr<Poco::FileChannel> makeChannel(const std::filesystem::path & dir)
{
    Poco::AutoPtr<Poco::FileChannel> channel = new Poco::FileChannel((dir / "server.log").string());
    channel->setProperty(Poco::FileChannel::PROP_STREAMCOMPRESS, "true");
    return channel;
}

Poco::Message makeMessage(const std::string & text)
{
    return Poco::Message("gtest_compressed_log_file", text, Poco::Message::PRIO_INFORMATION);
}

std::string makeIncompressiblePayload(size_t size)
{
    std::mt19937 rng(42); // NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp): deterministic seed for reproducible test
    std::string payload(size, ' ');
    for (auto & c : payload)
        c = static_cast<char>('a' + rng() % 26);
    return payload;
}

struct DecodedFrame
{
    std::string text;
    bool complete = false;
};

DecodedFrame decodeFrame(const std::filesystem::path & path)
{
    std::ifstream in(path, std::ios::binary);
    const std::string compressed((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());

    DecodedFrame frame;
    LZ4F_dctx * ctx = nullptr;
    if (LZ4F_isError(LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION)))
    {
        ADD_FAILURE() << "Cannot create an LZ4F decompression context";
        return frame;
    }

    std::vector<char> buffer(64 * 1024);
    size_t pos = 0;
    while (true)
    {
        size_t src_size = compressed.size() - pos;
        size_t dst_size = buffer.size();
        size_t ret = LZ4F_decompress(ctx, buffer.data(), &dst_size, compressed.data() + pos, &src_size, nullptr);
        if (LZ4F_isError(ret))
        {
            ADD_FAILURE() << path << ": " << LZ4F_getErrorName(ret);
            break;
        }
        frame.text.append(buffer.data(), dst_size);
        pos += src_size;
        /// `LZ4F_decompress` returns 0 once it has decoded the end mark.
        if (ret == 0)
        {
            frame.complete = true;
            break;
        }
        if (src_size == 0 && dst_size == 0)
            break;
    }

    LZ4F_freeDecompressionContext(ctx);
    return frame;
}

}

TEST(CompressedLogFile, RotationWhenEndMarkCannotBeWritten)
{
    struct rlimit original_limit{};
    ASSERT_EQ(0, getrlimit(fsize_resource, &original_limit));
    /// Writes past the limit then fail with `EFBIG` instead of killing the process.
    TestEnvironmentGuard guard{makeTestDirectory("rotation"), original_limit, std::signal(SIGXFSZ, SIG_IGN)};

    auto channel = makeChannel(guard.dir);
    channel->setProperty(Poco::FileChannel::PROP_ROTATION, "100");

    const std::string payload = makeIncompressiblePayload(256);
    ASSERT_NO_THROW(channel->log(makeMessage(payload)));
    ASSERT_GE(channel->size(), 100u);

    /// The next message rotates the file: its end mark no longer fits, while the new file does.
    ASSERT_EQ(0, setFileSizeLimit(std::filesystem::file_size(guard.dir / "server.log.lz4"), original_limit));
    EXPECT_NO_THROW(channel->log(makeMessage("after rotation")));

    ASSERT_EQ(0, setrlimit(fsize_resource, &original_limit));
    channel->close();

    const auto archived = decodeFrame(guard.dir / "server.log.0.lz4");
    EXPECT_EQ(payload + "\n", archived.text);
    EXPECT_FALSE(archived.complete);

    const auto current = decodeFrame(guard.dir / "server.log.lz4");
    EXPECT_EQ("after rotation\n", current.text);
    EXPECT_TRUE(current.complete);
}

TEST(CompressedLogFile, CloseAndReopenWhenFileCannotGrow)
{
    struct rlimit original_limit{};
    ASSERT_EQ(0, getrlimit(fsize_resource, &original_limit));
    TestEnvironmentGuard guard{makeTestDirectory("reopen"), original_limit, std::signal(SIGXFSZ, SIG_IGN)};

    auto channel = makeChannel(guard.dir);
    ASSERT_NO_THROW(channel->open());

    ASSERT_EQ(0, setFileSizeLimit(0, original_limit));
    EXPECT_NO_THROW(channel->log(makeMessage("lost")));
    EXPECT_NO_THROW(channel->close());

    /// The reopened file cannot take its header.
    EXPECT_THROW(channel->log(makeMessage("lost too")), Poco::WriteFileException);

    ASSERT_EQ(0, setrlimit(fsize_resource, &original_limit));
    const std::string marker = "logged after the file can grow again";
    EXPECT_NO_THROW(channel->log(makeMessage(marker)));
    channel->close();

    const auto current = decodeFrame(guard.dir / "server.log.lz4");
    EXPECT_EQ(marker + "\n", current.text);
    EXPECT_TRUE(current.complete);
}
