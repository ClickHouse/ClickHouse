#include <gtest/gtest.h>

#include <IO/ReadBufferFromFileView.h>
#include <IO/ReadBufferFromFileBase.h>

#include <cstring>

using namespace DB;

namespace
{

/// How the inner buffer reacts to setReadUntilPosition - the axis that broke B115.
enum class InnerMode : uint8_t
{
    /// Like local file descriptors: setReadUntilPosition is a no-op, the buffer is kept.
    FileLike,
    /// Like ReadBufferFromS3: a range change rebases the offset to the CONSUMER position and
    /// DISCARDS the working buffer (the next nextImpl re-fetches from the consumer position).
    RemoteLike,
};

/// A seekable ReadBufferFromFileBase over a string, reading at most `chunk` bytes per nextImpl,
/// with selectable setReadUntilPosition semantics. Mirrors the state conventions of real
/// implementations: `file_offset` is the absolute offset of working_buffer.end().
class FakeInnerBuffer : public ReadBufferFromFileBase
{
public:
    FakeInnerBuffer(String data_, size_t chunk, InnerMode mode_)
        : ReadBufferFromFileBase(chunk, nullptr, 0)
        , data(std::move(data_))
        , mode(mode_)
    {
    }

    String getFileName() const override { return "fake_inner"; }
    std::optional<size_t> tryGetFileSize() override { return data.size(); }
    size_t getFileOffsetOfBufferEnd() const override { return file_offset; }
    off_t getPosition() override { return file_offset - available(); }

    off_t seek(off_t off, int whence) override
    {
        EXPECT_EQ(whence, SEEK_SET);
        const size_t target = static_cast<size_t>(off);
        /// In-buffer seek (both real local and S3 buffers do this).
        if (!working_buffer.empty() && target + working_buffer.size() >= file_offset && target < file_offset)
        {
            pos = working_buffer.end() - (file_offset - target);
            return off;
        }
        resetWorkingBuffer();
        file_offset = target;
        return off;
    }

    void setReadUntilPosition(size_t position) override
    {
        if (read_until && *read_until == position)
            return;
        if (mode == InnerMode::RemoteLike)
        {
            /// ReadBufferFromS3: offset = getPosition(); resetWorkingBuffer(); impl.reset();
            file_offset = getPosition();
            resetWorkingBuffer();
        }
        read_until = position;
    }

    void setReadUntilEnd() override { setReadUntilPosition(data.size()); }

private:
    bool nextImpl() override
    {
        const size_t limit = read_until ? std::min(*read_until, data.size()) : data.size();
        if (file_offset >= limit)
            return false;
        const size_t to_read = std::min(limit - file_offset, internal_buffer.size());
        memcpy(internal_buffer.begin(), data.data() + file_offset, to_read);
        working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + to_read);
        file_offset += to_read;
        return true;
    }

    String data;
    InnerMode mode;
    size_t file_offset = 0;
    std::optional<size_t> read_until;
};

constexpr size_t kHeader = 256; /// the view's left bound (the CHCA envelope size in production)

String makePayload(size_t size)
{
    String s(size, 0);
    for (size_t i = 0; i < size; ++i)
        s[i] = static_cast<char>((i * 131 + 7) % 251);
    return s;
}

std::unique_ptr<ReadBufferFromFileView> makeView(const String & payload, size_t chunk, InnerMode mode)
{
    String object = String(kHeader, '\xee') + payload;
    auto inner = std::make_unique<FakeInnerBuffer>(std::move(object), chunk, mode);
    return std::make_unique<ReadBufferFromFileView>(std::move(inner), "viewed", kHeader, kHeader + payload.size());
}

String readExact(ReadBuffer & buf, size_t n)
{
    String out(n, 0);
    buf.readStrict(out.data(), n);
    return out;
}

struct Case
{
    size_t chunk;
    InnerMode mode;
};

class ReadBufferFromFileViewTest : public ::testing::TestWithParam<Case>
{
};

}

TEST_P(ReadBufferFromFileViewTest, SequentialReadWholeView)
{
    const auto [chunk, mode] = GetParam();
    const auto payload = makePayload(1000);
    auto view = makeView(payload, chunk, mode);

    EXPECT_EQ(readExact(*view, payload.size()), payload);
    EXPECT_TRUE(view->eof());
    EXPECT_EQ(view->getPosition(), static_cast<off_t>(payload.size()));
}

TEST_P(ReadBufferFromFileViewTest, SeekAndRead)
{
    const auto [chunk, mode] = GetParam();
    const auto payload = makePayload(1000);
    auto view = makeView(payload, chunk, mode);

    for (size_t target : {size_t(0), size_t(700), size_t(20), size_t(21), size_t(999), size_t(5)})
    {
        EXPECT_EQ(view->seek(target, SEEK_SET), static_cast<off_t>(target));
        EXPECT_EQ(view->getPosition(), static_cast<off_t>(target));
        EXPECT_EQ(readExact(*view, 1), payload.substr(target, 1));
        EXPECT_EQ(view->getPosition(), static_cast<off_t>(target + 1));
    }
}

/// B115 regression. The in-order MergeTree reader adjusts the right mark (setReadUntilPosition)
/// while the consumer is mid-buffer. A remote-like inner buffer legitimately discards its working
/// buffer on the range change; the view MUST keep reporting the consumer's position - before the
/// fix it teleported forward by the discarded bytes, so the next seek was treated as "already
/// there" and a stale block was re-served (duplicated + missing granules at the SQL level).
TEST_P(ReadBufferFromFileViewTest, SetReadUntilPositionMidBufferKeepsPosition)
{
    const auto [chunk, mode] = GetParam();
    const auto payload = makePayload(1000);
    auto view = makeView(payload, chunk, mode);

    EXPECT_EQ(readExact(*view, 36), payload.substr(0, 36));
    EXPECT_EQ(view->getPosition(), 36);

    view->setReadUntilPosition(72);
    EXPECT_EQ(view->getPosition(), 36) << "position must survive a right-bound change";

    /// The consumer's next seek to its current position must be a no-op...
    EXPECT_EQ(view->seek(36, SEEK_SET), 36);
    /// ...and the bytes must continue from 36, not from a stale buffer.
    EXPECT_EQ(readExact(*view, 36), payload.substr(36, 36));
}

/// Truncate-then-extend: the right bound shrinks below already-buffered data, the consumer reads
/// up to it, the bound is extended again. The continuation must produce the file's real bytes
/// (before the fix the view's incremental buffer-end accounting drifted from the inner buffer's).
TEST_P(ReadBufferFromFileViewTest, SetReadUntilTruncateThenExtend)
{
    const auto [chunk, mode] = GetParam();
    const auto payload = makePayload(1000);
    auto view = makeView(payload, chunk, mode);

    EXPECT_EQ(readExact(*view, 10), payload.substr(0, 10));

    view->setReadUntilPosition(30);
    EXPECT_EQ(view->getPosition(), 10);
    EXPECT_EQ(readExact(*view, 20), payload.substr(10, 20));
    EXPECT_TRUE(view->eof());
    EXPECT_EQ(view->getPosition(), 30);

    view->setReadUntilPosition(500);
    EXPECT_EQ(view->getPosition(), 30);
    EXPECT_EQ(readExact(*view, 100), payload.substr(30, 100));

    view->setReadUntilEnd();
    EXPECT_EQ(readExact(*view, payload.size() - 130), payload.substr(130));
    EXPECT_TRUE(view->eof());
}

/// The exact shape of the failing compact-part in-order read: per granule, adjust the right
/// mark, seek to the granule's block, read it. Every block must contain its own bytes.
TEST_P(ReadBufferFromFileViewTest, GranulePatternRegression)
{
    const auto [chunk, mode] = GetParam();
    constexpr size_t block = 36;
    constexpr size_t blocks = 20;
    const auto payload = makePayload(block * blocks);
    auto view = makeView(payload, chunk, mode);

    for (size_t g = 0; g < blocks; ++g)
    {
        view->setReadUntilPosition(std::min((g + 2) * block, payload.size()));
        EXPECT_EQ(view->seek(g * block, SEEK_SET), static_cast<off_t>(g * block));
        EXPECT_EQ(readExact(*view, block), payload.substr(g * block, block)) << "block " << g;
    }
}

/// Randomized conformance battery against a golden model.
TEST_P(ReadBufferFromFileViewTest, RandomizedOps)
{
    const auto [chunk, mode] = GetParam();
    const auto payload = makePayload(2000);

    for (unsigned seed = 1; seed <= 5; ++seed)
    {
        auto view = makeView(payload, chunk, mode);
        size_t model_pos = 0;
        size_t model_until = payload.size();
        unsigned rng = seed;
        auto next_rand = [&rng] { rng = rng * 1103515245 + 12345; return (rng >> 8) % 1000; };

        for (int step = 0; step < 300; ++step)
        {
            switch (next_rand() % 3)
            {
                case 0: /// read up to the current until-bound
                {
                    const size_t want = next_rand() % 64;
                    const size_t n = std::min(want, model_until - model_pos);
                    if (n)
                    {
                        ASSERT_EQ(readExact(*view, n), payload.substr(model_pos, n)) << "seed " << seed << " step " << step;
                        model_pos += n;
                    }
                    break;
                }
                case 1: /// seek (never beyond the current until-bound - the consumer contract:
                        /// the right mark always covers the ranges being read)
                {
                    const size_t target = next_rand() % (model_until + 1);
                    ASSERT_EQ(view->seek(target, SEEK_SET), static_cast<off_t>(target));
                    model_pos = target;
                    break;
                }
                case 2: /// move the right bound (never below the consumer position)
                {
                    const size_t until = model_pos + next_rand() % (payload.size() - model_pos + 1);
                    view->setReadUntilPosition(until);
                    model_until = until;
                    break;
                }
                default:
                    UNREACHABLE();
            }
            ASSERT_EQ(view->getPosition(), static_cast<off_t>(model_pos)) << "seed " << seed << " step " << step;
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    ChunksAndModes,
    ReadBufferFromFileViewTest,
    ::testing::Values(
        Case{7, InnerMode::FileLike},
        Case{7, InnerMode::RemoteLike},
        Case{108, InnerMode::FileLike},
        Case{108, InnerMode::RemoteLike},
        Case{1 << 20, InnerMode::FileLike},
        Case{1 << 20, InnerMode::RemoteLike}));
