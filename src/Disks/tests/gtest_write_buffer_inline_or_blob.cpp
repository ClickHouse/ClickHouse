#include <gtest/gtest.h>

#include <Disks/IO/WriteBufferInlineOrBlob.h>
#include <IO/WriteBufferFromFileBase.h>

#include <fmt/format.h>

#include <optional>

using namespace DB;

namespace
{

/// Test double for the blob write path: appends into a shared string and records lifecycle calls.
class WriteBufferToSharedString final : public WriteBufferFromFileBase
{
public:
    WriteBufferToSharedString(String & target_, bool & finalized_flag_)
        : WriteBufferFromFileBase(64, nullptr, 0), target(target_), finalized_flag(finalized_flag_)
    {
    }

    ~WriteBufferToSharedString() override
    {
        cancel();
    }

    void sync() override {}
    std::string getFileName() const override { return "shared_string"; }

private:
    void nextImpl() override { target.append(working_buffer.begin(), offset()); }

    void finalizeImpl() override
    {
        next();
        finalized_flag = true;
    }

    String & target;
    bool & finalized_flag;
};

struct InlineBufferHarness
{
    String blob_content;
    bool blob_finalized = false;
    size_t underlying_created = 0;
    std::optional<String> inline_content;
    std::optional<size_t> blob_bytes;

    std::unique_ptr<WriteBufferInlineOrBlob> makeBuffer(size_t max_inline_bytes, size_t buf_size = 16, bool create_blob_if_empty = true)
    {
        return std::make_unique<WriteBufferInlineOrBlob>(
            "test_file",
            max_inline_bytes,
            create_blob_if_empty,
            [this]() -> std::unique_ptr<WriteBuffer>
            {
                ++underlying_created;
                return std::make_unique<WriteBufferToSharedString>(blob_content, blob_finalized);
            },
            [this](FinalizeResult result)
            {
                if (auto * inline_data = std::get_if<InlineData>(&result))
                    inline_content = std::move(inline_data->data);
                else
                    blob_bytes = std::get<WrittenBlob>(result).bytes_count;
            },
            buf_size);
    }
};

}

TEST(WriteBufferInlineOrBlob, SmallContentStaysInline)
{
    InlineBufferHarness harness;
    auto buf = harness.makeBuffer(/*max_inline_bytes=*/10);
    buf->write("hello", 5);
    buf->finalize();

    ASSERT_TRUE(harness.inline_content.has_value());
    EXPECT_EQ(*harness.inline_content, "hello");
    EXPECT_EQ(harness.underlying_created, 0u);
}

TEST(WriteBufferInlineOrBlob, EmptyContentStaysInline)
{
    InlineBufferHarness harness;
    auto buf = harness.makeBuffer(/*max_inline_bytes=*/10);
    buf->finalize();

    ASSERT_TRUE(harness.inline_content.has_value());
    EXPECT_EQ(*harness.inline_content, "");
    EXPECT_EQ(harness.underlying_created, 0u);
}

TEST(WriteBufferInlineOrBlob, ExactThresholdStaysInline)
{
    InlineBufferHarness harness;
    auto buf = harness.makeBuffer(/*max_inline_bytes=*/5);
    buf->write("12345", 5);
    buf->finalize();

    ASSERT_TRUE(harness.inline_content.has_value());
    EXPECT_EQ(*harness.inline_content, "12345");
    EXPECT_EQ(harness.underlying_created, 0u);
}

TEST(WriteBufferInlineOrBlob, OneBytePastThresholdSpills)
{
    InlineBufferHarness harness;
    auto buf = harness.makeBuffer(/*max_inline_bytes=*/5);
    buf->write("123456", 6);
    buf->finalize();

    EXPECT_FALSE(harness.inline_content.has_value());
    EXPECT_EQ(harness.underlying_created, 1u);
    EXPECT_EQ(harness.blob_content, "123456");
    EXPECT_TRUE(harness.blob_finalized);
    EXPECT_EQ(harness.blob_bytes, 6u);
}

TEST(WriteBufferInlineOrBlob, SpillMidStreamPreservesAllContent)
{
    /// Cross the threshold gradually with a tiny internal buffer so the spill happens between writes.
    InlineBufferHarness harness;
    auto buf = harness.makeBuffer(/*max_inline_bytes=*/20, /*buf_size=*/4);
    String expected;
    for (size_t i = 0; i < 10; ++i)
    {
        String chunk = fmt::format("<{}>", i);
        buf->write(chunk.data(), chunk.size());
        expected += chunk;
    }
    buf->finalize();

    EXPECT_FALSE(harness.inline_content.has_value());
    EXPECT_EQ(harness.underlying_created, 1u);
    EXPECT_EQ(harness.blob_content, expected);
    EXPECT_TRUE(harness.blob_finalized);
}

TEST(WriteBufferInlineOrBlob, CancelCommitsNothing)
{
    InlineBufferHarness harness;
    auto buf = harness.makeBuffer(/*max_inline_bytes=*/10);
    buf->write("hello", 5);
    buf->cancel();

    EXPECT_FALSE(harness.inline_content.has_value());
    EXPECT_FALSE(harness.blob_bytes.has_value());
    EXPECT_EQ(harness.underlying_created, 0u);
}

TEST(WriteBufferInlineOrBlob, StagingBufferStaysSmall)
{
    /// One such buffer exists per column stream of a wide part, so it must not allocate the full
    /// requested buffer size (that would defeat the adaptive write buffer sizing of the writers).
    InlineBufferHarness harness;
    auto buf = harness.makeBuffer(/*max_inline_bytes=*/1024, /*buf_size=*/DBMS_DEFAULT_BUFFER_SIZE);
    EXPECT_EQ(buf->internalBuffer().size(), DBMS_DEFAULT_INITIAL_ADAPTIVE_BUFFER_SIZE);
    buf->cancel();

    auto big_threshold = harness.makeBuffer(/*max_inline_bytes=*/65536, /*buf_size=*/DBMS_DEFAULT_BUFFER_SIZE);
    EXPECT_EQ(big_threshold->internalBuffer().size(), 65536u);
    big_threshold->cancel();

    auto small_requested = harness.makeBuffer(/*max_inline_bytes=*/0, /*buf_size=*/16);
    EXPECT_EQ(small_requested->internalBuffer().size(), 16u);
    small_requested->cancel();
}

TEST(WriteBufferInlineOrBlob, ZeroThresholdBuildsStackAtConstruction)
{
    InlineBufferHarness harness;
    auto buf = harness.makeBuffer(/*max_inline_bytes=*/0);
    EXPECT_EQ(harness.underlying_created, 1u);
    buf->cancel();
}

TEST(WriteBufferInlineOrBlob, ZeroThresholdWritesBlob)
{
    InlineBufferHarness harness;
    auto buf = harness.makeBuffer(/*max_inline_bytes=*/0);
    buf->write("hello", 5);
    buf->finalize();

    EXPECT_FALSE(harness.inline_content.has_value());
    EXPECT_EQ(harness.underlying_created, 1u);
    EXPECT_EQ(harness.blob_content, "hello");
    EXPECT_TRUE(harness.blob_finalized);
    EXPECT_EQ(harness.blob_bytes, 5u);
}

TEST(WriteBufferInlineOrBlob, ZeroThresholdEmptyCreatesBlobWhenRequired)
{
    InlineBufferHarness harness;
    auto buf = harness.makeBuffer(/*max_inline_bytes=*/0, /*buf_size=*/16, /*create_blob_if_empty=*/true);
    buf->finalize();

    EXPECT_FALSE(harness.inline_content.has_value());
    EXPECT_EQ(harness.underlying_created, 1u);
    EXPECT_TRUE(harness.blob_finalized);
    EXPECT_EQ(harness.blob_bytes, 0u);
}

TEST(WriteBufferInlineOrBlob, ZeroThresholdEmptyCancelsBlobWhenNotRequired)
{
    InlineBufferHarness harness;
    auto buf = harness.makeBuffer(/*max_inline_bytes=*/0, /*buf_size=*/16, /*create_blob_if_empty=*/false);
    buf->finalize();

    EXPECT_FALSE(harness.inline_content.has_value());
    EXPECT_EQ(harness.underlying_created, 1u);
    EXPECT_FALSE(harness.blob_finalized);
    EXPECT_EQ(harness.blob_bytes, 0u);
}

TEST(WriteBufferInlineOrBlob, PreFinalizeThenFinalizeInline)
{
    InlineBufferHarness harness;
    auto buf = harness.makeBuffer(/*max_inline_bytes=*/10);
    buf->write("hello", 5);
    buf->preFinalize();
    buf->finalize();

    ASSERT_TRUE(harness.inline_content.has_value());
    EXPECT_EQ(*harness.inline_content, "hello");
}
