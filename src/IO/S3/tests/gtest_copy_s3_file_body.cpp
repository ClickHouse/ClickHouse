#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3

#include <IO/S3/copyS3File.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/StdStreamFromReadBuffer.h>

#include <Common/Exception.h>

#include <memory>
#include <string>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{

/// A seekable source that serves `data` once and then becomes unreadable: after the first full
/// pass it throws from nextImpl() on any further read. This models a live source that cannot be
/// re-read on an upload retry (a stale ReadBufferFromS3 whose connection is gone, the canceled
/// source from the original crash). It also records how much was read so the test can prove the
/// production body reads the source exactly once.
///
/// createS3UploadBody must copy the whole part into memory up front, so the S3 request body it
/// returns has no inner source buffer and survives any number of SDK rewinds (clear(); seekg(0)).
/// If the production code regressed to attaching a LimitSeekableReadBuffer straight over the
/// source, the SDK rewind would re-read this source and trip the throw below, failing the test.
class ReadOnceThenFailSource : public SeekableReadBuffer
{
public:
    explicit ReadOnceThenFailSource(std::string data_, size_t * bytes_read_)
        : SeekableReadBuffer(nullptr, 0), data(std::move(data_)), bytes_read(bytes_read_)
    {
    }

    off_t seek(off_t off, int whence) override
    {
        if (whence != SEEK_SET)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Only SEEK_SET supported in test source");
        /// Seeking backwards into already-served data is what a streaming-body retry would do.
        /// This source refuses it: once consumed, it cannot be rewound and reread.
        if (static_cast<size_t>(off) < read_offset)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Source cannot be rewound after it was read");
        read_offset = static_cast<size_t>(off);
        set(nullptr, 0);
        return off;
    }

    off_t getPosition() override { return static_cast<off_t>(read_offset) - static_cast<off_t>(available()); }

private:
    std::string data;
    size_t * bytes_read;
    size_t read_offset = 0;
    char one_byte = 0;

    bool nextImpl() override
    {
        if (read_offset >= data.size())
            return false;

        /// Serve a single byte at a time so a partial reread is also caught.
        one_byte = data[read_offset];
        ++read_offset;
        ++*bytes_read;
        BufferBase::set(&one_byte, 1, 0);
        return true;
    }
};

/// Reads the whole body the way the AWS SDK does on a successful send, then returns what it got.
std::string drainBody(StdStreamFromReadBuffer & body, size_t size)
{
    std::string out(size, '\0');
    body.read(out.data(), static_cast<std::streamsize>(size));
    out.resize(static_cast<size_t>(body.gcount()));
    return out;
}

}

/// Regression test for the backup-to-S3 upload retry crash:
///   "ReadBuffer is canceled. Can't read from it." (chassert in ReadBuffer::next()).
///
/// This drives the production request-body builder createS3UploadBody (used by both
/// CopyDataToFileHelper::fillPutRequest and makeUploadPartRequest). It asserts the body is a
/// self-contained in-memory copy: the source is read exactly once, and the body can be rewound
/// and reread (as a retrying SDK does) even after the source has become unreadable. A regression
/// that puts a LimitSeekableReadBuffer straight on the request would re-read the source on the
/// rewind and fail here.
TEST(CopyS3FileBody, BodyIsMemoryBackedAndSurvivesRewind)
{
    const std::string payload = "the quick brown fox jumps over the lazy dog";
    const size_t offset = 0;
    const size_t size = payload.size();

    size_t source_bytes_read = 0;
    size_t source_factory_calls = 0;

    /// The CreateReadBuffer the production code is given. The source it returns can be read
    /// exactly once; any rewind+reread of the source throws.
    CreateReadBuffer create_read_buffer = [&]() -> std::unique_ptr<SeekableReadBuffer>
    {
        ++source_factory_calls;
        return std::make_unique<ReadOnceThenFailSource>(payload, &source_bytes_read);
    };

    /// Build the body via the real production path. With the in-memory body this reads the source
    /// once into an owned string; a LimitSeekableReadBuffer body would defer reads to send time.
    auto body = createS3UploadBody(create_read_buffer, offset, size);
    ASSERT_TRUE(body);

    /// The SDK reads the body, then rewinds and rereads it across retry attempts
    /// (GetContentBody()->clear(); seekg(0)). Repeat that cycle several times.
    for (int attempt = 0; attempt < 3; ++attempt)
    {
        body->clear();
        body->seekg(0);
        EXPECT_EQ(drainBody(*body, size), payload) << "rewind attempt " << attempt;
    }

    /// The body is memory-backed: the source was opened once and read exactly `size` bytes total,
    /// regardless of how many times the body was rewound and resent. If the request body were a
    /// live LimitSeekableReadBuffer over the source, the rewinds above would have re-read (and
    /// here, failed on) the source.
    EXPECT_EQ(source_factory_calls, 1u);
    EXPECT_EQ(source_bytes_read, size);
}

/// The same property for a part that starts at a non-zero offset (the multipart UploadPart path,
/// makeUploadPartRequest, builds the body for [part_offset, part_offset + part_size)).
TEST(CopyS3FileBody, BodyForPartAtOffsetIsMemoryBacked)
{
    const std::string source_data(256, 'x');
    const std::string part(64, 'x');
    const size_t offset = 64;
    const size_t size = part.size();

    size_t source_bytes_read = 0;
    size_t source_factory_calls = 0;

    CreateReadBuffer create_read_buffer = [&]() -> std::unique_ptr<SeekableReadBuffer>
    {
        ++source_factory_calls;
        return std::make_unique<ReadOnceThenFailSource>(source_data, &source_bytes_read);
    };

    auto body = createS3UploadBody(create_read_buffer, offset, size);
    ASSERT_TRUE(body);

    for (int attempt = 0; attempt < 3; ++attempt)
    {
        body->clear();
        body->seekg(0);
        EXPECT_EQ(drainBody(*body, size), part) << "rewind attempt " << attempt;
    }

    EXPECT_EQ(source_factory_calls, 1u);
    /// LimitSeekableReadBuffer reaches `offset` by seeking the source (not by reading), so only
    /// the `size` bytes of the part are read. The point is the source is read ONCE: rewinding the
    /// body rereads nothing from the source.
    EXPECT_EQ(source_bytes_read, size);
}

#endif
