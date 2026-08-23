#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <cstring>
#include <vector>

#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>

#include <gtest/gtest.h>

namespace
{

/// A body stream that reports a `Content-Length` unrelated to the amount of data it holds,
/// the way a malicious or broken remote endpoint can.
class LyingBodyStream : public Azure::Core::IO::BodyStream
{
public:
    LyingBodyStream(std::vector<uint8_t> data_, int64_t reported_length_)
        : data(std::move(data_)), reported_length(reported_length_)
    {
    }

    int64_t Length() const override { return reported_length; }

    void Rewind() override { position = 0; }

private:
    size_t OnRead(uint8_t * buffer, size_t count, const Azure::Core::Context &) override
    {
        const size_t to_read = std::min(count, data.size() - position);
        memcpy(buffer, data.data() + position, to_read);
        position += to_read;
        return to_read;
    }

    std::vector<uint8_t> data;
    int64_t reported_length;
    size_t position = 0;
};

}

/// The endpoint returns more data than was requested: the copy must be capped at the size of
/// the destination buffer instead of overflowing it.
TEST(AzureBodyStreamCopy, OverlongResponse)
{
    const size_t requested = 8;
    LyingBodyStream body_stream(std::vector<uint8_t>(1024, 0xAB), 1024);

    std::vector<char> destination(requested);
    const size_t copied = DB::copyFromAzureBodyStream(body_stream, destination.data(), requested, Azure::Core::Context());

    ASSERT_EQ(copied, requested);
    for (char byte : destination)
        ASSERT_EQ(static_cast<uint8_t>(byte), 0xAB);
}

/// The endpoint reports a large `Content-Length` but sends fewer bytes: only the received bytes
/// are copied and reported.
TEST(AzureBodyStreamCopy, ShortResponse)
{
    const size_t requested = 8;
    LyingBodyStream body_stream(std::vector<uint8_t>(3, 0xCD), 1024);

    std::vector<char> destination(requested);
    const size_t copied = DB::copyFromAzureBodyStream(body_stream, destination.data(), requested, Azure::Core::Context());

    ASSERT_EQ(copied, 3);
}

/// A stream of unknown length must not be copied beyond the destination buffer either.
TEST(AzureBodyStreamCopy, UnknownLength)
{
    const size_t requested = 4;
    LyingBodyStream body_stream(std::vector<uint8_t>(64, 0xEF), -1);

    std::vector<char> destination(requested);
    const size_t copied = DB::copyFromAzureBodyStream(body_stream, destination.data(), requested, Azure::Core::Context());

    ASSERT_EQ(copied, requested);
}

#endif
