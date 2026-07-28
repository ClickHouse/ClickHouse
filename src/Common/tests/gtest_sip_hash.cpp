#include <Common/SipHash.h>

#include <cstdlib>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

TEST(SipHash, UpdateWithEmptyStringViewDoesNotInvokeUB)
{
    std::string_view empty;

    SipHash hash;
    hash.update(empty);
    hash.update(std::string_view{"Interval"});
    hash.update(empty);
    (void)hash.get128();
}

TEST(SipHash, UpdateWithEmptyStringViewIsNoOp)
{
    const std::string_view payload{"Interval"};

    SipHash baseline;
    baseline.update(payload);
    UInt128 baseline_value = baseline.get128();

    SipHash with_empty;
    with_empty.update(std::string_view{});
    with_empty.update(payload);
    with_empty.update(std::string_view{});
    UInt128 with_empty_value = with_empty.get128();

    EXPECT_EQ(baseline_value, with_empty_value);
}

namespace
{
UInt128 hashWhole(const char * data, size_t size)
{
    SipHash hash;
    hash.update(data, size);
    return hash.get128();
}

UInt128 hashByteByByte(const char * data, size_t size)
{
    SipHash hash;
    for (size_t i = 0; i < size; ++i)
        hash.update(data + i, 1);
    return hash.get128();
}
}

/// Buffers are allocated at exactly their length, so a 1-7 byte tail sits at the allocation end:
/// the unrolled loop must read it without forming a pointer past end (ASan catches an over-read).
TEST(SipHash, UpdateBoundaryLengths)
{
    for (size_t size = 0; size <= 64; ++size)
    {
        std::vector<char> buffer(size == 0 ? 1 : size);
        for (size_t i = 0; i < size; ++i)
            buffer[i] = static_cast<char>(i * 31 + 7);

        /// Streaming one byte at a time must equal hashing the whole buffer in one update.
        EXPECT_EQ(hashWhole(buffer.data(), size), hashByteByByte(buffer.data(), size))
            << "size=" << size;
    }
}

/// Splitting an update at every chunk size drives the cnt&7 remainder path into the loop.
TEST(SipHash, UpdateChunkedMatchesWhole)
{
    const std::string payload = "The quick brown fox jumps over the lazy dog 0123456789";
    const UInt128 whole = hashWhole(payload.data(), payload.size());

    for (size_t chunk = 1; chunk <= payload.size(); ++chunk)
    {
        SipHash hash;
        for (size_t offset = 0; offset < payload.size(); offset += chunk)
            hash.update(payload.data() + offset, std::min(chunk, payload.size() - offset));
        EXPECT_EQ(whole, hash.get128()) << "chunk=" << chunk;
    }
}
