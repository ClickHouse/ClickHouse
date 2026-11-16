#include <gtest/gtest.h>

#include <string>
#include <vector>

#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/Optimizations/SipHashingWriteBuffer.h>
#include <Common/SipHash.h>

using namespace DB;

namespace
{

/// The value `calculateHashFromStep` produced before it streamed: accumulate the whole stream, then
/// hash the contiguous result in one call.
UInt64 bufferedKey(const std::vector<std::string> & writes)
{
    WriteBufferFromOwnString out;
    for (const auto & w : writes)
        out.write(w.data(), w.size());

    SipHash hash;
    hash.update(out.str());
    return hash.get64();
}

/// The value it produces now: hash each chunk as it is written, keeping no copy of the stream.
UInt64 streamedKey(const std::vector<std::string> & writes)
{
    SipHash hash;
    SipHashingWriteBuffer out(hash);
    for (const auto & w : writes)
        out.write(w.data(), w.size());
    out.finalize();
    return hash.get64();
}

/// Writes that between them cross the sink's flush boundary at every alignment: sub-word chunks
/// (which exercise `SipHash::update`'s carried remainder), chunks straddling one window, and a
/// single write many windows long.
std::vector<std::vector<std::string>> streamShapes()
{
    const size_t w = SipHashingWriteBuffer::window_bytes;
    return {
        {},
        {""},
        {"a"},
        {"a", "b", "c"},
        {std::string(7, 'x')},
        {std::string(8, 'x')},
        {std::string(9, 'x')},
        {std::string(1, 'a'), std::string(w - 1, 'b')},
        {std::string(w - 1, 'a'), std::string(1, 'b')},
        {std::string(w, 'a')},
        {std::string(w, 'a'), std::string(1, 'b')},
        {std::string(w + 1, 'a')},
        {std::string(3, 'a'), std::string(w, 'b'), std::string(5, 'c')},
        {std::string(4 * w + 3, 'a')},
    };
}

}

/// The property the whole change rests on: streaming the key must not change its value. If this
/// fails, every cached hash-table-statistics entry and every parallel-replicas node match silently
/// stops agreeing with the plan that produced it.
TEST(SipHashingWriteBuffer, KeyIsIdenticalToBufferedHash)
{
    for (const auto & writes : streamShapes())
        ASSERT_EQ(streamedKey(writes), bufferedKey(writes)) << "stream shape with " << writes.size() << " write(s)";
}

/// Negative control for the test above. Perturbing the stream by a single byte in one shape must
/// change the key, otherwise `KeyIsIdenticalToBufferedHash` would pass for a sink that hashes
/// nothing at all.
TEST(SipHashingWriteBuffer, KeyDiffersWhenStreamIsPerturbed)
{
    const std::vector<std::string> writes{std::string(SipHashingWriteBuffer::window_bytes + 1, 'a')};

    auto perturbed = writes;
    perturbed.front().back() = 'b';

    ASSERT_NE(streamedKey(writes), streamedKey(perturbed));
    ASSERT_NE(streamedKey(writes), streamedKey({}));
}

/// Destroying the sink with a stream still half-written must not trip `~WriteBuffer`'s "neither
/// finalized nor canceled" assertion, since callers are free to abandon a key half-computed.
TEST(SipHashingWriteBuffer, DestroyedWithoutFinalizeAfterPartialWrite)
{
    SipHash hash;
    {
        SipHashingWriteBuffer out(hash);
        const std::string chunk(SipHashingWriteBuffer::window_bytes + 17, 'x');
        out.write(chunk.data(), chunk.size());
    }
}
