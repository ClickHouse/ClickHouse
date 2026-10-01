#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include <IO/SipHashingWriteBuffer.h>
#include <IO/WriteBufferFromString.h>
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

/// The same two arms read as the two words `hash128` callers consume, entered with `seed` already
/// hashed. Both getters finalize the hash, so each arm needs its own instance.
using Key128 = std::pair<UInt64, UInt64>;

Key128 bufferedKey128(const std::string & seed, const std::vector<std::string> & writes)
{
    WriteBufferFromOwnString out;
    for (const auto & w : writes)
        out.write(w.data(), w.size());

    SipHash hash;
    hash.update(seed);
    hash.update(out.str());

    Key128 key;
    hash.get128(key.first, key.second);
    return key;
}

Key128 streamedKey128(const std::string & seed, const std::vector<std::string> & writes, size_t window_size, char * window)
{
    SipHash hash;
    hash.update(seed);
    {
        SipHashingWriteBuffer out(hash, window_size, window);
        for (const auto & w : writes)
            out.write(w.data(), w.size());
        out.finalize();
    }

    Key128 key;
    hash.get128(key.first, key.second);
    return key;
}

/// Writes that between them cross a window of `w` bytes at every alignment: sub-word chunks
/// (which exercise `SipHash::update`'s carried remainder), chunks straddling one window, and a
/// single write many windows long.
std::vector<std::vector<std::string>> streamShapes(size_t w)
{
    return {
        {},
        {""},
        {"a"},
        {"a", "b", "c"},
        {std::string(7, 'x')},
        {std::string(8, 'x')},
        {std::string(9, 'x')},
        {std::string(w - 1, 'a')},
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
    for (const auto & writes : streamShapes(SipHashingWriteBuffer::window_bytes))
        ASSERT_EQ(streamedKey(writes), bufferedKey(writes)) << "stream shape with " << writes.size() << " write(s)";
}

/// `ColumnAggregateFunction::updateHashWithValue` uses the other ctor: a window it owns on the stack,
/// small enough that ordinary values flush it repeatedly, and it is entered from `hash128`, which
/// feeds one key column after another into the same `SipHash`. The test above covers neither, since
/// every shape there gets the default window and a hash with nothing in it yet.
TEST(SipHashingWriteBuffer, KeyIsIdenticalOverACallerOwnedWindow)
{
    char window[64];

    for (const auto & writes : streamShapes(sizeof(window)))
    {
        /// A seed whose length is not a multiple of the word size, so the first written byte lands on
        /// the remainder `SipHash::update` carries rather than on a word boundary.
        for (const std::string & seed : {std::string(), std::string("abc")})
            ASSERT_EQ(streamedKey128(seed, writes, sizeof(window), window), bufferedKey128(seed, writes))
                << "stream shape with " << writes.size() << " write(s), seed of " << seed.size() << " byte(s)";
    }
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
