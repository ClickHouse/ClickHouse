#include <DataTypes/Serializations/SerializationMapPresence.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

String serializeBlock(size_t rows, const std::vector<std::vector<UInt8>> & presence)
{
    WriteBufferFromOwnString buffer;
    MapKeyPresenceBlock::serialize(buffer, rows, presence);
    buffer.finalize();
    return buffer.str();
}

std::vector<UInt8> makeConstant(size_t rows, UInt8 value)
{
    return std::vector<UInt8>(rows, value);
}

std::vector<UInt8> makeMixed(size_t rows)
{
    std::vector<UInt8> presence(rows, 0);
    for (size_t i = 0; i < rows; ++i)
        presence[i] = static_cast<UInt8>(i % 2);
    return presence;
}

void expectRoundTrip(size_t rows, const std::vector<std::vector<UInt8>> & presence)
{
    const auto bytes = serializeBlock(rows, presence);
    ReadBufferFromString all(bytes);
    std::vector<std::vector<UInt8>> decoded;
    size_t bitmap_bytes = 0;
    MapKeyPresenceBlock::deserializeAll(all, rows, presence.size(), decoded, &bitmap_bytes);
    ASSERT_EQ(decoded.size(), presence.size());
    for (size_t i = 0; i < presence.size(); ++i)
        EXPECT_EQ(decoded[i], presence[i]) << "key " << i;

    for (size_t i = 0; i < presence.size(); ++i)
    {
        ReadBufferFromString one(bytes);
        std::vector<UInt8> one_decoded;
        size_t one_bitmap_bytes = 0;
        MapKeyPresenceBlock::deserializeKey(one, rows, presence.size(), i, one_decoded, &one_bitmap_bytes);
        EXPECT_EQ(one_decoded, presence[i]) << "single key " << i;

        ReadBufferFromString kind_in(bytes);
        const UInt8 kind = MapKeyPresenceBlock::deserializeKind(kind_in, presence.size(), i);
        if (kind != MapKeyPresenceBlock::KIND_MIXED)
            EXPECT_EQ(one_bitmap_bytes, 0u) << "kind " << static_cast<UInt32>(kind) << " must not read bitmaps";
    }
}

}

TEST(MapWithKeyColumnsPresence, KindRoundTrip)
{
    constexpr size_t rows = 16;
    expectRoundTrip(rows, {makeConstant(rows, 1), makeConstant(rows, 0), makeMixed(rows)});
}

TEST(MapWithKeyColumnsPresence, GranuleSizes)
{
    for (size_t rows : {size_t(1), size_t(7), size_t(8), size_t(9), size_t(8192)})
    {
        SCOPED_TRACE(rows);
        expectRoundTrip(rows, {makeConstant(rows, 1), makeMixed(rows), makeConstant(rows, 0)});
    }
}

TEST(MapWithKeyColumnsPresence, KeyCounts)
{
    constexpr size_t rows = 8;
    for (size_t keys : {size_t(1), size_t(2), size_t(64), size_t(4096)})
    {
        SCOPED_TRACE(keys);
        std::vector<std::vector<UInt8>> presence;
        presence.reserve(keys);
        for (size_t i = 0; i < keys; ++i)
        {
            if (i % 3 == 0)
                presence.push_back(makeConstant(rows, 1));
            else if (i % 3 == 1)
                presence.push_back(makeConstant(rows, 0));
            else
                presence.push_back(makeMixed(rows));
        }
        expectRoundTrip(rows, presence);
    }
}

TEST(MapWithKeyColumnsPresence, KindZeroAndOneSkipBitmap)
{
    constexpr size_t rows = 9;
    const auto presence = std::vector<std::vector<UInt8>>{makeConstant(rows, 0), makeConstant(rows, 1), makeMixed(rows)};
    auto bytes = serializeBlock(rows, presence);

    /// Keep only the kind array. Kind 0/1 must still succeed; kind 2 must fail.
    bytes.resize(MapKeyPresenceBlock::kindArrayBytes(presence.size()));

    {
        ReadBufferFromString in(bytes);
        std::vector<UInt8> out;
        size_t bitmap_bytes = 0;
        MapKeyPresenceBlock::deserializeKey(in, rows, presence.size(), 0, out, &bitmap_bytes);
        EXPECT_EQ(out, makeConstant(rows, 0));
        EXPECT_EQ(bitmap_bytes, 0u);
    }

    {
        ReadBufferFromString in(bytes);
        std::vector<UInt8> out;
        size_t bitmap_bytes = 0;
        MapKeyPresenceBlock::deserializeKey(in, rows, presence.size(), 1, out, &bitmap_bytes);
        EXPECT_EQ(out, makeConstant(rows, 1));
        EXPECT_EQ(bitmap_bytes, 0u);
    }

    {
        ReadBufferFromString in(bytes);
        std::vector<UInt8> out;
        EXPECT_THROW(MapKeyPresenceBlock::deserializeKey(in, rows, presence.size(), 2, out), Exception);
    }
}
