#include <Common/HashTable/StringHashMap.h>
#include <Common/HashTable/TwoLevelStringHashMap.h>

#include <Columns/ColumnString.h>

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

using namespace DB;

namespace
{

/// Keys covering every size class of the compound string table: the empty key (m0), every length
/// from 1 to 30 (m1: 1-8 bytes, m2: 9-16, m3: 17-24, ms: 25+), keys whose trailing zero byte
/// forces them into the long-string submap regardless of length, and a key with an embedded
/// (non-trailing) zero byte, which stays in its fixed-size class.
ColumnString::MutablePtr testKeys()
{
    auto keys = ColumnString::create();
    keys->insertDefault();
    for (size_t len = 1; len <= 30; ++len)
    {
        std::string key(len, ' ');
        for (size_t i = 0; i < len; ++i)
            key[i] = static_cast<char>('a' + (len + i) % 26);
        keys->insertData(key.data(), key.size());
    }
    for (size_t len : {3, 8, 16, 24})
    {
        std::string key(len - 1, 'z');
        key += '\0';
        keys->insertData(key.data(), key.size());
    }
    keys->insertData("a\0b", 3);
    return keys;
}

}

/// The canonical hash returned by `hash` must be exactly the hash the plain dispatch routes by:
/// a key inserted through the ordinary emplace is found by the supplied-hash find, at any length
/// class. The supplied-hash prefetch must accept the same (key, hash) pair.
TEST(StringHashTableSuppliedHash, FindWithCanonicalHash)
{
    using Map = StringHashMap<UInt64>;
    Map map;

    const auto keys = testKeys();
    for (size_t i = 0; i < keys->size(); ++i)
    {
        Map::LookupResult it;
        bool inserted = false;
        map.emplace(keys->getDataAt(i), it, inserted);
        ASSERT_TRUE(inserted);
        it->getMapped() = i;
    }
    ASSERT_EQ(map.size(), keys->size());

    for (size_t i = 0; i < keys->size(); ++i)
    {
        const std::string_view key = keys->getDataAt(i);
        const size_t hash = map.hash(key);

        map.prefetch(key, hash);

        auto found = map.find(key, hash);
        ASSERT_NE(found, nullptr) << "key size " << key.size();
        ASSERT_EQ(found->getMapped(), i) << "key size " << key.size();
    }
}

/// The reverse direction: keys inserted through the supplied-hash emplace occupy the same slots
/// the plain dispatch probes — the ordinary find sees every one of them, re-inserting through
/// either path deduplicates, and the map size matches the distinct key count.
TEST(StringHashTableSuppliedHash, EmplaceWithSuppliedHash)
{
    using Map = StringHashMap<UInt64>;
    Map map;

    const auto keys = testKeys();
    for (size_t i = 0; i < keys->size(); ++i)
    {
        const std::string_view key = keys->getDataAt(i);
        Map::LookupResult it;
        bool inserted = false;
        map.emplace(key, it, inserted, map.hash(key));
        ASSERT_TRUE(inserted) << "key size " << key.size();
        it->getMapped() = i;
    }
    ASSERT_EQ(map.size(), keys->size());

    for (size_t i = 0; i < keys->size(); ++i)
    {
        const std::string_view key = keys->getDataAt(i);

        auto found = map.find(key);
        ASSERT_NE(found, nullptr) << "key size " << key.size();
        ASSERT_EQ(found->getMapped(), i) << "key size " << key.size();

        Map::LookupResult it;
        bool inserted = false;
        map.emplace(key, it, inserted);
        ASSERT_FALSE(inserted) << "key size " << key.size();
        ASSERT_EQ(it->getMapped(), i);

        map.emplace(key, it, inserted, map.hash(key));
        ASSERT_FALSE(inserted) << "key size " << key.size();
        ASSERT_EQ(it->getMapped(), i);
    }
}

/// On the two-level table the supplied hash also selects the bucket. A key inserted through the
/// plain dispatch must be owned by exactly the bucket `getBucketFromHash` derives from the
/// canonical hash, and be reachable through the supplied-hash find; no other bucket may hold it.
TEST(StringHashTableSuppliedHash, TwoLevelBucketRouting)
{
    using Map = TwoLevelStringHashMap<UInt64>;
    /// The two-level table holds its 256 sub-tables inline and does not fit a stack frame.
    auto map_holder = std::make_unique<Map>();
    auto & map = *map_holder;

    const auto keys = testKeys();
    for (size_t i = 0; i < keys->size(); ++i)
    {
        Map::LookupResult it;
        bool inserted = false;
        map.emplace(keys->getDataAt(i), it, inserted);
        ASSERT_TRUE(inserted);
        it->getMapped() = i;
    }

    for (size_t i = 0; i < keys->size(); ++i)
    {
        const std::string_view key = keys->getDataAt(i);
        const size_t hash = map.hash(key);

        auto found = map.find(key, hash);
        ASSERT_NE(found, nullptr) << "key size " << key.size();
        ASSERT_EQ(found->getMapped(), i) << "key size " << key.size();

        size_t owners = 0;
        for (auto & impl : map.impls)
            owners += impl.find(key) != nullptr;
        ASSERT_EQ(owners, 1) << "key size " << key.size();
        ASSERT_NE(map.impls[Map::getBucketFromHash(hash)].find(key, hash), nullptr) << "key size " << key.size();
    }
}

/// Supplied-hash emplace into the two-level table must land the key in the canonical bucket's
/// submap, where the one-level plain dispatch finds it.
TEST(StringHashTableSuppliedHash, TwoLevelEmplaceWithSuppliedHash)
{
    using Map = TwoLevelStringHashMap<UInt64>;
    /// The two-level table holds its 256 sub-tables inline and does not fit a stack frame.
    auto map_holder = std::make_unique<Map>();
    auto & map = *map_holder;

    const auto keys = testKeys();
    for (size_t i = 0; i < keys->size(); ++i)
    {
        const std::string_view key = keys->getDataAt(i);
        Map::LookupResult it;
        bool inserted = false;
        map.emplace(key, it, inserted, map.hash(key));
        ASSERT_TRUE(inserted) << "key size " << key.size();
        it->getMapped() = i;
    }

    for (size_t i = 0; i < keys->size(); ++i)
    {
        const std::string_view key = keys->getDataAt(i);
        auto found = map.impls[Map::getBucketFromHash(map.hash(key))].find(key);
        ASSERT_NE(found, nullptr) << "key size " << key.size();
        ASSERT_EQ(found->getMapped(), i) << "key size " << key.size();
    }
}

/// `usesStringViewSubmap` identifies exactly the keys the dispatch stores as raw strings: for
/// them the canonical hash is the plain hash of the key bytes (what a caller staging keys for a
/// later drain saves and reuses), and reserving long-string room ahead of their insertion keeps
/// them all findable.
TEST(StringHashTableSuppliedHash, LongStringClassification)
{
    using Map = StringHashMap<UInt64>;
    Map map;
    map.reserveAdditionalStringViewKeys(1000);

    const auto keys = testKeys();
    size_t long_keys = 0;
    for (size_t i = 0; i < keys->size(); ++i)
    {
        const std::string_view key = keys->getDataAt(i);
        if (Map::usesStringViewSubmap(key))
        {
            ++long_keys;
            ASSERT_EQ(map.hash(key), StringHashTableHash{}(key)) << "key size " << key.size();
        }

        Map::LookupResult it;
        bool inserted = false;
        map.emplace(key, it, inserted);
        ASSERT_TRUE(inserted);
        it->getMapped() = i;
    }
    /// Lengths 25..30 plus the four trailing-zero keys; the embedded-zero key stays fixed-size.
    ASSERT_EQ(long_keys, 10);

    for (size_t i = 0; i < keys->size(); ++i)
    {
        auto found = map.find(keys->getDataAt(i));
        ASSERT_NE(found, nullptr) << "key size " << keys->getDataAt(i).size();
        ASSERT_EQ(found->getMapped(), i);
    }
}

/// `forEachValueWithHash` hands out every key, at every size class, with the canonical hash of the original key, which
/// it takes from the cell: a short key it hands out is a view of the integer the key is packed into in its cell, which
/// `hash` must not read.
TEST(StringHashTableSuppliedHash, ForEachValueWithHash)
{
    using Map = StringHashMap<UInt64>;
    Map map;

    const auto keys = testKeys();
    for (size_t i = 0; i < keys->size(); ++i)
    {
        Map::LookupResult it;
        bool inserted = false;
        map.emplace(keys->getDataAt(i), it, inserted);
        ASSERT_TRUE(inserted);
        it->getMapped() = i;
    }

    size_t visited = 0;
    map.forEachValueWithHash(
        [&](std::string_view key, UInt64 & mapped, size_t hash)
        {
            const std::string_view original = keys->getDataAt(mapped);
            ASSERT_EQ(key, original);
            ASSERT_EQ(hash, map.hash(original)) << "key size " << key.size();
            ++visited;
        });
    ASSERT_EQ(visited, keys->size());
}

/// The keys the iteration hands out go into another table with `emplaceIteratedKey` and the hashes of
/// `forEachValueWithHash`, at every size class: each lands once where the plain find looks for the original key, and a
/// second pass finds every one of them already inserted.
TEST(StringHashTableSuppliedHash, EmplaceIteratedKey)
{
    using Map = StringHashMap<UInt64>;
    Map source;
    Map destination;

    const auto keys = testKeys();
    for (size_t i = 0; i < keys->size(); ++i)
    {
        Map::LookupResult it;
        bool inserted = false;
        source.emplace(keys->getDataAt(i), it, inserted);
        ASSERT_TRUE(inserted);
        it->getMapped() = i;
    }

    for (const bool first_pass : {true, false})
    {
        source.forEachValueWithHash(
            [&](std::string_view key, UInt64 & mapped, size_t hash)
            {
                Map::LookupResult it;
                bool inserted = false;
                destination.emplaceIteratedKey(key, it, inserted, hash);
                ASSERT_EQ(inserted, first_pass) << "key size " << key.size();
                if (inserted)
                    it->getMapped() = mapped;
                ASSERT_EQ(it->getMapped(), mapped) << "key size " << key.size();
            });
    }
    ASSERT_EQ(destination.size(), keys->size());

    for (size_t i = 0; i < keys->size(); ++i)
    {
        auto found = destination.find(keys->getDataAt(i));
        ASSERT_NE(found, nullptr) << "key size " << keys->getDataAt(i).size();
        ASSERT_EQ(found->getMapped(), i);
    }
}
