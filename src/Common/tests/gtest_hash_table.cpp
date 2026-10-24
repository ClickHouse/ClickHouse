#include <iomanip>
#include <limits>
#include <numeric>
#include <random>
#include <ranges>
#include <unordered_map>

#include <Interpreters/AggregationCommon.h>

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/HashTable/TwoLevelHashMap.h>
#include <Common/HashTable/Hash.h>
#include <Common/MemoryTracker.h>
#include <Common/iota.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <base/getPageSize.h>
#include <base/scope_guard.h>

#include <gtest/gtest.h>

#include <sys/mman.h>


using namespace DB;

namespace
{
std::vector<UInt64> getVectorWithNumbersUpToN(size_t n)
{
    std::vector<UInt64> res(n);
    iota(res.data(), res.size(), UInt64(0));
    return res;
}

}


/// To test dump functionality without using other hashes that can change
template <typename T>
struct DummyHash
{
    size_t operator()(T key) const { return T(key); }
};

template<typename HashTable>
std::set<std::string> convertToSet(const HashTable & table)
{
    std::set<std::string> result;

    for (auto v: table)
        result.emplace(toString(v.getValue()));

    return result;
}


TEST(HashTable, Insert)
{
    using Cont = HashSet<int, DefaultHash<int>, HashTableGrowerWithPrecalculation<1>>;

    Cont cont;

    cont.insert(1);
    cont.insert(2);

    ASSERT_EQ(cont.size(), 2);
}

TEST(HashTable, Emplace)
{
    using Cont = HashSet<int, DefaultHash<int>, HashTableGrowerWithPrecalculation<1>>;

    Cont cont;

    Cont::LookupResult it = {};
    bool inserted = false;
    cont.emplace(1, it, inserted);
    ASSERT_EQ(it->getKey(), 1);
    ASSERT_EQ(inserted, true);

    cont.emplace(2, it, inserted);
    ASSERT_EQ(it->getKey(), 2);
    ASSERT_EQ(inserted, true);

    cont.emplace(1, it, inserted);
    ASSERT_EQ(it->getKey(), 1);
    ASSERT_EQ(inserted, false);
}

TEST(HashTable, Lookup)
{
    using Cont = HashSet<int, DefaultHash<int>, HashTableGrowerWithPrecalculation<1>>;

    Cont cont;

    cont.insert(1);
    cont.insert(2);

    Cont::LookupResult it = cont.find(1);
    ASSERT_TRUE(it != nullptr);

    it = cont.find(2);
    ASSERT_TRUE(it != nullptr);

    it = cont.find(3);
    ASSERT_TRUE(it == nullptr);
}

TEST(HashTable, Iteration)
{
    using Cont = HashSet<int, DefaultHash<int>, HashTableGrowerWithPrecalculation<1>>;

    Cont cont;

    cont.insert(1);
    cont.insert(2);
    cont.insert(3);

    std::set<std::string> expected = {"1", "2", "3"};
    std::set<std::string> actual = convertToSet(cont);

    ASSERT_EQ(actual, expected);
}

TEST(HashTable, Erase)
{
    {
        /// Check zero element deletion
        using Cont = HashSet<int, DummyHash<int>, HashTableGrowerWithPrecalculation<4>>;
        Cont cont;

        cont.insert(0);

        ASSERT_TRUE(cont.find(0) != nullptr && cont.find(0)->getKey() == 0);

        cont.erase(0);

        ASSERT_TRUE(cont.find(0) == nullptr);
    }
    {
        using Cont = HashSet<int, DummyHash<int>, HashTableGrowerWithPrecalculation<4>>;
        Cont cont;

        /// [.(1)..............] erase of (1).
        cont.insert(1);

        ASSERT_TRUE(cont.find(1) != nullptr && cont.find(1)->getKey() == 1);

        cont.erase(1);

        ASSERT_TRUE(cont.find(1) == nullptr);
    }
    {
        using Cont = HashSet<int, DummyHash<int>, HashTableGrowerWithPrecalculation<4>>;
        Cont cont;

        /// [.(1)(2)(3)............] erase of (1) does not break search for (2) (3).
        cont.insert(1);
        cont.insert(2);
        cont.insert(3);
        cont.erase(1);

        ASSERT_TRUE(cont.find(1) == nullptr);
        ASSERT_TRUE(cont.find(2) != nullptr && cont.find(2)->getKey() == 2);
        ASSERT_TRUE(cont.find(3) != nullptr && cont.find(3)->getKey() == 3);

        cont.erase(2);
        cont.erase(3);
        ASSERT_TRUE(cont.find(2) == nullptr);
        ASSERT_TRUE(cont.find(3) == nullptr);
        ASSERT_EQ(cont.size(), 0);
    }
    {
        using Cont = HashSet<int, DummyHash<int>, HashTableGrowerWithPrecalculation<4>>;
        Cont cont;

        /// [.(1)(17).............] erase of (1) breaks search for (17) because their natural position is 1.
        cont.insert(1);
        cont.insert(17);
        cont.erase(1);

        ASSERT_TRUE(cont.find(1) == nullptr);
        ASSERT_TRUE(cont.find(17) != nullptr && cont.find(17)->getKey() == 17);
    }
    {
        using Cont = HashSet<int, DummyHash<int>, HashTableGrowerWithPrecalculation<4>>;
        Cont cont;

        /// [.(1)(2)(3)(17)...........] erase of (2) breaks search for (17) because their natural position is 1.

        cont.insert(1);
        cont.insert(2);
        cont.insert(3);
        cont.insert(17);
        cont.erase(2);

        ASSERT_TRUE(cont.find(2) == nullptr);
        ASSERT_TRUE(cont.find(1) != nullptr && cont.find(1)->getKey() == 1);
        ASSERT_TRUE(cont.find(3) != nullptr && cont.find(3)->getKey() == 3);
        ASSERT_TRUE(cont.find(17) != nullptr && cont.find(17)->getKey() == 17);
    }
    {
        using Cont = HashSet<int, DummyHash<int>, HashTableGrowerWithPrecalculation<4>>;
        Cont cont;

        /// [(16)(30)............(14)(15)] erase of (16) breaks search for (30) because their natural position is 14.
        cont.insert(14);
        cont.insert(15);
        cont.insert(16);
        cont.insert(30);
        cont.erase(16);

        ASSERT_TRUE(cont.find(16) == nullptr);
        ASSERT_TRUE(cont.find(14) != nullptr && cont.find(14)->getKey() == 14);
        ASSERT_TRUE(cont.find(15) != nullptr && cont.find(15)->getKey() == 15);
        ASSERT_TRUE(cont.find(30) != nullptr && cont.find(30)->getKey() == 30);
    }
    {
        using Cont = HashSet<int, DummyHash<int>, HashTableGrowerWithPrecalculation<4>>;
        Cont cont;

        /// [(16)(30)............(14)(15)] erase of (15) breaks search for (30) because their natural position is 14.
        cont.insert(14);
        cont.insert(15);
        cont.insert(16);
        cont.insert(30);
        cont.erase(15);

        ASSERT_TRUE(cont.find(15) == nullptr);
        ASSERT_TRUE(cont.find(14) != nullptr && cont.find(14)->getKey() == 14);
        ASSERT_TRUE(cont.find(16) != nullptr && cont.find(16)->getKey() == 16);
        ASSERT_TRUE(cont.find(30) != nullptr && cont.find(30)->getKey() == 30);
    }
    {
        using Cont = HashSet<int, DefaultHash<int>, HashTableGrowerWithPrecalculation<1>>;
        Cont cont;

        for (int i = 0; i < 5000; ++i)
        {
            cont.insert(i);
        }

        for (int i = 0; i < 2500; ++i)
        {
            cont.erase(i);
        }

        for (int i = 5000; i < 10000; ++i)
        {
            cont.insert(i);
        }

        for (int i = 5000; i < 10000; ++i)
        {
            cont.erase(i);
        }

        for (int i = 2500; i < 5000; ++i)
        {
            cont.erase(i);
        }

        ASSERT_EQ(cont.size(), 0);
    }
}

TEST(HashTable, SerializationDeserialization)
{
    {
        /// Use dummy hash to make it reproducible if default hash implementation will be changed
        using Cont = HashSet<int, DummyHash<int>, HashTableGrowerWithPrecalculation<1>>;

        Cont cont;

        cont.insert(1);
        cont.insert(2);
        cont.insert(3);

        WriteBufferFromOwnString wb;
        cont.writeText(wb);

        std::string expected = "3,1,2,3";

        ASSERT_EQ(wb.str(), expected);

        ReadBufferFromString rb(expected);

        Cont deserialized;
        deserialized.readText(rb);
        ASSERT_EQ(convertToSet(cont), convertToSet(deserialized));
    }
    {
        using Cont = HashSet<int, DefaultHash<int>, HashTableGrowerWithPrecalculation<1>>;

        Cont cont;

        cont.insert(1);
        cont.insert(2);
        cont.insert(3);

        WriteBufferFromOwnString wb;
        cont.write(wb);

        ReadBufferFromString rb(wb.str());

        Cont deserialized;
        deserialized.read(rb);
        ASSERT_EQ(convertToSet(cont), convertToSet(deserialized));
    }
    {
        using Cont = HashSet<int, DummyHash<int>, HashTableGrowerWithPrecalculation<1>>;
        Cont cont;

        WriteBufferFromOwnString wb;
        cont.writeText(wb);

        std::string expected = "0";
        ASSERT_EQ(wb.str(), expected);

        ReadBufferFromString rb(expected);

        Cont deserialized;
        deserialized.readText(rb);
        ASSERT_EQ(convertToSet(cont), convertToSet(deserialized));
    }
    {
        using Cont = HashSet<UInt128, UInt128TrivialHash>;
        Cont cont;

        WriteBufferFromOwnString wb;
        cont.write(wb);

        std::string expected;
        expected += static_cast<char>(0);

        ASSERT_EQ(wb.str(), expected);

        ReadBufferFromString rb(expected);

        Cont deserialized;
        deserialized.read(rb);
        ASSERT_EQ(convertToSet(cont), convertToSet(deserialized));
    }
}

template <typename T>
struct IdentityHash
{
    size_t operator()(T x) const { return x; }
};

struct OneElementResizeGrower
{
    /// If collision resolution chains are contiguous, we can implement erase operation by moving the elements.
    static constexpr auto performs_linear_probing_with_single_step = true;

    static constexpr size_t initial_count = 1;

    size_t bufSize() const { return buf_size; }

    size_t place(size_t x) const { return x % buf_size; }

    size_t next(size_t pos) const { return (pos + 1) % buf_size; }

    bool overflow(size_t elems) const { return elems >= buf_size; }

    void increaseSize() { ++buf_size; }

    void set(size_t) { }

    void setBufSize(size_t buf_size_) { buf_size = buf_size_; }

    size_t buf_size = initial_count;
};

TEST(HashTable, Resize)
{
    {
        /// Test edge case if after resize all cells are resized in end of buf and will take half of
        /// hash table place.
        using HashSet = HashSet<int, IdentityHash<int>, OneElementResizeGrower>;
        HashSet cont;

        cont.insert(3);
        cont.insert(1);

        std::set<std::string> expected = {"1", "3"};
        std::set<std::string> actual = convertToSet(cont);

        ASSERT_EQ(actual, expected);
    }
}


using HashSetContent = std::vector<UInt64>;

class TwoLevelHashSetFixture : public ::testing::TestWithParam<HashSetContent>
{
};


TEST_P(TwoLevelHashSetFixture, WriteAsSingleLevel)
{
    using Key = UInt64;

    {
        const auto & hash_set_content = GetParam();

        TwoLevelHashSet<Key, HashCRC32<Key>> two_level;
        for (const auto & elem : hash_set_content)
            two_level.insert(elem);

        WriteBufferFromOwnString wb;
        two_level.writeAsSingleLevel(wb);

        ReadBufferFromString rb(wb.str());
        HashSet<Key, HashCRC32<Key>> single_level;
        single_level.read(rb);

        EXPECT_EQ(single_level.size(), hash_set_content.size());
        for (const auto & elem : hash_set_content)
            EXPECT_NE(single_level.find(elem), nullptr);
    }
}


INSTANTIATE_TEST_SUITE_P(
    TwoLevelHashSetTests,
    TwoLevelHashSetFixture,
    ::testing::Values(
        HashSetContent{},
        getVectorWithNumbersUpToN(1),
        getVectorWithNumbersUpToN(100),
        getVectorWithNumbersUpToN(1000),
        getVectorWithNumbersUpToN(10000),
        getVectorWithNumbersUpToN(100000),
        getVectorWithNumbersUpToN(1000000)));


class TwoLevelHashSetMergingFixture : public ::testing::TestWithParam<std::tuple<size_t, bool, bool>>
{
};


TEST_P(TwoLevelHashSetMergingFixture, MergeWithSingleLevelSet)
{
    using Key = UInt64;

    const auto [set_size, two_level_has_zero, single_level_has_zero] = GetParam();

    TwoLevelHashSet<Key, HashCRC32<Key>> two_level;
    for (size_t elem = 1; elem < set_size + 1; ++elem)
        two_level.insert(elem);
    if (two_level_has_zero)
        two_level.insert(0);

    HashSet<Key, HashCRC32<Key>> single_level;
    for (size_t elem = 42; elem < set_size + 42; ++elem)
        single_level.insert(elem);
    if (single_level_has_zero)
        single_level.insert(0);

    two_level.merge(single_level);

    ASSERT_EQ(two_level.size(), 2 * set_size + ((two_level_has_zero || single_level_has_zero) ? 1 : 0));

    if (two_level_has_zero || single_level_has_zero)
        ASSERT_NE(two_level.find(0), nullptr);
    else
        ASSERT_EQ(two_level.find(0), nullptr);

    for (size_t elem = 1; elem < set_size + 1; ++elem)
        ASSERT_NE(two_level.find(elem), nullptr);

    for (size_t elem = 42; elem < set_size + 42; ++elem)
        ASSERT_NE(two_level.find(elem), nullptr);
}


INSTANTIATE_TEST_SUITE_P(
    TwoLevelHashSetMergingTests,
    TwoLevelHashSetMergingFixture,
    ::testing::Values(
        std::make_tuple(0, false, false),
        std::make_tuple(0, false, true),
        std::make_tuple(0, true, false),
        std::make_tuple(0, true, true),

        std::make_tuple(1, false, false),
        std::make_tuple(1, false, true),
        std::make_tuple(1, true, false),
        std::make_tuple(1, true, true),

        std::make_tuple(10, false, false),
        std::make_tuple(10, false, true),
        std::make_tuple(10, true, false),
        std::make_tuple(10, true, true)
    )
);

TEST(HashTable, StringHashMapMoveConstructorDoesNotAllocate)
{
    /// Populate a StringHashMap that covers all five sub-maps:
    /// m0 (empty key), m1 (1-8 bytes), m2 (9-16 bytes), m3 (17-24 bytes), ms (25+ bytes).
    /// Then move-construct it under DENY_ALLOCATIONS_IN_SCOPE to verify
    /// the move constructor does not allocate memory.

    using Map = StringHashMap<UInt64>;
    Map src;

    std::pair<std::string, UInt64> entries[] = {
        {"", 0},                              // m0 (empty key)
        {"hello", 1},                         // m1 (1-8 bytes)
        {"medium_key_9abc", 2},               // m2 (9-16 bytes)
        {"long_key_17bytes_value", 3},        // m3 (17-24 bytes)
        {"this_is_a_very_long_key_value!!", 4}, // ms (25+ bytes)
    };

    for (const auto & [key, value] : entries)
    {
        Map::LookupResult it;
        bool inserted = false;
        std::string_view key_view = key;
        src.emplace(key_view, it, inserted);
        ASSERT_TRUE(inserted);
        it->getMapped() = value;
    }

    auto check_map = [&](const auto & map)
    {
        ASSERT_EQ(map.size(), 5);

        /// Verify all keys are findable in the destination map.
        for (const auto & [key, value] : entries)
        {
            std::string_view key_view = key;
            auto it = map.find(key_view);
            ASSERT_TRUE(it != nullptr) << "key not found after move: size=" << key.size();
            ASSERT_EQ(it->getMapped(), value);
        }
    };

    /// The move constructor must not allocate — it only swaps buffer pointers.
    DENY_ALLOCATIONS_IN_SCOPE;
    Map dst1(std::move(src));
    ALLOW_ALLOCATIONS_IN_SCOPE;

    check_map(dst1);

    DENY_ALLOCATIONS_IN_SCOPE;
    Map dst2 = std::move(dst1);
    ALLOW_ALLOCATIONS_IN_SCOPE;

    check_map(dst2);
}

TEST(HashTable, StringHashMapShortKeyBeforeUnreadablePage)
{
    /// `dispatchOnKeyClass` reads a 1..8-byte key from its start, or from its end when the key starts
    /// in the last 7 bytes of a 4 KiB page. Both reads must give the same key wherever it sits in memory.
    /// The page after the keys is unreadable, so neither read may cross its page end.
    /// Pages may be up to 64 KiB on some AArch64 systems, but they still end at a 4 KiB boundary — what `dispatchOnKeyClass` checks.
    const size_t page_size = getPageSize();
    char * mem = static_cast<char *>(mmap(nullptr, 2 * page_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    ASSERT_NE(mem, MAP_FAILED);
    SCOPE_EXIT({ munmap(mem, 2 * page_size); });
    ASSERT_EQ(mprotect(mem + page_size, page_size, PROT_NONE), 0);
    memset(mem, 'X', page_size);

    for (size_t sz = 1; sz <= 8; ++sz)
    {
        StringHashMap<UInt64> map;
        /// The key ends at or before the unreadable page.
        for (size_t offset = page_size - 16; offset + sz <= page_size; ++offset)
        {
            memcpy(mem + offset, "abcdefgh", sz);
            ++map[std::string_view(mem + offset, sz)];
        }
        ASSERT_EQ(map.size(), 1) << "sz=" << sz;

        /// Both reads could agree on a wrong key, so check the stored key itself.
        map.forEachValue(
            [&](std::string_view key, UInt64 & count)
            {
                EXPECT_EQ(key, std::string_view("abcdefgh", sz)) << "sz=" << sz;
                EXPECT_EQ(count, 17 - sz) << "sz=" << sz;
            });
    }
}

namespace
{

/// Records the allocation overlap required by the memory tracker before a replacement is admitted.
struct GrowthTrackingAllocator : HashTableAllocator
{
    static inline size_t allocated_bytes = 0;
    static inline size_t peak_bytes = 0;

    void * alloc(size_t bytes)
    {
        void * result = HashTableAllocator::alloc(bytes);
        allocated_bytes += bytes;
        peak_bytes = std::max(peak_bytes, allocated_bytes);
        return result;
    }

    void * realloc(void * buffer, size_t old_bytes, size_t new_bytes)
    {
        peak_bytes = std::max(peak_bytes, allocated_bytes + new_bytes);
        void * result = HashTableAllocator::realloc(buffer, old_bytes, new_bytes);
        allocated_bytes += new_bytes - old_bytes;
        return result;
    }

    void free(void * buffer, size_t bytes)
    {
        HashTableAllocator::free(buffer, bytes);
        allocated_bytes -= bytes;
    }
};


template <typename Grower>
void checkGrowthAllocations()
{
    using Table = HashSet<UInt64, DefaultHash<UInt64>, Grower, GrowthTrackingAllocator>;
    Table table;
    UInt64 next_key = 1;
    for (size_t additional_keys : {0, 1, 2, 20, 500, 2000, 40000})
    {
        const size_t initial_bytes = GrowthTrackingAllocator::allocated_bytes;
        GrowthTrackingAllocator::peak_bytes = initial_bytes;
        const size_t growth_memory = table.estimateGrowthMemory(additional_keys);
        for (size_t i = 0; i < additional_keys; ++i)
            table.insert(next_key++);
        EXPECT_EQ(GrowthTrackingAllocator::peak_bytes, initial_bytes + growth_memory);
    }
}

}

TEST(HashTableGrowth, MatchesSuccessiveReplacementAllocations)
{
    checkGrowthAllocations<HashTableGrower<2>>();
    checkGrowthAllocations<HashTableGrowerWithPrecalculation<2>>();
    checkGrowthAllocations<TwoLevelHashTableGrower<15>>();
}

TEST(HashTableGrowth, SaturatesAtRepresentableSize)
{
    constexpr size_t max_size = std::numeric_limits<size_t>::max();
    auto check = [&]<typename Key, typename Grower>()
    {
        HashSet<Key, DefaultHash<Key>, Grower> table;
        const size_t initial_bytes = table.getBufferSizeInBytes();
        EXPECT_EQ(table.estimateGrowthMemory(max_size), max_size);
        EXPECT_EQ(table.estimateGrowthMemory(max_size / 2), max_size);
        EXPECT_TRUE(table.empty());
        EXPECT_EQ(table.getBufferSizeInBytes(), initial_bytes);

        table.insert(1);
        EXPECT_EQ(table.estimateGrowthMemory(max_size), max_size);
        EXPECT_EQ(table.size(), 1);
        EXPECT_EQ(table.getBufferSizeInBytes(), initial_bytes);
    };
    check.template operator()<UInt8, HashTableGrower<2>>();
    check.template operator()<UInt64, HashTableGrower<2>>();
    check.template operator()<UInt8, HashTableGrowerWithPrecalculation<2>>();
    check.template operator()<UInt64, HashTableGrowerWithPrecalculation<2>>();
    check.template operator()<UInt8, TwoLevelHashTableGrower<15>>();
    check.template operator()<UInt64, TwoLevelHashTableGrower<15>>();
    check.template operator()<UInt8, StringHashTableGrower<2>>();
    check.template operator()<UInt64, StringHashTableGrower<2>>();
}

TEST(HashTableGrowth, SaturatesCombinedReplacementMemory)
{
    /// Each buffer fits in `size_t`, but accounting for both during replacement exceeds it.
    HashMap<UInt64, std::pair<UInt64, UInt64>> table;
    const size_t initial_bytes = table.getBufferSizeInBytes();
    EXPECT_EQ(table.estimateGrowthMemory(size_t(1) << 58), std::numeric_limits<size_t>::max());
    EXPECT_TRUE(table.empty());
    EXPECT_EQ(table.getBufferSizeInBytes(), initial_bytes);
}

namespace
{

/// Gives access to the internals of a hash table to check the invariants of the collision resolution chains.
template <typename Base>
struct HashTableWithInvariantsCheck : Base
{
    using Base::Base;

    bool isRobinHoodMode() const { return this->isRobinHood(); }

    void checkInvariants() const
    {
        const auto & grower = this->grower;
        size_t num_elements = this->hasZero() ? 1 : 0;
        for (size_t i = 0; i < grower.bufSize(); ++i)
        {
            if (this->buf[i].isZero(*this))
                continue;
            ++num_elements;

            /// The element is reachable from its optimal position.
            size_t distance = this->distanceFromOptimalPosition(this->buf[i], i);
            for (size_t j = 1; j <= distance; ++j)
                ASSERT_FALSE(this->buf[grower.place(i - j)].isZero(*this)) << "element at " << i << " is unreachable";

            /// The elements are ordered by their optimal positions.
            size_t prev = grower.place(i - 1);
            if (this->isRobinHood() && !this->buf[prev].isZero(*this))
                ASSERT_GE(this->distanceFromOptimalPosition(this->buf[prev], prev) + 1, distance) << "elements at " << prev << " and " << i << " are out of order";
        }
        ASSERT_EQ(num_elements, this->size());
    }
};

/// Many keys share the optimal position, and the chains wrap around the end of the buffer.
struct CollidingHash
{
    size_t operator()(UInt64 key) const { return (key / 8) * 0x9E3779B97F4A7C15ULL; }
};

template <typename Map>
void checkRandomOperations(size_t num_operations, UInt64 max_key, bool with_erase)
{
    Map map;
    std::unordered_map<UInt64, UInt64> expected;
    std::mt19937_64 rng(42); /// NOLINT(cert-msc32-c,cert-msc51-cpp,bugprone-random-generator-seed) a reproducible sequence
    bool switched_to_robin_hood = false;

    for (size_t i = 0; i < num_operations; ++i)
    {
        UInt64 key = rng() % max_key;
        size_t operation = rng() % 8;
        if (with_erase && operation == 0)
        {
            ASSERT_EQ(map.erase(key), expected.erase(key) != 0);
        }
        else if (operation <= 2)
        {
            auto * it = map.find(key);
            auto expected_it = expected.find(key);
            ASSERT_EQ(it != nullptr, expected_it != expected.end());
            if (it)
                ASSERT_EQ(it->getMapped(), expected_it->second);
        }
        else
        {
            typename Map::LookupResult it;
            bool inserted = false;
            map.emplace(key, it, inserted);
            ASSERT_EQ(inserted, expected.emplace(key, i).second);
            ASSERT_EQ(it->getKey(), key);
            if (inserted)
                new (&it->getMapped()) UInt64(i);
            ASSERT_EQ(it->getMapped(), expected[key]);
        }

        switched_to_robin_hood |= map.isRobinHoodMode();
        if (i % 1000 == 0)
            map.checkInvariants();
    }

    ASSERT_TRUE(switched_to_robin_hood);
    map.checkInvariants();
    ASSERT_EQ(map.size(), expected.size());

    size_t num_iterated = 0;
    for (const auto & cell : map)
    {
        ASSERT_EQ(cell.getMapped(), expected.at(cell.getKey()));
        ++num_iterated;
    }
    ASSERT_EQ(num_iterated, expected.size());

    for (const auto & [key, value] : expected)
    {
        auto * it = map.find(key);
        ASSERT_TRUE(it);
        ASSERT_EQ(it->getMapped(), value);
    }
}

using RobinHoodTestGrower = HashTableGrowerWithPrecalculationAndRobinHood<4, 23, 8>;

}

TEST(HashTableRobinHood, RandomOperations)
{
    using Map = HashTableWithInvariantsCheck<HashMap<UInt64, UInt64, DefaultHash<UInt64>, RobinHoodTestGrower>>;
    using MapWithSavedHash = HashTableWithInvariantsCheck<HashMapWithSavedHash<UInt64, UInt64, DefaultHash<UInt64>, RobinHoodTestGrower>>;
    using MapWithCollisions = HashTableWithInvariantsCheck<HashMap<UInt64, UInt64, CollidingHash, RobinHoodTestGrower>>;

    for (bool with_erase : {false, true})
    {
        checkRandomOperations<Map>(200000, 30000, with_erase);
        checkRandomOperations<MapWithSavedHash>(200000, 30000, with_erase);
        checkRandomOperations<MapWithCollisions>(200000, 30000, with_erase);
        /// The table stays small, so the erasures happen on a table with the high load factor.
        checkRandomOperations<Map>(100000, 1000, with_erase);
        checkRandomOperations<MapWithCollisions>(100000, 1000, with_erase);
    }
}

TEST(HashTableRobinHood, GrowerSwitchesInPlace)
{
    HashTableGrowerWithPrecalculationAndRobinHood<4, 23, 8> grower;
    while (grower.bufSize() < 256)
    {
        ASSERT_FALSE(grower.isRobinHood());
        ASSERT_TRUE(grower.overflow(grower.bufSize() / 2 + 1));
        grower.increaseSize();
    }
    ASSERT_EQ(grower.bufSize(), 256);
    ASSERT_FALSE(grower.isRobinHood());

    /// The switch to the Robin Hood mode keeps the size and increases the load factor.
    grower.increaseSize();
    ASSERT_EQ(grower.bufSize(), 256);
    ASSERT_TRUE(grower.isRobinHood());
    ASSERT_FALSE(grower.overflow(230));
    ASSERT_TRUE(grower.overflow(231));

    /// Then the size doubles.
    grower.increaseSize();
    ASSERT_EQ(grower.bufSize(), 512);
    ASSERT_TRUE(grower.isRobinHood());

    grower.set(100);
    ASSERT_EQ(grower.bufSize(), 256);
    ASSERT_FALSE(grower.isRobinHood());
    grower.set(1000);
    ASSERT_EQ(grower.bufSize(), 2048);
    ASSERT_TRUE(grower.isRobinHood());
    grower.set(10000);
    ASSERT_EQ(grower.bufSize(), 16384);
    ASSERT_TRUE(grower.isRobinHood());
}

TEST(HashTableRobinHood, ReserveSwitchesInPlace)
{
    using Map = HashTableWithInvariantsCheck<HashMap<UInt64, UInt64, CollidingHash, RobinHoodTestGrower>>;

    Map map;
    for (UInt64 i = 0; i < 100; ++i)
        map[i * 3] = i;
    ASSERT_EQ(map.getBufferSizeInCells(), 256);
    ASSERT_FALSE(map.isRobinHoodMode());

    /// The reserved number of elements fits into the same buffer with the higher load factor.
    map.reserve(200);
    ASSERT_EQ(map.getBufferSizeInCells(), 256);
    ASSERT_TRUE(map.isRobinHoodMode());
    map.checkInvariants();

    for (UInt64 i = 0; i < 200; ++i)
        map[i * 3] = i;
    ASSERT_EQ(map.getBufferSizeInCells(), 256);
    map.checkInvariants();
}

TEST(HashTableRobinHood, ReserveSerializationAndTwoLevel)
{
    using Map = HashTableWithInvariantsCheck<HashMap<UInt64, UInt64, CollidingHash, RobinHoodTestGrower>>;

    Map map;
    for (UInt64 i = 0; i < 100; ++i)
        map[i * 3] = i;
    ASSERT_FALSE(map.isRobinHoodMode());

    /// Reserving the space for many elements switches to the Robin Hood mode and has to reorder the existing elements.
    map.reserve(10000);
    ASSERT_TRUE(map.isRobinHoodMode());
    map.checkInvariants();
    for (UInt64 i = 0; i < 10000; ++i)
        map[i * 3] = i;
    map.checkInvariants();

    WriteBufferFromOwnString out;
    map.write(out);
    ReadBufferFromString in(out.str());
    Map deserialized;
    deserialized.read(in);
    ASSERT_TRUE(deserialized.isRobinHoodMode());
    deserialized.checkInvariants();
    ASSERT_EQ(deserialized.size(), 10000);

    /// The conversion to the two-level table copies the cells with `insertUniqueNonZero`.
    using TwoLevelMap = TwoLevelHashMap<UInt64, UInt64, CollidingHash, HashTableGrowerWithPrecalculationAndRobinHood<4, 15, 5>>;
    TwoLevelMap two_level(map);
    ASSERT_EQ(two_level.size(), 10000);
    for (UInt64 i = 0; i < 10000; ++i)
    {
        auto * it = two_level.find(i * 3);
        ASSERT_TRUE(it);
        ASSERT_EQ(it->getMapped(), i);
        ASSERT_FALSE(two_level.find(i * 3 + 1));
    }

    for (UInt64 i = 0; i < 10000; ++i)
        ASSERT_TRUE(deserialized.erase(i * 3));
    ASSERT_TRUE(deserialized.empty());
    deserialized.checkInvariants();
}
