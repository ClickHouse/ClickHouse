#include <iomanip>
#include <iostream>

#include <Interpreters/AggregationCommon.h>

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>

#include <IO/ReadBufferFromString.h>

#include <gtest/gtest.h>

/// To test dump functionality without using other hashes that can change
template <typename T>
struct DummyHash
{
    size_t operator()(T key) const { return T(key); }
};

template<typename HashTable>
std::set<typename HashTable::value_type> convertToSet(const HashTable& table)
{
    std::set<typename HashTable::value_type> result;

    for (auto v: table)
        result.emplace(v.getValue());

    return result;
}


TEST(HashTable, Insert)
{
    using Cont = HashSet<int, DefaultHash<int>, HashTableGrower<1>>;

    Cont cont;

    cont.insert(1);
    cont.insert(2);

    ASSERT_EQ(cont.size(), 2);
}

TEST(HashTable, Emplace)
{
    using Cont = HashSet<int, DefaultHash<int>, HashTableGrower<1>>;

    Cont cont;

    Cont::LookupResult it;
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
    using Cont = HashSet<int, DefaultHash<int>, HashTableGrower<1>>;

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
    using Cont = HashSet<int, DefaultHash<int>, HashTableGrower<1>>;

    Cont cont;

    cont.insert(1);
    cont.insert(2);
    cont.insert(3);

    std::set<int> expected = {1, 2, 3};
    std::set<int> actual = convertToSet(cont);

    ASSERT_EQ(actual, expected);
}

TEST(HashTable, Erase)
{
    {
        /// Check zero element deletion
        using Cont = HashSet<int, DummyHash<int>, HashTableGrower<4>>;
        Cont cont;

        cont.insert(0);

        ASSERT_TRUE(cont.find(0) != nullptr && cont.find(0)->getKey() == 0);

        cont.erase(0);

        ASSERT_TRUE(cont.find(0) == nullptr);
    }
    {
        using Cont = HashSet<int, DummyHash<int>, HashTableGrower<4>>;
        Cont cont;

        /// [.(1)..............] erase of (1).
        cont.insert(1);

        ASSERT_TRUE(cont.find(1) != nullptr && cont.find(1)->getKey() == 1);

        cont.erase(1);

        ASSERT_TRUE(cont.find(1) == nullptr);
    }
    {
        using Cont = HashSet<int, DummyHash<int>, HashTableGrower<4>>;
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
        using Cont = HashSet<int, DummyHash<int>, HashTableGrower<4>>;
        Cont cont;

        /// [.(1)(17).............] erase of (1) breaks search for (17) because their natural position is 1.
        cont.insert(1);
        cont.insert(17);
        cont.erase(1);

        ASSERT_TRUE(cont.find(1) == nullptr);
        ASSERT_TRUE(cont.find(17) != nullptr && cont.find(17)->getKey() == 17);
    }
    {
        using Cont = HashSet<int, DummyHash<int>, HashTableGrower<4>>;
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
        using Cont = HashSet<int, DummyHash<int>, HashTableGrower<4>>;
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
        using Cont = HashSet<int, DummyHash<int>, HashTableGrower<4>>;
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
        using Cont = HashSet<int, DefaultHash<int>, HashTableGrower<1>>;
        Cont cont;

        for (size_t i = 0; i < 5000; ++i)
        {
            cont.insert(i);
        }

        for (size_t i = 0; i < 2500; ++i)
        {
            cont.erase(i);
        }

        for (size_t i = 5000; i < 10000; ++i)
        {
            cont.insert(i);
        }

        for (size_t i = 5000; i < 10000; ++i)
        {
            cont.erase(i);
        }

        for (size_t i = 2500; i < 5000; ++i)
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
        using Cont = HashSet<int, DummyHash<int>, HashTableGrower<1>>;

        Cont cont;

        cont.insert(1);
        cont.insert(2);
        cont.insert(3);

        DB::WriteBufferFromOwnString wb;
        cont.writeText(wb);

        std::string expected = "3,1,2,3";

        ASSERT_EQ(wb.str(), expected);

        DB::ReadBufferFromString rb(expected);

        Cont deserialized;
        deserialized.readText(rb);
        ASSERT_EQ(convertToSet(cont), convertToSet(deserialized));
    }
    {
        using Cont = HashSet<int, DefaultHash<int>, HashTableGrower<1>>;

        Cont cont;

        cont.insert(1);
        cont.insert(2);
        cont.insert(3);

        DB::WriteBufferFromOwnString wb;
        cont.write(wb);

        DB::ReadBufferFromString rb(wb.str());

        Cont deserialized;
        deserialized.read(rb);
        ASSERT_EQ(convertToSet(cont), convertToSet(deserialized));
    }
    {
        using Cont = HashSet<int, DummyHash<int>, HashTableGrower<1>>;
        Cont cont;

        DB::WriteBufferFromOwnString wb;
        cont.writeText(wb);

        std::string expected = "0";
        ASSERT_EQ(wb.str(), expected);

        DB::ReadBufferFromString rb(expected);

        Cont deserialized;
        deserialized.readText(rb);
        ASSERT_EQ(convertToSet(cont), convertToSet(deserialized));
    }
    {
        using Cont = HashSet<DB::UInt128, DB::UInt128TrivialHash>;
        Cont cont;

        DB::WriteBufferFromOwnString wb;
        cont.write(wb);

        std::string expected;
        expected += static_cast<char>(0);

        ASSERT_EQ(wb.str(), expected);

        DB::ReadBufferFromString rb(expected);

        Cont deserialized;
        deserialized.read(rb);
        ASSERT_EQ(convertToSet(cont), convertToSet(deserialized));
    }
}
