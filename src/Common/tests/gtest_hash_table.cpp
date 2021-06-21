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
