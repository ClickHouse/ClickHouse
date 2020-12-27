#include <iomanip>
#include <iostream>

#include <Common/ArrayCache.h>

#include <gtest/gtest.h>

TEST(ArrayCache, Set)
{
    ArrayCache<int, int> cache(ArrayCache<int, int>::min_chunk_size);

    for (size_t i = 0; i < 120; ++i)
    {
        auto holder = cache.getOrSet(
            i,
            [&] { return sizeof(Int64); },
            [&](void * ptr, int & payload) {
                Int64* int_ptr = static_cast<Int64*>(ptr);
                *int_ptr = i;
                payload = i;
            },
            nullptr);

        ASSERT_TRUE(holder != nullptr);
    }

    for (size_t i = 0; i < 120; ++i)
    {
        auto result = cache.get(i);

        ASSERT_TRUE(result != nullptr);

        ASSERT_EQ(result->key(), i);
        ASSERT_EQ(result->payload(), i);
        ASSERT_EQ(*static_cast<Int64*>(result->ptr()), i);
    }
}

TEST(ArrayCache, SetGreaterThanSize)
{
    ArrayCache<int, int> cache(ArrayCache<int, int>::min_chunk_size);

    auto result = cache.getOrSet(
        0, [&] { return ArrayCache<int, int>::min_chunk_size + 1; }, [&](void *, int &) {}, nullptr);

    ASSERT_TRUE(result == nullptr);
}

TEST(ArrayCache, SetUnaligned)
{
    ArrayCache<int, int> cache(ArrayCache<int, int>::min_chunk_size);

    auto result = cache.getOrSet(0, [&] { return 15; }, [&](void *, int&) {}, nullptr);

    ASSERT_TRUE(result != nullptr);
    ASSERT_EQ(result->size(), 15);
}

TEST(ArrayCache, SetLRU)
{
    ArrayCache<int, int> cache(ArrayCache<int, int>::min_chunk_size);

    auto first = cache.getOrSet(
        0, [&] { return ArrayCache<int, int>::min_chunk_size / 4; }, [&](void *, int &) {}, nullptr);

    ASSERT_TRUE(first != nullptr);
    
    auto second = cache.getOrSet(
        1, [&] { return ArrayCache<int, int>::min_chunk_size / 4; }, [&](void *, int &) {}, nullptr);

    ASSERT_TRUE(second != nullptr);

    auto third = cache.getOrSet(
        2, [&] { return ArrayCache<int, int>::min_chunk_size / 2; }, [&](void *, int &) {}, nullptr);
    
    ASSERT_TRUE(third != nullptr);

    auto full_chunk_set_result = cache.getOrSet(
        4, [&] { return ArrayCache<int, int>::min_chunk_size; }, [&](void *, int &) {}, nullptr);
    
    ASSERT_TRUE(full_chunk_set_result == nullptr);

    first.reset();
    second.reset();
    third.reset();

    full_chunk_set_result = cache.getOrSet(
        5, [&] { return ArrayCache<int, int>::min_chunk_size; }, [&](void *, int &) {}, nullptr);
    
    ASSERT_TRUE(full_chunk_set_result != nullptr);
}