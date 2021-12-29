#include <iomanip>
#include <iostream>
#include <gtest/gtest.h>
#include <Common/LRUResourceCache.h>

TEST(LRUResourceCache, acquire)
{
    using MyCache = DB::LRUResourceCache<int, int>;
    auto mcache = MyCache(10, 10);
    int x = 10;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto val = mcache.acquire(1, load_int);
    x = 11;
    val = mcache.acquire(2, load_int);
    ASSERT_TRUE(val != nullptr);
    ASSERT_TRUE(*val == 11);

    val = mcache.acquire(1);
    ASSERT_TRUE(val != nullptr);
    ASSERT_TRUE(*val == 10);
}

TEST(LRUResourceCache, remove)
{
    using MyCache = DB::LRUResourceCache<int, int>;
    auto mcache = MyCache(10, 10);
    int x = 10;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto val = mcache.acquire(1, load_int);
    x = 11;
    val = mcache.acquire(2, load_int);

    auto succ = mcache.tryRemove(3);
    ASSERT_TRUE(succ);

    succ = mcache.tryRemove(1);
    ASSERT_TRUE(!succ);
    val = mcache.acquire(1);
    ASSERT_TRUE(val != nullptr);
    ASSERT_TRUE(*val == 10);

    mcache.release(1);
    succ = mcache.tryRemove(1);
    ASSERT_TRUE(!succ);
    mcache.release(1);
    succ = mcache.tryRemove(1);
    ASSERT_TRUE(succ);
    val = mcache.acquire(1);
    ASSERT_TRUE(val == nullptr);
}

struct MyWeight
{
    size_t operator()(const int & x) const { return static_cast<size_t>(x); }
};

TEST(LRUResourceCache, evict_on_weight)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(5, 10);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto val = mcache.acquire(1, load_int);
    mcache.release(1);

    val = mcache.acquire(2, load_int);
    mcache.release(2);

    x = 3;
    val = mcache.acquire(3, load_int);
    ASSERT_TRUE(val != nullptr);

    auto w = mcache.weight();
    ASSERT_EQ(w, 5);
    auto n = mcache.size();
    ASSERT_EQ(n, 2);

    val = mcache.acquire(1);
    ASSERT_TRUE(val == nullptr);
    val = mcache.acquire(2);
    ASSERT_TRUE(val != nullptr);
    val = mcache.acquire(3);
    ASSERT_TRUE(val != nullptr);
}

TEST(LRUResourceCache, evict_on_weight_v2)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(5, 10);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto val = mcache.acquire(1, load_int);
    mcache.release(1);

    val = mcache.acquire(2, load_int);
    mcache.release(2);

    val = mcache.acquire(1);
    mcache.release(1);

    x = 3;
    val = mcache.acquire(3, load_int);
    ASSERT_TRUE(val != nullptr);

    auto w = mcache.weight();
    ASSERT_EQ(w, 5);
    auto n = mcache.size();
    ASSERT_EQ(n, 2);

    val = mcache.acquire(1);
    ASSERT_TRUE(val != nullptr);
    val = mcache.acquire(2);
    ASSERT_TRUE(val == nullptr);
    val = mcache.acquire(3);
    ASSERT_TRUE(val != nullptr);
}

TEST(LRUResourceCache, evict_on_size)
{
    using MyCache = DB::LRUResourceCache<int, int>;
    auto mcache = MyCache(5, 2);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto val = mcache.acquire(1, load_int);
    mcache.release(1);

    val = mcache.acquire(2, load_int);
    mcache.release(2);

    x = 3;
    val = mcache.acquire(3, load_int);
    ASSERT_TRUE(val != nullptr);

    auto n = mcache.size();
    ASSERT_EQ(n, 2);
    auto w = mcache.weight();
    ASSERT_EQ(w, 2);

    val = mcache.acquire(1);
    ASSERT_TRUE(val == nullptr);
    val = mcache.acquire(2);
    ASSERT_TRUE(val != nullptr);
    val = mcache.acquire(3);
    ASSERT_TRUE(val != nullptr);
}

TEST(LRUResourceCache, not_evict_used_element)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(7, 10);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto val = mcache.acquire(1, load_int);

    val = mcache.acquire(2, load_int);
    mcache.release(2);

    val = mcache.acquire(3, load_int);
    mcache.release(3);

    x = 3;
    val = mcache.acquire(4, load_int);
    ASSERT_TRUE(val != nullptr);

    auto n = mcache.size();
    ASSERT_EQ(n, 3);
    auto w = mcache.weight();
    ASSERT_EQ(w, 7);

    val = mcache.acquire(1);
    ASSERT_TRUE(val != nullptr);
    val = mcache.acquire(2);
    ASSERT_TRUE(val == nullptr);
    val = mcache.acquire(3);
    ASSERT_TRUE(val != nullptr);
    val = mcache.acquire(4);
    ASSERT_TRUE(val != nullptr);
}

TEST(LRUResourceCache, acquire_fail)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(5, 10);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto val = mcache.acquire(1, load_int);
    val = mcache.acquire(2, load_int);
    val = mcache.acquire(3, load_int);
    ASSERT_TRUE(val == nullptr);

    auto n = mcache.size();
    ASSERT_EQ(n, 2);
    auto w = mcache.weight();
    ASSERT_EQ(w, 4);
    val = mcache.acquire(1);
    ASSERT_TRUE(val != nullptr);
    val = mcache.acquire(2);
    ASSERT_TRUE(val != nullptr);
    val = mcache.acquire(3);
    ASSERT_TRUE(val == nullptr);
}

TEST(LRUResourceCache, dup_acquire)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(20, 10);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto val = mcache.acquire(1, load_int);
    mcache.release(1);
    x = 11;
    val = mcache.acquire(1, load_int);
    ASSERT_TRUE(val != nullptr);

    auto n = mcache.size();
    ASSERT_EQ(n, 1);
    auto w = mcache.weight();
    ASSERT_EQ(w, 2);
    val = mcache.acquire(1);
    ASSERT_TRUE(val != nullptr);
    ASSERT_TRUE(*val == 2);
}

TEST(LRUResourceCache, re_acquire)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(20, 10);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto val = mcache.acquire(1, load_int);
    mcache.release(1);
    mcache.tryRemove(1);
    x = 11;
    val = mcache.acquire(1, load_int);
    ASSERT_TRUE(val != nullptr);

    auto n = mcache.size();
    ASSERT_EQ(n, 1);
    auto w = mcache.weight();
    ASSERT_EQ(w, 11);
    val = mcache.acquire(1);
    ASSERT_TRUE(val != nullptr);
    ASSERT_TRUE(*val == 11);
}
