#include <iomanip>
#include <gtest/gtest.h>
#include <Common/LRUResourceCache.h>

TEST(LRUResourceCache, get)
{
    using MyCache = DB::LRUResourceCache<int, int>;
    auto mcache = MyCache(10, 10);
    int x = 10;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto holder1 = mcache.getOrSet(1, load_int);
    x = 11;
    auto holder2 = mcache.getOrSet(2, load_int);
    ASSERT_TRUE(holder2 != nullptr);
    ASSERT_TRUE(holder2->value() == 11);

    auto holder3 = mcache.get(1);
    ASSERT_TRUE(holder3 != nullptr);
    ASSERT_TRUE(holder3->value() == 10);
}

TEST(LRUResourceCache, remove)
{
    using MyCache = DB::LRUResourceCache<int, int>;
    auto mcache = MyCache(10, 10);
    int x = 10;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto holder0 = mcache.getOrSet(1, load_int);
    auto holder1 = mcache.getOrSet(1, load_int);

    mcache.tryRemove(1);
    holder0 = mcache.get(1);
    ASSERT_TRUE(holder0 == nullptr);
    auto n = mcache.size();
    ASSERT_TRUE(n == 1);

    holder0.reset();
    holder1.reset();
    n = mcache.size();
    ASSERT_TRUE(n == 0);
}

struct MyWeight
{
    size_t operator()(const int & x) const { return static_cast<size_t>(x); }
};

TEST(LRUResourceCache, remove2)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(10, 10);
    for (int i = 1; i < 5; ++i)
    {
        auto load_int = [&] { return std::make_shared<int>(i); };
        mcache.getOrSet(i, load_int);
    }

    auto n = mcache.size();
    ASSERT_EQ(n, 4);
    auto w = mcache.weight();
    ASSERT_EQ(w, 10);
    auto holder4 = mcache.get(4);
    ASSERT_TRUE(holder4 != nullptr);
    mcache.tryRemove(4);
    auto holder_reget_4 = mcache.get(4);
    ASSERT_TRUE(holder_reget_4 == nullptr);
    mcache.getOrSet(4, [&]() { return std::make_shared<int>(4); });
    holder4.reset();
    auto holder1 = mcache.getOrSet(1, [&]() { return std::make_shared<int>(1); });
    ASSERT_TRUE(holder1 != nullptr);
    auto holder7 = mcache.getOrSet(7, [&] { return std::make_shared<int>(7); });
    ASSERT_TRUE(holder7 != nullptr);
}

TEST(LRUResourceCache, evictOnWweight)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(5, 10);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto holder1 = mcache.getOrSet(1, load_int);
    holder1.reset();

    auto holder2 = mcache.getOrSet(2, load_int);
    holder2.reset();

    x = 3;
    auto holder3 = mcache.getOrSet(3, load_int);
    ASSERT_TRUE(holder3 != nullptr);

    auto w = mcache.weight();
    ASSERT_EQ(w, 5);
    auto n = mcache.size();
    ASSERT_EQ(n, 2);

    holder1 = mcache.get(1);
    ASSERT_TRUE(holder1 == nullptr);
    holder2 = mcache.get(2);
    ASSERT_TRUE(holder2 != nullptr);
    holder3 = mcache.get(3);
    ASSERT_TRUE(holder3 != nullptr);
}

TEST(LRUResourceCache, evictOnWeightV2)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(5, 10);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto holder1 = mcache.getOrSet(1, load_int);
    holder1.reset();

    auto holder2 = mcache.getOrSet(2, load_int);
    holder2.reset();

    holder1 = mcache.get(1);
    holder1.reset();

    x = 3;
    auto holder3 = mcache.getOrSet(3, load_int);
    ASSERT_TRUE(holder3 != nullptr);

    auto w = mcache.weight();
    ASSERT_EQ(w, 5);
    auto n = mcache.size();
    ASSERT_EQ(n, 2);

    holder1 = mcache.get(1);
    ASSERT_TRUE(holder1 != nullptr);
    holder2 = mcache.get(2);
    ASSERT_TRUE(holder2 == nullptr);
    holder3 = mcache.get(3);
    ASSERT_TRUE(holder3 != nullptr);
}

TEST(LRUResourceCache, evictOnWeightV3)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(5, 10);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto holder1 = mcache.getOrSet(1, load_int);
    holder1.reset();

    auto holder2 = mcache.getOrSet(2, load_int);
    holder2.reset();

    holder1 = mcache.getOrSet(1, load_int);
    holder1.reset();

    x = 3;
    auto holder3 = mcache.getOrSet(3, load_int);
    ASSERT_TRUE(holder3 != nullptr);

    auto w = mcache.weight();
    ASSERT_EQ(w, 5);
    auto n = mcache.size();
    ASSERT_EQ(n, 2);

    holder1 = mcache.get(1);
    ASSERT_TRUE(holder1 != nullptr);
    holder2 = mcache.get(2);
    ASSERT_TRUE(holder2 == nullptr);
    holder3 = mcache.get(3);
    ASSERT_TRUE(holder3 != nullptr);
}

TEST(LRUResourceCache, evictOnSize)
{
    using MyCache = DB::LRUResourceCache<int, int>;
    auto mcache = MyCache(5, 2);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto holder1 = mcache.getOrSet(1, load_int);
    holder1.reset();

    auto holder2 = mcache.getOrSet(2, load_int);
    holder2.reset();

    x = 3;
    auto holder3 = mcache.getOrSet(3, load_int);
    ASSERT_TRUE(holder3 != nullptr);

    auto n = mcache.size();
    ASSERT_EQ(n, 2);
    auto w = mcache.weight();
    ASSERT_EQ(w, 2);

    holder1 = mcache.get(1);
    ASSERT_TRUE(holder1 == nullptr);
    holder2 = mcache.get(2);
    ASSERT_TRUE(holder2 != nullptr);
    holder3 = mcache.get(3);
    ASSERT_TRUE(holder3 != nullptr);
}

TEST(LRUResourceCache, notEvictUsedElement)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(7, 10);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto holder1 = mcache.getOrSet(1, load_int);

    auto holder2 = mcache.getOrSet(2, load_int);
    holder2.reset();

    auto holder3 = mcache.getOrSet(3, load_int);
    holder3.reset();

    x = 3;
    auto holder4 = mcache.getOrSet(4, load_int);
    ASSERT_TRUE(holder4 != nullptr);

    auto n = mcache.size();
    ASSERT_EQ(n, 3);
    auto w = mcache.weight();
    ASSERT_EQ(w, 7);

    holder1 = mcache.get(1);
    ASSERT_TRUE(holder1 != nullptr);
    holder2 = mcache.get(2);
    ASSERT_TRUE(holder2 == nullptr);
    holder3 = mcache.get(3);
    ASSERT_TRUE(holder3 != nullptr);
    holder4 = mcache.get(4);
    ASSERT_TRUE(holder4 != nullptr);
}

TEST(LRUResourceCache, getFail)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(5, 10);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto holder1 = mcache.getOrSet(1, load_int);
    auto holder2 = mcache.getOrSet(2, load_int);
    auto holder3 = mcache.getOrSet(3, load_int);
    ASSERT_TRUE(holder3 == nullptr);

    auto n = mcache.size();
    ASSERT_EQ(n, 2);
    auto w = mcache.weight();
    ASSERT_EQ(w, 4);
    holder1 = mcache.get(1);
    ASSERT_TRUE(holder1 != nullptr);
    holder2 = mcache.get(2);
    ASSERT_TRUE(holder2 != nullptr);
    holder3 = mcache.get(3);
    ASSERT_TRUE(holder3 == nullptr);
}

TEST(LRUResourceCache, dupGet)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(20, 10);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto holder1 = mcache.getOrSet(1, load_int);
    holder1.reset();
    x = 11;
    holder1 = mcache.getOrSet(1, load_int);
    ASSERT_TRUE(holder1 != nullptr);

    auto n = mcache.size();
    ASSERT_EQ(n, 1);
    auto w = mcache.weight();
    ASSERT_EQ(w, 2);
    holder1 = mcache.get(1);
    ASSERT_TRUE(holder1 != nullptr);
    ASSERT_TRUE(holder1->value() == 2);
}

TEST(LRUResourceCache, reGet)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(20, 10);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto holder1 = mcache.getOrSet(1, load_int);
    mcache.tryRemove(1);

    x = 11;
    holder1.reset();
    holder1 = mcache.getOrSet(1, load_int);
    ASSERT_TRUE(holder1 != nullptr);

    auto n = mcache.size();
    ASSERT_EQ(n, 1);
    auto w = mcache.weight();
    ASSERT_EQ(w, 11);
    holder1 = mcache.get(1);
    ASSERT_TRUE(holder1 != nullptr);
    ASSERT_TRUE(holder1->value() == 11);
}

