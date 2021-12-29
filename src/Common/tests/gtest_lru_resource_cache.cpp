#include <iomanip>
#include <iostream>
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

    auto succ = MyCache::MappedHolder::tryRemove(&holder0);
    ASSERT_TRUE(!succ);
    holder0 = mcache.get(1);
    ASSERT_TRUE(holder0 != nullptr);
    ASSERT_TRUE(holder0->value() == 10);

    holder0 = nullptr;
    succ = MyCache::MappedHolder::tryRemove(&holder1);
    ASSERT_TRUE(succ);
    holder1 = mcache.get(1);
    ASSERT_TRUE(holder1 == nullptr);
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
    auto holder1 = mcache.getOrSet(1, load_int);
    holder1 = nullptr;

    auto holder2 = mcache.getOrSet(2, load_int);
    holder2 = nullptr;

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

TEST(LRUResourceCache, evict_on_weight_v2)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(5, 10);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto holder1 = mcache.getOrSet(1, load_int);
    holder1 = nullptr;

    auto holder2 = mcache.getOrSet(2, load_int);
    holder2 = nullptr;

    holder1 = mcache.get(1);
    holder1 = nullptr;

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

TEST(LRUResourceCache, evict_on_weight_v3)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(5, 10);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto holder1 = mcache.getOrSet(1, load_int);
    holder1 = nullptr;

    auto holder2 = mcache.getOrSet(2, load_int);
    holder2 = nullptr;

    holder1 = mcache.getOrSet(1, load_int);
    holder1 = nullptr;

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

TEST(LRUResourceCache, evict_on_size)
{
    using MyCache = DB::LRUResourceCache<int, int>;
    auto mcache = MyCache(5, 2);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto holder1 = mcache.getOrSet(1, load_int);
    holder1 = nullptr;

    auto holder2 = mcache.getOrSet(2, load_int);
    holder2 = nullptr;

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

TEST(LRUResourceCache, not_evict_used_element)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(7, 10);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto holder1 = mcache.getOrSet(1, load_int);

    auto holder2 = mcache.getOrSet(2, load_int);
    holder2 = nullptr;

    auto holder3 = mcache.getOrSet(3, load_int);
    holder3 = nullptr;

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

TEST(LRUResourceCache, get_fail)
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

TEST(LRUResourceCache, dup_get)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(20, 10);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto holder1 = mcache.getOrSet(1, load_int);
    holder1 = nullptr;
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

TEST(LRUResourceCache, re_get)
{
    using MyCache = DB::LRUResourceCache<int, int, MyWeight>;
    auto mcache = MyCache(20, 10);
    int x = 2;
    auto load_int = [&] { return std::make_shared<int>(x); };
    auto holder1 = mcache.getOrSet(1, load_int);
    MyCache::MappedHolder::tryRemove(&holder1);

    x = 11;
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
