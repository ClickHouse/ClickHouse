#include <memory>
#include <gtest/gtest.h>
#include <Common/PoolBase.h>
#include <Poco/Logger.h>
using namespace DB;

class PoolObject
{
public:
    int x = 0;
};

class MyPoolBase : public PoolBase<PoolObject>
{
public:
    using Object = PoolBase<PoolObject>::Object;
    using ObjectPtr = std::shared_ptr<Object>;
    using Ptr = PoolBase<PoolObject>::Ptr;

    int last_destroy_value = 0;
    MyPoolBase() : PoolBase<PoolObject>(100, getLogger("MyPoolBase")) { }

protected:
    ObjectPtr allocObject() override { return std::make_shared<Object>(); }

    void expireObject(ObjectPtr obj) override
    {
        LOG_TRACE(log, "expire object");
        ASSERT_TRUE(obj->x == 100);
        last_destroy_value = obj->x;
    }
};

TEST(PoolBase, testDestroy1)
{
    MyPoolBase pool;
    {
        auto obj_entry = pool.get(-1);
        ASSERT_TRUE(!obj_entry.isNull());
        obj_entry->x = 100;
        obj_entry.expire();
    }
    ASSERT_EQ(1, pool.size());

    {
        auto obj_entry = pool.get(-1);
        ASSERT_TRUE(!obj_entry.isNull());
        ASSERT_EQ(obj_entry->x, 0);
        ASSERT_EQ(1, pool.size());
    }
    ASSERT_EQ(100, pool.last_destroy_value);
}
