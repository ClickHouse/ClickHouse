#include <base/BorrowedObjectPool.h>

#include <memory>
#include <stdexcept>

#include <gtest/gtest.h>

TEST(BorrowedObjectPool, FactoryExceptionDoesNotConsumeCapacity)
{
    BorrowedObjectPool<std::unique_ptr<int>> pool(1);
    std::unique_ptr<int> object;

    EXPECT_THROW(
        pool.tryBorrowObject(
            object,
            []() -> std::unique_ptr<int>
            {
                throw std::runtime_error("factory failed");
            }),
        std::runtime_error);

    EXPECT_EQ(pool.allocatedObjectsSize(), 0);
    EXPECT_EQ(pool.borrowedObjectsSize(), 0);

    EXPECT_TRUE(pool.tryBorrowObject(object, []
    {
        return std::make_unique<int>(42);
    }));
    ASSERT_NE(object, nullptr);
    EXPECT_EQ(*object, 42);
    EXPECT_EQ(pool.allocatedObjectsSize(), 1);
    EXPECT_EQ(pool.borrowedObjectsSize(), 1);

    pool.returnObject(std::move(object));
    EXPECT_EQ(pool.allocatedObjectsSize(), 1);
    EXPECT_EQ(pool.borrowedObjectsSize(), 0);
}
