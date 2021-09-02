#include <gtest/gtest.h>

#include <random>

#include <Common/RingBuffer.h>

using namespace DB;

TEST(RingBuffer, Empty)
{
    RingBuffer<int> buffer(1);

    ASSERT_TRUE(buffer.size() == 0u); // NOLINT
    ASSERT_TRUE(buffer.empty());
}

TEST(RingBuffer, PushAndPop)
{
    RingBuffer<int> buffer(2);

    int i;
    ASSERT_TRUE(true == buffer.tryPush(0));
    ASSERT_TRUE(true == buffer.tryPush(1));
    ASSERT_TRUE(false == buffer.tryPush(2));

    ASSERT_TRUE(2u == buffer.size());
    ASSERT_TRUE(false == buffer.empty());

    ASSERT_TRUE(true == buffer.tryPop(&i));
    ASSERT_TRUE(0 == i);
    ASSERT_TRUE(true == buffer.tryPop(&i));
    ASSERT_TRUE(1 == i);

    ASSERT_TRUE(false == buffer.tryPop(&i));
    ASSERT_TRUE(buffer.empty());
    ASSERT_TRUE(true == buffer.empty());
}

TEST(RingBuffer, Random)
{
    std::random_device device;
    std::mt19937 generator(device());

    std::uniform_int_distribution<> distribution(0, 3);

    RingBuffer<int> buffer(10);

    int next_element = 0;
    int next_received_element = 0;
    for (int i = 0; i < 100000; ++i) {
        if (distribution(generator) == 0)
        {
            if (buffer.tryPush(next_element))
                next_element++;
        }
        else
        {
            int element;
            if (buffer.tryPop(&element))
            {
                ASSERT_TRUE(next_received_element == element);
                next_received_element++;
            }
        }
    }
}


TEST(RingBuffer, Resize)
{
    RingBuffer<int> buffer(10);

    for (size_t i = 0; i < 10; ++i)
        ASSERT_TRUE(buffer.tryPush(i));

    buffer.resize(0);

    ASSERT_TRUE(buffer.empty());
    ASSERT_EQ(buffer.size(), 0u);

    ASSERT_FALSE(buffer.tryPush(42));

    int value;
    ASSERT_FALSE(buffer.tryPop(&value));

    buffer.resize(1);

    ASSERT_TRUE(buffer.tryPush(42));
    ASSERT_TRUE(buffer.tryPop(&value));
    ASSERT_EQ(value, 42);

    buffer.resize(42);

    for (size_t i = 0; i < 42; ++i)
        ASSERT_TRUE(buffer.tryPush(i));

    buffer.resize(56);

    for (size_t i = 0; i < 42; ++i)
    {
        ASSERT_TRUE(buffer.tryPop(&value));
        ASSERT_EQ(value, i);
    }

    for (size_t i = 0; i < 56; ++i)
        ASSERT_TRUE(buffer.tryPush(i));

    buffer.resize(13);

    for (size_t i = 0; i < 13; ++i)
    {
        ASSERT_TRUE(buffer.tryPop(&value));
        ASSERT_EQ(value, i);
    }
}


TEST(RingBuffer, removeElements)
{
    RingBuffer<int> buffer(10);

    for (size_t i = 0; i < 10; ++i)
        ASSERT_TRUE(buffer.tryPush(i));

    int value;
    ASSERT_TRUE(buffer.tryPop(&value));
    ASSERT_TRUE(buffer.tryPop(&value));
    ASSERT_TRUE(buffer.tryPop(&value));

    buffer.eraseAll([](int current) { return current % 2 == 0; });

    ASSERT_EQ(buffer.size(), 4);

    ASSERT_TRUE(buffer.tryPop(&value));
    ASSERT_EQ(value, 3);
    ASSERT_TRUE(buffer.tryPop(&value));
    ASSERT_EQ(value, 5);
    ASSERT_TRUE(buffer.tryPop(&value));
    ASSERT_EQ(value, 7);
    ASSERT_TRUE(buffer.tryPop(&value));
    ASSERT_EQ(value, 9);
}
