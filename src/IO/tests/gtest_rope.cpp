#include <IO/Rope.h>
#include <gtest/gtest.h>

#include <cstring>

using namespace DB;

TEST(Range, Basic)
{
    Range r{100, 50};
    EXPECT_EQ(r.offset, 100);
    EXPECT_EQ(r.size, 50);
    EXPECT_EQ(r.end(), 150);
}

TEST(Range, ZeroSize)
{
    Range r{0, 0};
    EXPECT_EQ(r.end(), 0);
}

TEST(OwnedRopeBuffer, AllocateAndAccess)
{
    auto buf = std::make_shared<OwnedRopeBuffer>(1024);
    EXPECT_EQ(buf->size(), 1024);
    EXPECT_NE(buf->data(), nullptr);
    std::memset(buf->data(), 'A', 1024);
    EXPECT_EQ(buf->data()[0], 'A');
    EXPECT_EQ(buf->data()[1023], 'A');
}

TEST(RopeNode, SliceOfBuffer)
{
    auto buf = std::make_shared<OwnedRopeBuffer>(1024);
    std::memset(buf->data(), 0, 1024);
    buf->data()[100] = 'X';
    buf->data()[199] = 'Y';
    RopeNode node{buf, 100, 100, 5000};
    EXPECT_EQ(node.data()[0], 'X');
    EXPECT_EQ(node.data()[99], 'Y');
    EXPECT_EQ(node.size, 100);
    EXPECT_EQ(node.range().offset, 5000);
    EXPECT_EQ(node.range().size, 100);
}

TEST(RopeNode, SharedOwnership)
{
    auto buf = std::make_shared<OwnedRopeBuffer>(512);
    RopeNode node1{buf, 0, 256, 0};
    RopeNode node2{buf, 256, 256, 256};
    EXPECT_EQ(buf.use_count(), 3);
    EXPECT_EQ(node1.data(), buf->data());
    EXPECT_EQ(node2.data(), buf->data() + 256);
}
