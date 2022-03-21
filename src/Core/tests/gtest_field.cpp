#include <gtest/gtest.h>
#include <Core/Field.h>

using namespace DB;

GTEST_TEST(Field, FromBool)
{
    {
        Field f{false};
        ASSERT_EQ(f.getType(), Field::Types::Bool);
        ASSERT_EQ(f.get<UInt64>(), 0);
        ASSERT_EQ(f.get<bool>(), false);
    }

    {
        Field f{true};
        ASSERT_EQ(f.getType(), Field::Types::Bool);
        ASSERT_EQ(f.get<UInt64>(), 1);
        ASSERT_EQ(f.get<bool>(), true);
    }

    {
        Field f;
        f = false;
        ASSERT_EQ(f.getType(), Field::Types::Bool);
        ASSERT_EQ(f.get<UInt64>(), 0);
        ASSERT_EQ(f.get<bool>(), false);
    }

    {
        Field f;
        f = true;
        ASSERT_EQ(f.getType(), Field::Types::Bool);
        ASSERT_EQ(f.get<UInt64>(), 1);
        ASSERT_EQ(f.get<bool>(), true);
    }
}


GTEST_TEST(Field, Move)
{
    Field f;

    f = Field{String{"Hello, world (1)"}};
    ASSERT_EQ(f.get<String>(), "Hello, world (1)");
    f = Field{String{"Hello, world (2)"}};
    ASSERT_EQ(f.get<String>(), "Hello, world (2)");
    f = Field{Array{Field{String{"Hello, world (3)"}}}};
    ASSERT_EQ(f.get<Array>()[0].get<String>(), "Hello, world (3)");
    f = String{"Hello, world (4)"};
    ASSERT_EQ(f.get<String>(), "Hello, world (4)");
    f = Array{Field{String{"Hello, world (5)"}}};
    ASSERT_EQ(f.get<Array>()[0].get<String>(), "Hello, world (5)");
    f = Array{String{"Hello, world (6)"}};
    ASSERT_EQ(f.get<Array>()[0].get<String>(), "Hello, world (6)");
}
