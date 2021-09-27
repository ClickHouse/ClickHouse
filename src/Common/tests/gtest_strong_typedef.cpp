#include <common/StrongTypedef.h>
#include <set>
#include <unordered_set>
#include <memory>
#include <type_traits>

#include <gtest/gtest.h>

#define STRONG_TYPEDEF(T, D) using D = StrongTypedef<T, struct D ## Tag>;


TEST(StrongTypedef, TypedefsOfTheSameType)
{
    /// check that strong typedefs of same type differ
    STRONG_TYPEDEF(int, Int)
    STRONG_TYPEDEF(int, AnotherInt)

    EXPECT_TRUE(!(std::is_same<Int, AnotherInt>::value));
}

TEST(StrongTypedef, Map)
{
    STRONG_TYPEDEF(int, Int)

    /// check that this code compiles
    std::set<Int> int_set;
    int_set.insert(Int(1));
    std::unordered_set<Int> int_unorderd_set;
    int_unorderd_set.insert(Int(2));
}

TEST(StrongTypedef, ZeroAssignment)
{
    STRONG_TYPEDEF(int, Int)

    Int a;
    Int b = {};
    Int c(0);
    Int d = Int(0);

    EXPECT_EQ(a, 0);
    EXPECT_EQ(b, 0);
    EXPECT_EQ(c, 0);
    EXPECT_EQ(d, 0);
}

TEST(StrongTypedef, CopyAndMoveCtor)
{
    STRONG_TYPEDEF(int, Int)
    Int a(1);
    Int b(2);
    a = b;
    EXPECT_EQ(a.toUnderType(), 2);

    STRONG_TYPEDEF(std::unique_ptr<int>, IntPtr)
    {
        IntPtr ptr;
        ptr = IntPtr(std::make_unique<int>(3));
        EXPECT_EQ(*ptr.toUnderType(), 3);
    }

    {
        IntPtr ptr(std::make_unique<int>(3));
        EXPECT_EQ(*ptr.toUnderType(), 3);
    }
}

TEST(StrongTypedef, NoDefaultCtor)
{
    struct NoDefaultCtor
    {
        NoDefaultCtor(int) {} // NOLINT
    };

    STRONG_TYPEDEF(NoDefaultCtor, MyStruct)
    MyStruct m(1);
}
