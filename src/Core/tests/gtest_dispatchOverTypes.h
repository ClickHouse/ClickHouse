#include <gtest/gtest.h>
#include <common/extended_types.h>
#include <Core/dispatchOverTypes.h>

using namespace DB;

TEST(DispatchOverType, Basic)
{
    auto res = dispatchOverType(TypeIndex::Int8, []<class Type>(TypePair<void, Type>)
    {
        static_assert(std::is_same_v<Type, Int8>);
        return true;
    });

    EXPECT_TRUE(res);
}

TEST(DispatchOverType, NoMatch)
{
    constexpr Dispatch d { ._float = true };

    auto res = dispatchOverType<d>(TypeIndex::Int8, []<class Type>(TypePair<void, Type>)
    {
        return true;
    });

    EXPECT_FALSE(res); // no type match, so functor will not be called
}

TEST(DispatchOverType, BasicTypes)
{
    auto res = dispatchOverTypes(TypeIndex::Int8, TypeIndex::Float32, []<class L, class R>(TypePair<L, R>)
    {
        static_assert(std::is_same_v<L, Int8> && std::is_same_v<R, Float32>);
        return true;
    });

    EXPECT_TRUE(res);
}
