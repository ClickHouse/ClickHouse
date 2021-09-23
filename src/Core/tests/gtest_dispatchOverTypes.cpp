#include <gtest/gtest.h>
#include <Core/dispatchOverTypes.h>
#include <common/EnumReflection.h>

#include <Core/Field.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>

using namespace DB;
using namespace DB::detail;

// Emulating type checker
template <class L, class R>
void check()
{
    if constexpr(!std::is_same_v<L, R>)
        FAIL() << fmt::format("Type mismatch: expected {}, found {}",
            typeid(L).name(), typeid(R).name());
    else
        SUCCEED();
}

/*
 * For every element in TypeIndex that can be invoked with ReverseTypeId<> check that invoking
 * dispatchOverType succeeds and:
 * - Produced type in inner lambda equals ReverseTypeId<element> (direct transform, 1).
 * - TypeId on produced type returns the initial TypeIndex element (backward transform, 2).
 *
 * For every element in TypeIndex that can be invoked with ReverseDataTypeId<> check that invoking
 * dispatchOverDataType succeeds and:
 * - Produced type in inner lambda equals ReverseDataTypeId<element> (direct transform, 3).
 *
 * Tests are intentionally made runtime (instead of using static_asserts) as type mismatch in compile time is just
 * "static assertion failed".
 */
GTEST_TEST(DispatchOverType, TypeAndDataType)
{
    static_for<TypeIndex>([](auto type_index_value)
    {
        if constexpr(HasReverseTypeId<type_index_value>)
            EXPECT_TRUE(dispatchOverType(type_index_value, [type_index_value]<class Type>(TypePair<void, Type>)
            {
                check<Type, ReverseTypeId<type_index_value>>(); /// 1
                EXPECT_EQ(TypeId<Type>, type_index_value); /// 2
                return true;
            }));

        if constexpr(HasReverseDataTypeId<type_index_value>)
            EXPECT_TRUE(dispatchOverDataType(type_index_value, [type_index_value]<class Type>(TypePair<void, Type>)
            {
                check<Type, ReverseDataTypeId<type_index_value>>(); /// 3
                return true;
            }));

        return false;
    });
}

GTEST_TEST(DispatchOverType, TypesAndDataTypes)
{
    static_for<TypeIndex>([](auto left)
    {
        static_for<TypeIndex>([left](auto right)
        {
            if constexpr(HasReverseTypeId<left> && HasReverseTypeId<right>)
                EXPECT_TRUE(dispatchOverTypes(left, right,
                    [left, right]<class Left, class Right>(TypePair<Left, Right>)
                {
                    check<Left, ReverseTypeId<left>>();
                    check<Right, ReverseTypeId<right>>();

                    EXPECT_EQ(TypeId<Left>, left);
                    EXPECT_EQ(TypeId<Right>, right);

                    return true;
                }));

            if constexpr(HasReverseDataTypeId<left> && HasReverseDataTypeId<right>)
                EXPECT_TRUE(dispatchOverDataTypes(left, right,
                    [left, right]<class Left, class Right>(TypePair<Left, Right>)
                {
                    check<Left, ReverseDataTypeId<left>>();
                    check<Right, ReverseDataTypeId<right>>();

                    return true;
                }));

            return false;
        });
    });
}

TEST(DispatchOverType, NoMatch)
{
    constexpr Dispatch d { ._float = true };
    constexpr Dispatch empty { };

    EXPECT_FALSE(dispatchOverType<d>(TypeIndex::Int8, [](auto) { return true; })); // no type match
    EXPECT_FALSE(dispatchOverType<empty>(TypeIndex::Int8, [](auto) { return true; })); //no iteration

    EXPECT_FALSE(dispatchOverDataType<d>(TypeIndex::Int8, [](auto) { return true; })); // no type match
    EXPECT_FALSE(dispatchOverDataType<empty>(TypeIndex::Int8, [](auto) { return true; })); //no iteration
}

template <Dispatch d, CTArray a>
void checkWithConstraints()
{
    static_for<TypeIndex>([](auto type)
    {
        constexpr bool expected = a.contains(type);

        if constexpr(HasReverseTypeId<type>)
            EXPECT_EQ(dispatchOverType<d>(type, [](auto) { return true; }), expected);

        if constexpr(HasReverseDataTypeId<type>)
            EXPECT_EQ(dispatchOverDataType<d>(type, [](auto) { return true; }), expected);

        return false;
    });
}

TEST(DispatchOverType, MatchWithConstraints)
{
    checkWithConstraints<Dispatch{ ._int = true }, Ints>();
    checkWithConstraints<Dispatch{ ._float = true }, Floats>();
    checkWithConstraints<Dispatch{ ._decimal = true }, Decimals>();
    checkWithConstraints<Dispatch{ ._datetime = true }, DateTimes>();
}
