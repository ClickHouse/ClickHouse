#include <gtest/gtest.h>
#include <Core/dispatchOverTypes.h>
#include <base/EnumReflection.h>
#include <base/TypeName.h>

using namespace DB;

template <class L, class R>
void checkForward()
{
    if constexpr (!std::is_same_v<L, R>)
        FAIL() << fmt::format("Type mismatch: expected {}, found {}", TypeName<L>, TypeName<R>);
    else
        SUCCEED();
}

template <class T>
constexpr void checkBackward(TypeIndex index)
{
    constexpr TypeIndex real = TypeId<T>;

    // special cases listed in TypeId.h:102
    if (index == TypeIndex::Enum8)
        EXPECT_EQ(real, TypeIndex::Int8);
    else if (index == TypeIndex::Enum16)
        EXPECT_EQ(real, TypeIndex::Int16);
    else if (index == TypeIndex::Date)
        EXPECT_EQ(real, TypeIndex::UInt16);
    else if (index == TypeIndex::Date32)
        EXPECT_EQ(real, TypeIndex::Int32);
    else if (index == TypeIndex::DateTime)
        EXPECT_EQ(real, TypeIndex::UInt32);
    else
        EXPECT_EQ(index, real);
}

TEST(DispatchOverType, Type)
{
    int types_with_reverse_typeid = 0;
    int call_count = 0;

    static_for<TypeIndex>([&](auto index)
    {
        if constexpr (HasReverseTypeId<index>)
        {
            ++types_with_reverse_typeid;

            const bool was_called = dispatchOverType(index, [&]<class T>(TypePair<void, T>)
            {
                checkForward<T, ReverseTypeId<index>>();
                checkBackward<T>(index);

                ++call_count;

                return true;
            });

            /// String, UUID, and Array have ReverseTypeId but are not checked while dispatching on types
            if constexpr (
                index != TypeIndex::String
                && index != TypeIndex::Array
                && index != TypeIndex::UUID)
                EXPECT_TRUE(was_called);
            else
                EXPECT_FALSE(was_called);
        }

        return false;
    });

    /// Same as line 54
    EXPECT_EQ(call_count, types_with_reverse_typeid - 3);
}

TEST(DispatchOverType, DataType)
{
    int types_with_reverse_datatypeid = 0;
    int call_count = 0;

    static_for<TypeIndex>([&](auto index)
    {
        if constexpr (HasReverseDataTypeId<index>)
        {
            ++types_with_reverse_datatypeid;

            EXPECT_EQ(dispatchOverDataType(index, [&]<class T>(TypePair<void, T>)
            {
                ++call_count;
                checkForward<T, ReverseDataTypeId<index>>();
                return true;
            }), index != TypeIndex::Array);
        }

        return false;
    });

    /// Array is not matched while iterating over datatypes
    EXPECT_EQ(call_count, types_with_reverse_datatypeid - 1);
}

TEST(DispatchOverType, Types)
{
    using namespace ::DB::detail;
    constexpr CTArray all = Ints || Floats || Decimals || DateTimes;

    for (TypeIndex left : all)
        for (TypeIndex right : all)
            EXPECT_TRUE(dispatchOverTypes(left, right, [left, right]<class Left, class Right>(TypePair<Left, Right>)
            {
                checkBackward<Left>(left);
                checkBackward<Right>(right);
                return true;
            }));
}

template <Dispatch D, CTArray Arr>
void checkWithConstraints()
{
    static_for<TypeIndex>([](auto type)
    {
        if constexpr(HasReverseTypeId<type> && !D.other) // FixedString in Other cannot be passed to ReverseTypeId
            EXPECT_EQ(dispatchOverType<D>(type, [](auto) { return true; }), Arr.contains(type));

        if constexpr (HasReverseDataTypeId<type>)
            EXPECT_EQ(dispatchOverDataType<D>(type, [](auto) { return true; }), Arr.contains(type));

        return false;
    });
}

// Check that for each type constrained with Dispatch dispatch functor gets called (and does not get called for any
// type not satisfying constraints).
TEST(DispatchOverType, MatchWithConstraints)
{
    using namespace ::DB::detail;
    checkWithConstraints<Dispatch{ .ints = true }, Ints>();
    checkWithConstraints<Dispatch{ .floats = true }, Floats>();
    checkWithConstraints<Dispatch{ .decimals = true }, Decimals>();
    checkWithConstraints<Dispatch{ .datetimes = true }, DateTimes>();
    checkWithConstraints<Dispatch{ .other = true }, Other>();

    checkWithConstraints<Dispatch{ .ints = true, .floats = true }, Ints || Floats>();
    checkWithConstraints<Dispatch{ .decimals = true, .other = true }, Decimals || Other>();

    checkWithConstraints<DISPATCH_ALL, Ints || Floats || Decimals || DateTimes>();
    checkWithConstraints<DISPATCH_ALL_DT, Ints || Floats || Decimals || DateTimes || Other>();
}
