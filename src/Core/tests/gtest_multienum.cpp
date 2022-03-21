#include <gtest/gtest.h>

#include <base/types.h>
#include <type_traits>
#include <Core/MultiEnum.h>

namespace
{

using namespace DB;
enum class TestEnum : UInt8
{
    // name represents which bit is going to be set
    ZERO,
    ONE,
    TWO,
    THREE,
    FOUR,
    FIVE
};
}

GTEST_TEST(MultiEnum, WithDefault)
{
    MultiEnum<TestEnum, UInt8> multi_enum;
    ASSERT_EQ(0, multi_enum.getValue());
    ASSERT_EQ(0, multi_enum);

    ASSERT_FALSE(multi_enum.isSet(TestEnum::ZERO));
    ASSERT_FALSE(multi_enum.isSet(TestEnum::ONE));
    ASSERT_FALSE(multi_enum.isSet(TestEnum::TWO));
    ASSERT_FALSE(multi_enum.isSet(TestEnum::THREE));
    ASSERT_FALSE(multi_enum.isSet(TestEnum::FOUR));
    ASSERT_FALSE(multi_enum.isSet(TestEnum::FIVE));
}

GTEST_TEST(MultiEnum, WitheEnum)
{
    MultiEnum<TestEnum, UInt8> multi_enum(TestEnum::FOUR);
    ASSERT_EQ(16, multi_enum.getValue());
    ASSERT_EQ(16, multi_enum);

    ASSERT_FALSE(multi_enum.isSet(TestEnum::ZERO));
    ASSERT_FALSE(multi_enum.isSet(TestEnum::ONE));
    ASSERT_FALSE(multi_enum.isSet(TestEnum::TWO));
    ASSERT_FALSE(multi_enum.isSet(TestEnum::THREE));
    ASSERT_TRUE(multi_enum.isSet(TestEnum::FOUR));
    ASSERT_FALSE(multi_enum.isSet(TestEnum::FIVE));
}

GTEST_TEST(MultiEnum, WithValue)
{
    const MultiEnum<TestEnum> multi_enum(13u); // (1 | (1 << 2 | 1 << 3)

    ASSERT_TRUE(multi_enum.isSet(TestEnum::ZERO));
    ASSERT_FALSE(multi_enum.isSet(TestEnum::ONE));
    ASSERT_TRUE(multi_enum.isSet(TestEnum::TWO));
    ASSERT_TRUE(multi_enum.isSet(TestEnum::THREE));
    ASSERT_FALSE(multi_enum.isSet(TestEnum::FOUR));
    ASSERT_FALSE(multi_enum.isSet(TestEnum::FIVE));
}

GTEST_TEST(MultiEnum, WithMany)
{
    MultiEnum<TestEnum> multi_enum{TestEnum::ONE, TestEnum::FIVE};
    ASSERT_EQ(1 << 1 | 1 << 5, multi_enum.getValue());
    ASSERT_EQ(1 << 1 | 1 << 5, multi_enum);

    ASSERT_FALSE(multi_enum.isSet(TestEnum::ZERO));
    ASSERT_TRUE(multi_enum.isSet(TestEnum::ONE));
    ASSERT_FALSE(multi_enum.isSet(TestEnum::TWO));
    ASSERT_FALSE(multi_enum.isSet(TestEnum::THREE));
    ASSERT_FALSE(multi_enum.isSet(TestEnum::FOUR));
    ASSERT_TRUE(multi_enum.isSet(TestEnum::FIVE));
}

GTEST_TEST(MultiEnum, WithCopyConstructor)
{
    const MultiEnum<TestEnum> multi_enum_source{TestEnum::ONE, TestEnum::FIVE};
    MultiEnum<TestEnum> multi_enum{multi_enum_source};

    ASSERT_EQ(1 << 1 | 1 << 5, multi_enum.getValue());
}

GTEST_TEST(MultiEnum, SetAndUnSet)
{
    MultiEnum<TestEnum> multi_enum;
    multi_enum.set(TestEnum::ONE);
    ASSERT_EQ(1 << 1, multi_enum);

    multi_enum.set(TestEnum::TWO);
    ASSERT_EQ(1 << 1| (1 << 2), multi_enum);

    multi_enum.unSet(TestEnum::ONE);
    ASSERT_EQ(1 << 2, multi_enum);
}

GTEST_TEST(MultiEnum, SetValueOnDifferentTypes)
{
    MultiEnum<TestEnum> multi_enum;

    multi_enum.setValue(static_cast<UInt8>(1));
    ASSERT_EQ(1, multi_enum);

    multi_enum.setValue(static_cast<UInt16>(2));
    ASSERT_EQ(2, multi_enum);

    multi_enum.setValue(static_cast<UInt32>(3));
    ASSERT_EQ(3, multi_enum);

    multi_enum.setValue(static_cast<UInt64>(4));
    ASSERT_EQ(4, multi_enum);
}

// shouldn't compile
//GTEST_TEST(MultiEnum, WithOtherEnumType)
//{
//    MultiEnum<TestEnum> multi_enum;

//    enum FOO {BAR, FOOBAR};
//    MultiEnum<TestEnum> multi_enum2(BAR);
//    MultiEnum<TestEnum> multi_enum3(BAR, FOOBAR);
//    multi_enum.setValue(FOO::BAR);
//    multi_enum == FOO::BAR;
//    FOO::BAR == multi_enum;
//}

GTEST_TEST(MultiEnum, SetSameValueMultipleTimes)
{
    // Setting same value is idempotent.
    MultiEnum<TestEnum> multi_enum;
    multi_enum.set(TestEnum::ONE);
    ASSERT_EQ(1 << 1, multi_enum);

    multi_enum.set(TestEnum::ONE);
    ASSERT_EQ(1 << 1, multi_enum);
}

GTEST_TEST(MultiEnum, UnSetValuesThatWerentSet)
{
    // Unsetting values that weren't set shouldn't change other flags nor aggregate value.
    MultiEnum<TestEnum> multi_enum{TestEnum::ONE, TestEnum::THREE};
    multi_enum.unSet(TestEnum::TWO);
    ASSERT_EQ(1 << 1 | 1 << 3, multi_enum);

    multi_enum.unSet(TestEnum::FOUR);
    ASSERT_EQ(1 << 1 | 1 << 3, multi_enum);

    multi_enum.unSet(TestEnum::FIVE);
    ASSERT_EQ(1 << 1 | 1 << 3, multi_enum);
}

GTEST_TEST(MultiEnum, Reset)
{
    MultiEnum<TestEnum> multi_enum{TestEnum::ONE, TestEnum::THREE};
    multi_enum.reset();
    ASSERT_EQ(0, multi_enum);
}
