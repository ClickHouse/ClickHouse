#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

using namespace DB;

/// DataTypeFactory::tryGet promises to return nullptr (not throw) on invalid type text.
/// The specialized ASTEnumDataType / ASTTupleDataType branches in getImpl(const ASTPtr &)
/// construct the type directly, bypassing the registered-creator try/catch, so they have to
/// honor nullptr_on_error themselves. An out-of-range enum value (or a malformed tuple) used
/// to escape through tryGet as an exception, breaking callers such as Dynamic-subcolumn
/// type detection that treat the null result as the "not a type" signal.
/// See: https://github.com/ClickHouse/ClickHouse/pull/107081

TEST(DataTypeFactoryTryGet, OutOfRangeEnumReturnsNullptr)
{
    auto & factory = DataTypeFactory::instance();

    /// Value above Enum8 range: tryGet must return nullptr, not throw ARGUMENT_OUT_OF_BOUND.
    EXPECT_EQ(factory.tryGet("Enum8('a' = 200)"), nullptr);
    EXPECT_EQ(factory.tryGet("Enum8('a' = -200)"), nullptr);
    EXPECT_EQ(factory.tryGet("Enum16('a' = 40000)"), nullptr);
    /// Above Int64 (read as UInt64 by the parser) must also be nullptr, not throw.
    EXPECT_EQ(factory.tryGet("Enum8('a' = 18446744073709551615)"), nullptr);
    /// Int64-max base + auto-assigned element (overflow at synthesis) must be nullptr, not throw.
    EXPECT_EQ(factory.tryGet("Enum8('z' = 9223372036854775807, 'a')"), nullptr);
}

TEST(DataTypeFactoryTryGet, MalformedTupleReturnsNullptr)
{
    auto & factory = DataTypeFactory::instance();

    /// Mixed named/unnamed tuple element resolves to an unknown nested type; the specialized
    /// tuple branch must honor nullptr_on_error too.
    EXPECT_EQ(factory.tryGet("Tuple(a Int8, b)"), nullptr);
}

TEST(DataTypeFactoryTryGet, GetStillThrowsOnOutOfRangeEnum)
{
    auto & factory = DataTypeFactory::instance();

    /// The non-try get() path must keep rejecting out-of-range enum values.
    EXPECT_THROW(factory.get("Enum8('a' = 200)"), Exception);
    EXPECT_THROW(factory.get("Enum8('a' = 18446744073709551615)"), Exception);
    EXPECT_THROW(factory.get("Enum8('z' = 9223372036854775807, 'a')"), Exception);
}

TEST(DataTypeFactoryTryGet, ValidEnumStillParses)
{
    auto & factory = DataTypeFactory::instance();

    /// Valid enum type text must still parse via both tryGet and get.
    auto try_type = factory.tryGet("Enum8('a' = 100)");
    ASSERT_NE(try_type, nullptr);
    EXPECT_EQ(try_type->getName(), "Enum8('a' = 100)");

    auto get_type = factory.get("Enum8('a' = 100)");
    ASSERT_NE(get_type, nullptr);
    EXPECT_EQ(get_type->getName(), "Enum8('a' = 100)");
}
