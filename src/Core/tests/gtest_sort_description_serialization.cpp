#include <gtest/gtest.h>

#include <Core/ProtocolDefines.h>
#include <Core/SortDescription.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

/// A plain ascending `WITH FILL FROM 0 TO 100 STEP 2` on one column.
SortColumnDescription makeFillColumn(int direction = 1)
{
    SortColumnDescription desc("a");
    desc.direction = direction;
    desc.with_fill = true;
    desc.fill_description.fill_from = Field(UInt64(0));
    desc.fill_description.fill_to = Field(UInt64(100));
    desc.fill_description.fill_step = Field(Int64(direction > 0 ? 2 : -2));
    return desc;
}

std::string serialize(const SortColumnDescription & column)
{
    SortDescription description;
    description.push_back(column);
    WriteBufferFromOwnString out;
    serializeSortDescription(description, out, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    return out.str();
}

SortDescription deserialize(const std::string & blob)
{
    SortDescription description;
    ReadBufferFromString in(blob);
    deserializeSortDescription(description, in, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    return description;
}

/// The writer refuses nothing but the version, so an invalid description is built by serializing it and
/// reading the bytes back - which is exactly the shape of a forged client plan.
void expectRejected(const SortColumnDescription & column, const char * what)
{
    try
    {
        deserialize(serialize(column));
        FAIL() << "expected an exception for " << what;
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::INCORRECT_DATA) << what << ": " << e.message();
    }
}

}

GTEST_TEST(SortDescriptionSerialization, WithFillRoundTrip)
{
    auto column = makeFillColumn();
    column.alias = "a_alias";
    column.nulls_direction = -1;
    column.fill_description.step_kind = IntervalKind(IntervalKind::Kind::Second);

    const auto description = deserialize(serialize(column));

    ASSERT_EQ(description.size(), 1u);
    const auto & restored = description.front();
    ASSERT_EQ(restored.column_name, column.column_name);
    ASSERT_EQ(restored.alias, column.alias);
    ASSERT_EQ(restored.direction, column.direction);
    ASSERT_EQ(restored.nulls_direction, column.nulls_direction);
    ASSERT_TRUE(restored.with_fill);
    ASSERT_EQ(restored.fill_description.fill_from, column.fill_description.fill_from);
    ASSERT_EQ(restored.fill_description.fill_to, column.fill_description.fill_to);
    ASSERT_EQ(restored.fill_description.fill_step, column.fill_description.fill_step);
    ASSERT_TRUE(restored.fill_description.step_kind.has_value());
    ASSERT_EQ(restored.fill_description.step_kind->kind, IntervalKind::Kind::Second);
    ASSERT_FALSE(restored.fill_description.staleness_kind.has_value());
}

GTEST_TEST(SortDescriptionSerialization, WithoutFillRoundTrip)
{
    SortColumnDescription column("b");
    column.direction = -1;
    column.nulls_direction = 1;

    const auto description = deserialize(serialize(column));

    ASSERT_EQ(description.size(), 1u);
    ASSERT_EQ(description.front().column_name, "b");
    ASSERT_EQ(description.front().direction, -1);
    ASSERT_FALSE(description.front().with_fill);
}

GTEST_TEST(SortDescriptionSerialization, RejectsInvalidFillDescription)
{
    {
        auto column = makeFillColumn();
        column.fill_description.fill_step = Field(Int64(0));
        expectRejected(column, "a zero step");
    }
    {
        auto column = makeFillColumn();
        column.fill_description.fill_step = Field();
        expectRejected(column, "a null step");
    }
    {
        auto column = makeFillColumn();
        column.fill_description.fill_step = Field(String("2"));
        expectRejected(column, "a non-numeric step");
    }
    {
        auto column = makeFillColumn();
        column.fill_description.fill_step = Field(Int64(-2));
        expectRejected(column, "a negative step for an ascending sort");
    }
    {
        auto column = makeFillColumn(-1);
        column.fill_description.fill_step = Field(Int64(2));
        expectRejected(column, "a positive step for a descending sort");
    }
    {
        auto column = makeFillColumn();
        column.fill_description.fill_from = Field(UInt64(100));
        column.fill_description.fill_to = Field(UInt64(0));
        expectRejected(column, "TO below FROM for an ascending sort");
    }
    {
        auto column = makeFillColumn(-1);
        column.fill_description.fill_from = Field(UInt64(0));
        column.fill_description.fill_to = Field(UInt64(100));
        expectRejected(column, "FROM below TO for a descending sort");
    }
    {
        auto column = makeFillColumn();
        column.fill_description.fill_staleness = Field(Int64(5));
        expectRejected(column, "STALENESS together with FROM");
    }
    {
        auto column = makeFillColumn();
        column.fill_description.fill_from = Field();
        column.fill_description.fill_staleness = Field(Int64(-5));
        expectRejected(column, "a negative staleness for an ascending sort");
    }
    {
        auto column = makeFillColumn();
        column.fill_description.fill_from = Field();
        column.fill_description.staleness_kind = IntervalKind(IntervalKind::Kind::Second);
        expectRejected(column, "a staleness interval without a value");
    }
}

GTEST_TEST(SortDescriptionSerialization, RejectsMalformedBytes)
{
    const auto blob = serialize(makeFillColumn());

    /// Flags byte: right after the 1-byte-length column name "a".
    const size_t sort_flags_pos = 3;
    ASSERT_EQ(UInt8(blob[sort_flags_pos]) & 0x08, 0x08) << "expected the with_fill bit here";

    std::string unknown_flag = blob;
    unknown_flag[sort_flags_pos] = char(UInt8(blob[sort_flags_pos]) | 0x10);
    try
    {
        deserialize(unknown_flag);
        FAIL() << "expected an exception for an unknown flag bit";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::INCORRECT_DATA) << e.message();
    }

    /// An out-of-range `IntervalKind`. The step kind is written right before the (null) staleness field,
    /// so its byte is the one before the last.
    auto with_kind = makeFillColumn();
    with_kind.fill_description.step_kind = IntervalKind(IntervalKind::Kind::Second);
    std::string bad_kind = serialize(with_kind);
    bad_kind[bad_kind.size() - 2] = char(0x7F);
    try
    {
        deserialize(bad_kind);
        FAIL() << "expected an exception for an out-of-range IntervalKind";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::INCORRECT_DATA) << e.message();
    }
}
