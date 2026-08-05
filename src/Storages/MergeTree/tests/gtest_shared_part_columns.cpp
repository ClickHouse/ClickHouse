#include <gtest/gtest.h>

#include <Storages/MergeTree/SharedPartColumns.h>
#include <Storages/ColumnsDescription.h>
#include <DataTypes/DataTypeFactory.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

/// A serialization that reports no pooling keeps mutable state, so the parts of a table must not share
/// one: `SerializationJSON` accumulates caches inside its extraction tree and picks its parser from the
/// settings of the query that built it. The serializations of the other columns are still shared.
TEST(SharedPartColumns, NonPoolableSerializationsAreNotShared)
{
    /// The `JSON` serialization reads `allow_simdjson` from the query context, or the global one.
    const auto & context_holder = getContext();
    ASSERT_TRUE(context_holder.context != nullptr);

    NamesAndTypesList columns{
        {"id", DataTypeFactory::instance().get("UInt64")},
        {"data", DataTypeFactory::instance().get("JSON")},
    };

    auto description = std::make_shared<const ColumnsDescription>(columns);
    SharedPartColumns bundle(columns, description, description, false, SharedPartColumns::describeColumns(columns));

    SerializationInfoByName infos{SerializationInfoSettings{}};
    auto first = bundle.getSerializations(infos);
    auto second = bundle.getSerializations(infos);

    ASSERT_TRUE(first != nullptr && second != nullptr);
    /// `data` makes the whole object unshareable, so it is not interned either.
    EXPECT_NE(first, second);
    EXPECT_NE(first->tryGet("data"), second->tryGet("data"));
    EXPECT_EQ(first->tryGet("id"), second->tryGet("id"));
}
