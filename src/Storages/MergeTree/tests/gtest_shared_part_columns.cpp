#include <gtest/gtest.h>

#include <Storages/MergeTree/SharedPartColumns.h>
#include <Storages/ColumnsDescription.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNumberBase.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace
{

class NonPoolableSerialization : public SerializationNumber<UInt64>
{
public:
    bool supportsPooling() const override { return false; }
};

class NonPoolableDataType : public DataTypeNumberBase<UInt64>
{
public:
    bool equals(const IDataType & rhs) const override { return typeid(rhs) == typeid(*this); }

    SerializationPtr doGetSerialization(const SerializationInfoSettings &) const override
    {
        return std::make_shared<NonPoolableSerialization>();
    }
};

}

TEST(SharedPartColumns, NonPoolableSerializationsAreNotShared)
{
    NamesAndTypesList columns{
        {"id", DataTypeFactory::instance().get("UInt64")},
        {"data", std::make_shared<NonPoolableDataType>()},
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
    EXPECT_FALSE(first->tryGet("data")->supportsPooling());
    EXPECT_EQ(first->tryGet("id"), second->tryGet("id"));
}

TEST(SharedPartColumns, TypedJSONSubcolumnsAreIncluded)
{
    const auto & context_holder = getContext();
    ASSERT_TRUE(context_holder.context != nullptr);

    NamesAndTypesList columns{{"data", DataTypeFactory::instance().get("JSON(typed UInt64)")}};
    auto description = std::make_shared<const ColumnsDescription>(columns);
    SharedPartColumns bundle(columns, description, description, false, SharedPartColumns::describeColumns(columns));

    SerializationInfoByName infos{SerializationInfoSettings{}};
    auto serialization = bundle.getSerializations(infos)->tryGet("data.typed");
    ASSERT_NE(serialization, nullptr);
    EXPECT_TRUE(serialization->supportsPooling());
}

TEST(SharedPartColumns, VariantSubcolumnsMatchOnTheFlySerializations)
{
    const auto & context_holder = getContext();
    ASSERT_TRUE(context_holder.context != nullptr);

    /// A `Variant` element is wrapped into `Nullable` when its type allows it, so enumerating the subcolumns
    /// of any `Variant` needs the type - with no `Object` involved.
    auto type = DataTypeFactory::instance().get("Variant(UInt64, UInt8)");
    NamesAndTypesList columns{{"data", type}};
    auto description = std::make_shared<const ColumnsDescription>(columns);
    SharedPartColumns bundle(columns, description, description, false, SharedPartColumns::describeColumns(columns));

    SerializationInfoByName infos{SerializationInfoSettings{}};
    auto serializations = bundle.getSerializations(infos);
    auto column_serialization = serializations->tryGet("data");
    ASSERT_NE(column_serialization, nullptr);

    auto subcolumn_names = type->getSubcolumnNames();
    ASSERT_TRUE(std::ranges::find(subcolumn_names, "UInt64") != subcolumn_names.end());
    for (const auto & subcolumn : subcolumn_names)
    {
        auto serialization = serializations->tryGet("data." + subcolumn);
        ASSERT_NE(serialization, nullptr) << subcolumn;
        if (serialization->supportsPooling())
            EXPECT_EQ(serialization, type->getSubcolumnSerialization(subcolumn, column_serialization)) << subcolumn;
    }
}

TEST(SharedPartColumns, WrappedJSONSubcolumnsMatchOnTheFlySerializations)
{
    const auto & context_holder = getContext();
    ASSERT_TRUE(context_holder.context != nullptr);

    auto type = DataTypeFactory::instance().get("Variant(JSON(typed UInt64), UInt8)");
    NamesAndTypesList columns{{"data", type}};
    auto description = std::make_shared<const ColumnsDescription>(columns);
    SharedPartColumns bundle(columns, description, description, false, SharedPartColumns::describeColumns(columns));

    SerializationInfoByName infos{SerializationInfoSettings{}};
    auto serializations = bundle.getSerializations(infos);
    auto column_serialization = serializations->tryGet("data");
    ASSERT_NE(column_serialization, nullptr);

    /// The serializations of the subcolumns of a wrapped `JSON` depend on the enclosing types,
    /// so they must be the same as the ones built on the fly from the serialization of the column.
    auto subcolumn_names = type->getSubcolumnNames();
    ASSERT_TRUE(std::ranges::find(subcolumn_names, "JSON(typed UInt64).typed") != subcolumn_names.end());
    for (const auto & subcolumn : subcolumn_names)
    {
        auto serialization = serializations->tryGet("data." + subcolumn);
        ASSERT_NE(serialization, nullptr) << subcolumn;
        /// Only a poolable serialization is the same object as an equal one built again.
        if (serialization->supportsPooling())
            EXPECT_EQ(serialization, type->getSubcolumnSerialization(subcolumn, column_serialization)) << subcolumn;
    }
}
