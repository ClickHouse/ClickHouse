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
    EXPECT_EQ(first->tryGet("id"), second->tryGet("id"));
}
