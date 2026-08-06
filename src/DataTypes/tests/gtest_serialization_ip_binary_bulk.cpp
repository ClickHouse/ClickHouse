#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

using namespace DB;

/// IPv4 binary serialization uses the little-endian wire format on every host. The bulk
/// overloads are used by SerializationDynamic / SerializationVariant when a column reaches
/// parts or Native, so they must produce the same bytes as the row-wise overloads,
/// otherwise data written on a big-endian host is not portable.

TEST(SerializationIPBinaryBulk, IPv4WireFormatIsLittleEndian)
{
    auto type = DataTypeFactory::instance().get("IPv4");
    auto col = type->createColumn();
    auto & data = assert_cast<ColumnVector<IPv4> &>(*col).getData();
    data.push_back(IPv4(0x01020304)); /// 1.2.3.4
    data.push_back(IPv4(0xC0A80001)); /// 192.168.0.1

    WriteBufferFromOwnString ostr;
    type->getDefaultSerialization()->serializeBinaryBulk(*col, ostr, 0, 0);
    ASSERT_EQ(ostr.str(), (std::string{"\x04\x03\x02\x01\x01\x00\xA8\xC0", 8}));
}

TEST(SerializationIPBinaryBulk, IPv4BulkMatchesRowWise)
{
    auto type = DataTypeFactory::instance().get("IPv4");
    auto serialization = type->getDefaultSerialization();
    auto col = type->createColumn();
    auto & data = assert_cast<ColumnVector<IPv4> &>(*col).getData();
    data.push_back(IPv4(0x01020304));
    data.push_back(IPv4(0xC0A80001));
    data.push_back(IPv4(0xFFFFFFFF));

    WriteBufferFromOwnString bulk;
    serialization->serializeBinaryBulk(*col, bulk, 0, 0);

    WriteBufferFromOwnString row_wise;
    for (size_t i = 0; i != col->size(); ++i)
        serialization->serializeBinary(*col, i, row_wise, FormatSettings{});

    ASSERT_EQ(bulk.str(), row_wise.str());
}

TEST(SerializationIPBinaryBulk, IPv4BulkRoundTrip)
{
    auto type = DataTypeFactory::instance().get("IPv4");
    auto serialization = type->getDefaultSerialization();
    auto col = type->createColumn();
    auto & data = assert_cast<ColumnVector<IPv4> &>(*col).getData();
    data.push_back(IPv4(0x01020304));
    data.push_back(IPv4(0xC0A80001));

    WriteBufferFromOwnString ostr;
    serialization->serializeBinaryBulk(*col, ostr, 0, 0);

    auto restored = type->createColumn();
    ReadBufferFromString istr(ostr.str());
    serialization->deserializeBinaryBulk(*restored, istr, 0, 2, 0);

    ASSERT_EQ(restored->size(), 2u);
    const auto & restored_data = assert_cast<const ColumnVector<IPv4> &>(*restored).getData();
    ASSERT_EQ(restored_data[0], IPv4(0x01020304));
    ASSERT_EQ(restored_data[1], IPv4(0xC0A80001));
}

TEST(SerializationIPBinaryBulk, IPv4FieldMatchesIColumn)
{
    /// The `Field`-based overload is what `MergeTree` minmax-index files (`minmax_<column>.idx`)
    /// are actually serialized through. This proves it agrees byte-for-byte with the `IColumn`
    /// overload, which is exactly the guarantee a little-endian-only wire format relies on.
    auto type = DataTypeFactory::instance().get("IPv4");
    auto serialization = type->getDefaultSerialization();

    Field field(IPv4(0x01020304));
    WriteBufferFromOwnString field_wise;
    serialization->serializeBinary(field, field_wise, FormatSettings{});

    auto col = type->createColumn();
    auto & data = assert_cast<ColumnVector<IPv4> &>(*col).getData();
    data.push_back(IPv4(0x01020304));

    WriteBufferFromOwnString row_wise;
    serialization->serializeBinary(*col, 0, row_wise, FormatSettings{});

    ASSERT_EQ(field_wise.str(), row_wise.str());
}
