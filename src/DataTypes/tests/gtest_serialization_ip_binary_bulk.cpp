#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <Common/transformEndianness.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <bit>
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
    serialization->deserializeBinaryBulk(*restored, istr, 2, 0);

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

/// This machine is little-endian, so `writeBinaryLittleEndian`/`readBinaryLittleEndian` never
/// actually swap here: `transformEndianness<ToEndian, FromEndian = native>`'s `if constexpr
/// (ToEndian != FromEndian)` is false at compile time and the `std::byteswap` branch doesn't
/// exist in this binary. `transformEndianness` takes `FromEndian` as an independent template
/// argument rather than hard-coding `native`, so passing it explicitly (`<little, big>`) forces
/// the exact same branch a big-endian host takes by default -- same instantiation, same
/// std::byteswap call -- letting these branches be verified without big-endian hardware.

TEST(SerializationIPBinaryBulk, IPv4TransformEndiannessMatchesStdByteswap)
{
    for (UInt32 raw : {0x01020304u, 0xC0A80001u, 0xFFFFFFFFu, 0x00000000u})
    {
        IPv4 addr(raw);
        transformEndianness<std::endian::little, std::endian::big>(addr);
        ASSERT_EQ(addr.toUnderType(), std::byteswap(raw));
    }
}

TEST(SerializationIPBinaryBulk, IPv4BigEndianHostWireBytesMatchLittleEndianHost)
{
    /// What a big-endian host's writeBinaryLittleEndian(addr) would put on the wire: swap addr
    /// (the branch above), then memcpy its bytes MSB-first (that's what "native" means there).
    auto bigEndianHostWireBytes = [](UInt32 raw) -> std::string
    {
        IPv4 addr(raw);
        transformEndianness<std::endian::little, std::endian::big>(addr);
        UInt32 swapped = addr.toUnderType();
        std::string native_bytes(reinterpret_cast<const char *>(&swapped), sizeof(swapped));
        /// On a big-endian host the native bytes already are what its memcpy puts on the wire; a
        /// little-endian host's native order is the mirror image, so reversing emulates it.
        if constexpr (std::endian::native == std::endian::big)
            return native_bytes;
        return {native_bytes.rbegin(), native_bytes.rend()};
    };

    ASSERT_EQ(bigEndianHostWireBytes(0x01020304u), (std::string{"\x04\x03\x02\x01", 4}));
    ASSERT_EQ(bigEndianHostWireBytes(0xC0A80001u), (std::string{"\x01\x00\xA8\xC0", 4}));

    /// Same values, same expected bytes as IPv4WireFormatIsLittleEndian's little-endian-host path.
    auto type = DataTypeFactory::instance().get("IPv4");
    auto col = type->createColumn();
    auto & data = assert_cast<ColumnVector<IPv4> &>(*col).getData();
    data.push_back(IPv4(0x01020304));
    data.push_back(IPv4(0xC0A80001));
    WriteBufferFromOwnString ostr;
    type->getDefaultSerialization()->serializeBinaryBulk(*col, ostr, 0, 0);
    ASSERT_EQ(ostr.str(), bigEndianHostWireBytes(0x01020304u) + bigEndianHostWireBytes(0xC0A80001u));
}

TEST(SerializationIPBinaryBulk, IPv4BigEndianHostReadReconstructsOriginalValue)
{
    /// The inverse: readBinaryLittleEndian on a big-endian host first memcpy's the little-endian
    /// wire bytes MSB-first (producing the byte-reversed value below), then swaps -- reconstructing
    /// the original. transformEndianness is its own inverse (byteswap undoes byteswap), so the
    /// same explicit-FromEndian trick applies here too.
    const std::string wire_bytes{"\x04\x03\x02\x01", 4}; /// wire format for IPv4(0x01020304)
    /// A big-endian host's memcpy keeps the wire bytes as-is; a little-endian one mirrors them.
    std::string be_native_bytes;
    if constexpr (std::endian::native == std::endian::big)
        be_native_bytes = wire_bytes;
    else
        be_native_bytes.assign(wire_bytes.rbegin(), wire_bytes.rend());
    UInt32 be_native_value = 0;
    memcpy(&be_native_value, be_native_bytes.data(), sizeof(be_native_value));

    IPv4 addr(be_native_value);
    transformEndianness<std::endian::little, std::endian::big>(addr);
    ASSERT_EQ(addr, IPv4(0x01020304));
}
