#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnsNumber.h>
#include <Common/SipHash.h>
#include <Core/Field.h>
#include <Core/MergeTreeSerializationEnums.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <DataTypes/Serializations/SerializationMap.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <limits>
#include <map>
#include <memory>
#include <utility>
#include <vector>

using namespace DB;

namespace
{

constexpr size_t NUM_BUCKETS = 4;

/// A set of in-memory substreams, keyed by the substream name that a real data part would use
/// as a file name. It plays the role of the on-disk streams of a single column.
using Streams = std::map<String, String>;

DataTypePtr getMapType()
{
    return DataTypeFactory::instance().get("Map(String, UInt64)");
}

SerializationPtr getBucketedSerialization(const DataTypePtr & type)
{
    SerializationInfoSettings info_settings;
    info_settings.map_serialization_version = MergeTreeMapSerializationVersion::WITH_BUCKETS;
    return type->getSerialization(info_settings);
}

/// A `Map` column where every row has a different number of key-value pairs and the keys of a
/// single row land in several buckets in an order that is not the bucket order. Both properties
/// are needed to notice a bucket index stream that is out of sync with the bucket streams.
ColumnPtr makeTestColumn(const DataTypePtr & type, size_t num_rows)
{
    auto column = type->createColumn();
    for (size_t row = 0; row != num_rows; ++row)
    {
        Map map;
        for (size_t i = 0; i != 1 + row % 7; ++i)
            map.push_back(Tuple{"key_" + std::to_string(row) + "_" + std::to_string(i), UInt64(row * 100 + i)});
        column->insert(map);
    }
    return std::move(column);
}

Streams serializeWithBuckets(const DataTypePtr & type, const IColumn & column)
{
    std::map<String, std::unique_ptr<WriteBufferFromOwnString>> buffers;

    ISerialization::SerializeBinaryBulkSettings settings;
    settings.write_statistics = ISerialization::SerializeBinaryBulkSettings::StatisticsMode::PREFIX;
    settings.max_buckets_in_map = NUM_BUCKETS;
    settings.map_buckets_strategy = MergeTreeMapBucketsStrategy::CONSTANT;
    settings.getter = [&](const ISerialization::SubstreamPath & path) -> WriteBuffer *
    {
        auto name = ISerialization::getSubcolumnNameForStream(path);
        auto it = buffers.find(name);
        if (it == buffers.end())
            it = buffers.emplace(name, std::make_unique<WriteBufferFromOwnString>()).first;
        return it->second.get();
    };

    auto serialization = getBucketedSerialization(type);
    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization->serializeBinaryBulkStatePrefix(column, settings, state);
    serialization->serializeBinaryBulkWithMultipleStreams(column, 0, column.size(), settings, state);
    serialization->serializeBinaryBulkStateSuffix(settings, state);

    Streams streams;
    for (auto & [name, buffer] : buffers)
    {
        buffer->finalize();
        streams[name] = buffer->str();
    }
    return streams;
}

/// Reads `limit` rows out of the serialized streams.
/// `serialization` is either the whole-column serialization or a subcolumn one.
ColumnPtr deserializeRange(
    const SerializationPtr & serialization, const DataTypePtr & column_type, const Streams & streams, size_t limit)
{
    std::map<String, std::unique_ptr<ReadBufferFromString>> buffers;

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.read_statistics = true;
    settings.getter = [&](const ISerialization::SubstreamPath & path) -> ReadBuffer *
    {
        auto name = ISerialization::getSubcolumnNameForStream(path);
        auto stream_it = streams.find(name);
        if (stream_it == streams.end())
            return nullptr;

        auto it = buffers.find(name);
        if (it == buffers.end())
            it = buffers.emplace(name, std::make_unique<ReadBufferFromString>(stream_it->second)).first;
        return it->second.get();
    };
    settings.check_stream_exists_callback = [&](const ISerialization::SubstreamPath & path)
    {
        return streams.contains(ISerialization::getSubcolumnNameForStream(path));
    };

    ISerialization::DeserializeBinaryBulkStatePtr state;
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

    auto column = column_type->createColumn();
    serialization->deserializeBinaryBulkWithMultipleStreams(*column, limit, settings, state, nullptr);
    return std::move(column);
}

void assertColumnsEqual(const IColumn & expected, const IColumn & actual)
{
    ASSERT_EQ(expected.size(), actual.size());
    for (size_t row = 0; row != actual.size(); ++row)
        ASSERT_EQ(expected[row], actual[row]) << "at row " << row;
}

}

/// A multi-bucket `Map` round-trips through the in-memory substreams: the `bucket_indexes` stream
/// restores the original key order across all buckets.
TEST(MapBucketedSerialization, RoundTrip)
{
    auto type = getMapType();
    constexpr size_t num_rows = 20;
    auto column = makeTestColumn(type, num_rows);
    auto streams = serializeWithBuckets(type, *column);

    /// More than one bucket is what makes the bucket index stream necessary in the first place.
    ASSERT_TRUE(streams.contains("bucket_indexes"));

    auto serialization = getBucketedSerialization(type);
    auto result = deserializeRange(serialization, type, streams, num_rows);
    assertColumnsEqual(*column, *result);
}

/// The same holds for the `keys` subcolumn, which reassembles the keys of all buckets on its own.
TEST(MapBucketedSerialization, ReadKeysSubcolumn)
{
    auto type = getMapType();
    constexpr size_t num_rows = 20;
    auto column = makeTestColumn(type, num_rows);
    auto streams = serializeWithBuckets(type, *column);

    auto keys_type = type->getSubcolumnType("keys");
    auto keys_serialization = type->getSubcolumnSerialization("keys", getBucketedSerialization(type));
    auto result = deserializeRange(keys_serialization, keys_type, streams, num_rows);

    /// Ground truth: the keys subcolumn extracted directly from the source column.
    auto expected_keys = type->getSubcolumn("keys", column);
    assertColumnsEqual(*expected_keys, *result);
}

namespace
{

constexpr size_t NUM_KEYS = 1003;

/// The bucket counts whose buckets are pinned.
constexpr std::array<size_t, 3> PINNED_BUCKET_COUNTS = {2, 7, 256};

/// Deterministic bits of a row, spread over the whole word.
UInt64 rowBits(size_t row)
{
    UInt64 x = (row + 1) * 0x9E3779B97F4A7C15ULL;
    return x ^ (x >> 31);
}

UInt128 rowBits128(size_t row)
{
    return (UInt128(rowBits(row)) << 64) | UInt128(rowBits(row + NUM_KEYS));
}

/// `length` bytes, all 256 values occur over the rows.
String rowBytes(size_t row, size_t length)
{
    String value(length, '\0');
    for (size_t i = 0; i != length; ++i)
        value[i] = static_cast<char>((row * 131 + i * 31) % 256);
    return value;
}

/// Lengths 0..36 cross the 8-byte words of `SipHash`, and every 101st string is long.
String stringKey(size_t row)
{
    return rowBytes(row, row % 101 == 100 ? 300 + row % 50 : row % 37);
}

/// The extremes first, then values using all bits.
template <typename T>
Field integerKey(size_t row)
{
    T value;
    switch (row)
    {
        case 0: value = std::numeric_limits<T>::min(); break;
        case 1: value = std::numeric_limits<T>::max(); break;
        case 2: value = 0; break;
        case 3: value = 1; break;
        case 4: value = static_cast<T>(-1); break;
        default: value = static_cast<T>(rowBits(row)); break;
    }
    if constexpr (std::is_signed_v<T>)
        return Field(static_cast<Int64>(value));
    else
        return Field(static_cast<UInt64>(value));
}

Field uint128Key(size_t row)
{
    switch (row)
    {
        case 0: return Field(UInt128(0));
        case 1: return Field(std::numeric_limits<UInt128>::max());
        case 2: return Field(UInt128(1));
        default: return Field(rowBits128(row));
    }
}

Field int256Key(size_t row)
{
    switch (row)
    {
        case 0: return Field(std::numeric_limits<Int256>::min());
        case 1: return Field(std::numeric_limits<Int256>::max());
        case 2: return Field(Int256(0));
        case 3: return Field(Int256(-1));
        default:
        {
            Int256 value = (Int256(rowBits128(row)) << 128) | Int256(rowBits128(row + 2 * NUM_KEYS));
            return Field(row % 2 ? -value : value);
        }
    }
}

/// Both zeros (their bits differ), both NaNs, the infinities and the limits first, then the zeros again every 50th row.
template <typename T>
Field floatKey(size_t row)
{
    const std::array<T, 13> special = {
        T(-0.0),
        T(0.0),
        std::numeric_limits<T>::quiet_NaN(),
        -std::numeric_limits<T>::quiet_NaN(),
        std::numeric_limits<T>::infinity(),
        -std::numeric_limits<T>::infinity(),
        T(1),
        T(-1),
        std::numeric_limits<T>::min(),
        std::numeric_limits<T>::lowest(),
        std::numeric_limits<T>::max(),
        std::numeric_limits<T>::denorm_min(),
        std::numeric_limits<T>::epsilon()};

    T value;
    if (row < special.size())
        value = special[row];
    else if (row % 50 == 0)
        value = row % 100 ? T(-0.0) : T(0.0);
    else
        value = static_cast<T>(static_cast<Int64>(rowBits(row))) / static_cast<T>(1e6);
    return Field(static_cast<Float64>(value));
}

Field arrayOfFloat64Key(size_t row)
{
    const std::array<Float64, 6> elements
        = {-0.0, 0.0, 1.5, -2.25, std::numeric_limits<Float64>::quiet_NaN(), std::numeric_limits<Float64>::infinity()};
    Array value;
    for (size_t i = 0; i != row % 5; ++i)
        value.push_back(Field(elements[(row + i) % elements.size()]));
    return value;
}

/// No integer of `dynamicKey` and `jsonKey` needs a wider type than the first integer of its variant or path, so the keys
/// do not depend on whether inserting a `Field` widens the variant.
constexpr Int64 OFFSET_TO_64_BITS = Int64(1) << 40;

Field dynamicKey(size_t row)
{
    switch (row % 5)
    {
        case 0: return Field(rowBits(row));
        case 1: return Field(stringKey(row));
        case 2: return floatKey<Float64>(row);
        case 3: return Array{Field(static_cast<Int64>(row) - OFFSET_TO_64_BITS), Field(Int64(-1))};
        default: return Field();
    }
}

/// Every 7th object is empty.
Field jsonKey(size_t row)
{
    Object value;
    if (row % 7 == 6)
        return value;

    value["a"] = Field(1000 + rowBits(row) % 1000);
    if (row % 2)
        value["b"] = Field("s" + std::to_string(row % 50));
    if (row % 3 == 0)
        value["c.d"] = Field(-static_cast<Int64>(row) - OFFSET_TO_64_BITS);
    if (row % 5 == 0)
        value["e"] = Array{Field(static_cast<UInt64>(OFFSET_TO_64_BITS) + row), Field(static_cast<UInt64>(OFFSET_TO_64_BITS) + row + 1)};
    return value;
}

/// A class of `Map` keys: the key type and the key of each row.
struct KeyClass
{
    std::string_view type_name;
    Field (*make_key)(size_t row);
};

/// The writer dispatches on the `TypeIndex` of the key column (`DISPATCH_MAP_COLUMN_TYPE` in SerializationMap.cpp), so
/// the classes from `UInt8` to `FixedString(4)` and also `Date`, `Date32`, `DateTime`, `Enum8` and `Bool` (columns of
/// `UInt16`, `Int32`, `UInt32`, `Int8`, `UInt8`) take its typed branches, and the other classes the `IColumn` one.
std::vector<KeyClass> getKeyClasses()
{
    return {
        {"UInt8", &integerKey<UInt8>},
        {"UInt16", &integerKey<UInt16>},
        {"UInt32", &integerKey<UInt32>},
        {"UInt64", &integerKey<UInt64>},
        {"Int8", &integerKey<Int8>},
        {"Int16", &integerKey<Int16>},
        {"Int32", &integerKey<Int32>},
        {"Int64", &integerKey<Int64>},
        {"Float32", &floatKey<Float32>},
        {"Float64", &floatKey<Float64>},
        {"String", [](size_t row) -> Field { return stringKey(row); }},
        {"FixedString(4)", [](size_t row) -> Field { return rowBytes(row, row % 5); }},
        {"UInt128", &uint128Key},
        {"Int256", &int256Key},
        {"UUID", [](size_t row) -> Field { return UUID(row ? rowBits128(row) : UInt128(0)); }},
        {"IPv4", [](size_t row) -> Field { return IPv4(static_cast<UInt32>(rowBits(row))); }},
        {"IPv6", [](size_t row) -> Field { return IPv6(rowBits128(row)); }},
        {"Date", [](size_t row) -> Field { return static_cast<UInt64>(static_cast<UInt16>(rowBits(row))); }},
        {"Date32", [](size_t row) -> Field { return static_cast<Int64>(rowBits(row) % 145000) - 25567; }},
        {"DateTime", [](size_t row) -> Field { return static_cast<UInt64>(static_cast<UInt32>(rowBits(row))); }},
        {"DateTime64(3)",
         [](size_t row) -> Field
         {
             auto value = static_cast<Int64>(rowBits(row) >> 20);
             return DecimalField<DateTime64>(DateTime64(row % 2 ? -value : value), 3);
         }},
        {"Enum8('a' = -128, 'b' = 0, 'c' = 127)",
         [](size_t row) -> Field
         {
             const std::array<Int64, 3> values = {-128, 0, 127};
             return values[rowBits(row) % values.size()];
         }},
        {"Decimal64(4)",
         [](size_t row) -> Field
         {
             auto value = static_cast<Int64>(rowBits(row) % 1000000000000000000ULL);
             return DecimalField<Decimal64>(Decimal64(row % 2 ? -value : value), 4);
         }},
        {"Bool", [](size_t row) -> Field { return rowBits(row) & 1; }},
        {"LowCardinality(String)", [](size_t row) -> Field { return stringKey(row % 300); }},
        {"Array(UInt8)",
         [](size_t row) -> Field
         {
             Array value;
             for (size_t i = 0; i != row % 6; ++i)
                 value.push_back(Field((rowBits(row) >> (8 * i)) & 0xFF));
             return value;
         }},
        {"Array(Float64)", &arrayOfFloat64Key},
        {"Tuple(String, UInt64)", [](size_t row) -> Field { return Tuple{Field(stringKey(row)), Field(rowBits(row))}; }},
        {"Tuple(Float64, UInt8)", [](size_t row) -> Field { return Tuple{floatKey<Float64>(row), Field(static_cast<UInt64>(row % 256))}; }},
        {"Variant(UInt64, String)",
         [](size_t row) -> Field
         {
             switch (row % 3)
             {
                 case 0: return rowBits(row);
                 case 1: return stringKey(row);
                 default: return Field();
             }
         }},
        {"Dynamic", &dynamicKey},
        /// All types but the first one are in the shared variant.
        {"Dynamic(max_types=1)", &dynamicKey},
        {"JSON", &jsonKey},
        /// A typed path, one dynamic path, and the other paths in the shared data.
        {"JSON(a UInt32, max_dynamic_paths=1)", &jsonKey},
    };
}

/// The pinned values of a key class: checksums of the raw `get64` of the keys and of their buckets for `PINNED_BUCKET_COUNTS`.
struct PinnedKeyClass
{
    std::string_view type_name;
    UInt64 hash_checksum;
    std::array<UInt64, PINNED_BUCKET_COUNTS.size()> bucket_checksums;

    bool operator==(const PinnedKeyClass &) const = default;
};

/// `SipHash` of (row, value) for all rows, in the order of rows.
UInt64 checksumOfRows(const std::vector<UInt64> & values)
{
    SipHash hash;
    for (size_t row = 0; row != values.size(); ++row)
    {
        hash.update(static_cast<UInt64>(row));
        hash.update(values[row]);
    }
    return hash.get64();
}

Streams serializeWithBucketCount(const DataTypePtr & type, const IColumn & column, size_t num_buckets)
{
    std::map<String, std::unique_ptr<WriteBufferFromOwnString>> buffers;

    ISerialization::SerializeBinaryBulkSettings settings;
    settings.write_statistics = ISerialization::SerializeBinaryBulkSettings::StatisticsMode::PREFIX;
    settings.max_buckets_in_map = num_buckets;
    settings.map_buckets_strategy = MergeTreeMapBucketsStrategy::CONSTANT;
    settings.getter = [&](const ISerialization::SubstreamPath & path) -> WriteBuffer *
    {
        auto name = ISerialization::getSubcolumnNameForStream(path);
        auto it = buffers.find(name);
        if (it == buffers.end())
            it = buffers.emplace(name, std::make_unique<WriteBufferFromOwnString>()).first;
        return it->second.get();
    };

    auto serialization = getBucketedSerialization(type);
    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization->serializeBinaryBulkStatePrefix(column, settings, state);
    serialization->serializeBinaryBulkWithMultipleStreams(column, 0, column.size(), settings, state);
    serialization->serializeBinaryBulkStateSuffix(settings, state);

    Streams streams;
    for (auto & [name, buffer] : buffers)
    {
        buffer->finalize();
        streams[name] = buffer->str();
    }
    return streams;
}

/// Writes `keys` as a `Map` column with `num_buckets` buckets and returns the bucket of each key, as the writer recorded it
/// in the `bucket_indexes` stream.
std::vector<UInt64> getBucketsOfWriter(const DataTypePtr & key_type, const ColumnPtr & keys, size_t num_buckets)
{
    auto values = ColumnUInt64::create();
    for (size_t i = 0; i != keys->size(); ++i)
        values->insertValue(i);

    /// Rows of 1 to 7 keys.
    auto offsets = ColumnArray::ColumnOffsets::create();
    for (size_t row = 0, end = 0; end != keys->size(); ++row)
    {
        end = std::min(keys->size(), end + 1 + row % 7);
        offsets->insertValue(end);
    }

    auto type = std::make_shared<DataTypeMap>(key_type, std::make_shared<DataTypeUInt64>());
    auto column = ColumnMap::create(keys, std::move(values), std::move(offsets));
    auto streams = serializeWithBucketCount(type, *column, num_buckets);

    auto index_type = getSmallestIndexesType(num_buckets);
    auto indexes = index_type->createColumn();
    ReadBufferFromString buf(streams.at("bucket_indexes"));
    index_type->getDefaultSerialization()->deserializeBinaryBulk(*indexes, buf, keys->size(), 0);

    std::vector<UInt64> buckets(indexes->size());
    for (size_t i = 0; i != indexes->size(); ++i)
        buckets[i] = indexes->getUInt(i);
    return buckets;
}

}

/// The bucket of a key of every class, both through `getBucketForKey` (the reader of `m[key]` uses it) and through the writer.
TEST(MapBucketedSerialization, BucketsOfEveryKeyClassArePinned)
{
    /// The constants were computed by master's per-row hashing (`updateHashWithValue` of one key into a `SipHash`) and must
    /// not change: the bucket of a key is a part of the format of the data parts already written.
    const std::vector<PinnedKeyClass> expected = {
        {"UInt8", 0x3de27fb620697947, {0x5dfc6849ba7a2ad3, 0xe361eba39075ffbb, 0xd0ed5a3d9a494bc0}},
        {"UInt16", 0xa1eeee36c872ab7d, {0x23bc0b249521465b, 0xa690f63a199d71d6, 0x0227ea3ada7b92e6}},
        {"UInt32", 0x56d4e568677b79bf, {0x0589e02a8474076a, 0x42079758fb749c58, 0x4138278696be8cd4}},
        {"UInt64", 0x0166722e404eac17, {0x40b84d72fd02a143, 0x65b0bb1da438a42d, 0x30571d70468951ae}},
        {"Int8", 0x96f50d3b6ed079f9, {0x97064036313e580d, 0x56d3b3979e713eef, 0x57382905be603580}},
        {"Int16", 0x125d48532043ca2a, {0xd1bbed405c6c734d, 0xe5809febba656661, 0x3c035f9e1f45a199}},
        {"Int32", 0x6ce37884eff5ae31, {0xcc722af6b42747c9, 0x5adce767b33d95bb, 0x34480e4902550f4d}},
        {"Int64", 0xba889d9a59ab5da5, {0x40b84d72fd02a143, 0x20f0da2ec7007698, 0x4652a5df9a915004}},
        {"Float32", 0xe866ae8044ac0d4e, {0x44bcd5e4a748404a, 0x7d886395121ed465, 0x3bfe7340276552a9}},
        {"Float64", 0x75cd9d4dd2751cc0, {0x41f9793843c81f58, 0x04f5f35c05e20ee6, 0x63a288f3d97ee2e3}},
        {"String", 0x4c2fff1e52ac147e, {0xba767fad30287c39, 0xbe622dfd96ae0c69, 0x64f329bf96007e47}},
        {"FixedString(4)", 0xa0c44426b396b0a8, {0x8fcb84379d3ef0db, 0x106dd35d79cc4c88, 0x213ac1aabb14551a}},
        {"UInt128", 0xb40cabf4aba6725f, {0x32b127701fb5fbc4, 0x8f692658adb320a0, 0xb79436dd1911ac2e}},
        {"Int256", 0x005ebbdbe4da0868, {0xe67e1e4d04f81bf4, 0x1907248782d69755, 0x1d7b1938e0d8f35a}},
        {"UUID", 0xfca9d7e16c258f1d, {0x5b9c93bc100f94da, 0x1a540acebb866b7f, 0x4fe1dd77169cc01e}},
        {"IPv4", 0xf0706cd8a2777d0c, {0x74375c62bcc09e0c, 0x6ea66ce712972969, 0xc15ce6e458d20e7e}},
        {"IPv6", 0x0129702e79385da4, {0xf4d6cade3ba9267b, 0x1a540acebb866b7f, 0x663f203fbbda6037}},
        {"Date", 0xce3f6b33344fca3a, {0x0283aa229772fe37, 0xa8cafffa44951a2f, 0x1a6e604d1621b110}},
        {"Date32", 0x88df848ec1e2c157, {0xeabac3e050c950e4, 0xf67582e131c69c6b, 0x99db0a996622b826}},
        {"DateTime", 0xf0706cd8a2777d0c, {0x74375c62bcc09e0c, 0x6ea66ce712972969, 0xc15ce6e458d20e7e}},
        {"DateTime64(3)", 0x98f6cc3c7dba7f27, {0x7ffbff343114e01f, 0x4945049980f31628, 0x4de1078356c93bdc}},
        {"Enum8('a' = -128, 'b' = 0, 'c' = 127)", 0xe0b860ca05b189c1, {0x78958cfd2aeea04b, 0xfc511e971598bfc9, 0xbc42390e5eefd5bd}},
        {"Decimal64(4)", 0xa77403667d789141, {0x53853c3a43f8a4d2, 0xe677758f25d039d5, 0xc754ff97f4d1052b}},
        {"Bool", 0xab8093eda1497dde, {0x8fd4c06b978499b4, 0x6f6c5432486e1c18, 0x12daf396a9c0b17c}},
        {"LowCardinality(String)", 0x9792596775f218d0, {0x0056a469a9557c6a, 0xc7210e9107afb9af, 0x1b0e5efc89469a57}},
        {"Array(UInt8)", 0x79ec1ce8e13eda6e, {0x3ea2eee18ea83a0d, 0xc3dff9ead09cd2a0, 0xb903a5e3521099fc}},
        {"Array(Float64)", 0x3589453f364c75a8, {0xccb1c207ae35ffa6, 0x45966f6052d661d8, 0x46dd23fec99983a3}},
        {"Tuple(String, UInt64)", 0x3dcb75c6c8cd75a8, {0xeb3bf4a8f98f2090, 0x05eadc83f3afa527, 0x2a0b952b77178107}},
        {"Tuple(Float64, UInt8)", 0x97603802ecd0d152, {0xa65263dbb79e235d, 0x2117fceae739f28c, 0xe4308bc5a5984f3f}},
        {"Variant(UInt64, String)", 0xc9a2b7d3e706771b, {0x9ce7f7285d2d200b, 0x8b3cca82ad28dcf5, 0xc78b694d3034cf43}},
        {"Dynamic", 0xe8caef0e1dc7afb7, {0xa8de81bf5eb61051, 0x3b23085d43456ffb, 0xb7b5285110a9488a}},
        {"Dynamic(max_types=1)", 0xe8caef0e1dc7afb7, {0xa8de81bf5eb61051, 0x3b23085d43456ffb, 0xb7b5285110a9488a}},
        {"JSON", 0x54bf20ad6e005aab, {0x0fe6136777ff4acb, 0xd9ab465e72b0f17c, 0x2ced10401898911b}},
        {"JSON(a UInt32, max_dynamic_paths=1)", 0x85d4c13b756e46bf, {0xc02689496a0bfb8d, 0xdf38a0e5231eaa9d, 0x72caa298430d8bac}},
    };

    std::vector<PinnedKeyClass> actual;
    std::vector<std::pair<DataTypePtr, ColumnPtr>> keys_of_classes;
    for (const auto & key_class : getKeyClasses())
    {
        auto key_type = DataTypeFactory::instance().get(String(key_class.type_name));
        auto mutable_keys = key_type->createColumn();
        for (size_t row = 0; row != NUM_KEYS; ++row)
            mutable_keys->insert(key_class.make_key(row));
        ColumnPtr keys = std::move(mutable_keys);

        PinnedKeyClass pinned{key_class.type_name, 0, {}};

        /// `get64() % SIZE_MAX` is the raw `get64()` except for the single value `SIZE_MAX`.
        std::vector<UInt64> hashes(NUM_KEYS);
        for (size_t row = 0; row != NUM_KEYS; ++row)
            hashes[row] = SerializationMap::getBucketForKey(keys, row, std::numeric_limits<size_t>::max());
        pinned.hash_checksum = checksumOfRows(hashes);

        for (size_t i = 0; i != PINNED_BUCKET_COUNTS.size(); ++i)
        {
            std::vector<UInt64> buckets(NUM_KEYS);
            for (size_t row = 0; row != NUM_KEYS; ++row)
                buckets[row] = SerializationMap::getBucketForKey(keys, row, PINNED_BUCKET_COUNTS[i]);
            pinned.bucket_checksums[i] = checksumOfRows(buckets);
        }

        actual.push_back(pinned);
        keys_of_classes.emplace_back(key_type, keys);
    }

    if (actual != expected)
    {
        String computed;
        for (const auto & pinned : actual)
            computed += fmt::format(
                "        {{\"{}\", 0x{:016x}, {{0x{:016x}, 0x{:016x}, 0x{:016x}}}}},\n",
                pinned.type_name,
                pinned.hash_checksum,
                pinned.bucket_checksums[0],
                pinned.bucket_checksums[1],
                pinned.bucket_checksums[2]);
        ADD_FAILURE() << "The computed values, in the form of `expected`:\n" << computed;
    }

    /// The writer hashes the keys of the typed key columns in its own instantiation (`splitMapToBucketsTyped`), it must put
    /// every key into the bucket that `getBucketForKey` gives.
    for (const auto & [key_type, keys] : keys_of_classes)
    {
        SCOPED_TRACE(key_type->getName());
        for (size_t num_buckets : PINNED_BUCKET_COUNTS)
        {
            auto buckets_of_writer = getBucketsOfWriter(key_type, keys, num_buckets);
            EXPECT_EQ(buckets_of_writer.size(), NUM_KEYS);
            for (size_t row = 0; row != std::min(buckets_of_writer.size(), NUM_KEYS); ++row)
            {
                size_t bucket = SerializationMap::getBucketForKey(keys, row, num_buckets);
                if (buckets_of_writer[row] != bucket)
                {
                    ADD_FAILURE() << "With " << num_buckets << " buckets the writer puts key " << row << " into bucket "
                                  << buckets_of_writer[row] << ", getBucketForKey into bucket " << bucket;
                    break;
                }
            }
        }
    }
}
