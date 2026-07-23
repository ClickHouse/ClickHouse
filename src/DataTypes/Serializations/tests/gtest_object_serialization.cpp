#include <Columns/ColumnObject.h>
#include <Core/MergeTreeSerializationEnums.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/Serializations/SerializationObjectHelpers.h>
#include <DataTypes/Serializations/SerializationObjectSharedData.h>
#include <IO/ReadBufferFromString.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <limits>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
}

TEST(ObjectSerialization, FieldBinarySerialization)
{
    auto type = DataTypeFactory::instance().get("JSON(max_dynamic_types=10, max_dynamic_paths=2, a.b UInt32, a.c Array(String))");
    auto serialization = type->getDefaultSerialization();
    Object object1 = Object{{"a.c", Array{"Str1", "Str2"}}, {"a.d", Field(42)}, {"a.e", Tuple{Field(43), "Str3"}}};
    WriteBufferFromOwnString ostr;
    serialization->serializeBinary(object1, ostr, FormatSettings());
    ReadBufferFromString istr(ostr.str());
    Field object2;
    serialization->deserializeBinary(object2, istr, FormatSettings());
    ASSERT_EQ(object1, object2.safeGet<Object>());
}


TEST(ObjectSerialization, ColumnBinarySerialization)
{
    auto type = DataTypeFactory::instance().get("JSON(max_dynamic_types=10, max_dynamic_paths=2, a.b UInt32, a.c Array(String))");
    auto serialization = type->getDefaultSerialization();
    auto col = type->createColumn();
    auto & col_object = assert_cast<ColumnObject &>(*col);
    col_object.insert(Object{{"a.c", Array{"Str1", "Str2"}}, {"a.d", Field(42)}, {"a.e", Tuple{Field(43), "Str3"}}});
    WriteBufferFromOwnString ostr1;
    serialization->serializeBinary(col_object, 0, ostr1, FormatSettings());
    ReadBufferFromString istr1(ostr1.str());
    serialization->deserializeBinary(col_object, istr1, FormatSettings());
    ASSERT_EQ(col_object[0], col_object[1]);
    col_object.insert(Object{{"a.c", Array{"Str1", "Str2"}}, {"a.e", Field(42)}, {"b.d", Field(42)}, {"b.e", Tuple{Field(43), "Str3"}}, {"b.g", Field("Str4")}});
    WriteBufferFromOwnString ostr2;
    serialization->serializeBinary(col_object, 2, ostr2, FormatSettings());
    ReadBufferFromString istr2(ostr2.str());
    serialization->deserializeBinary(col_object, istr2, FormatSettings());
    ASSERT_EQ(col_object[2], col_object[3]);
}

TEST(ObjectSerialization, JSONSerialization)
{
    auto type = DataTypeFactory::instance().get("JSON(max_dynamic_types=10, max_dynamic_paths=2, a.b UInt32, a.c Array(String))");
    auto serialization = type->getDefaultSerialization();
    auto col = type->createColumn();
    auto & col_object = assert_cast<ColumnObject &>(*col);
    col_object.insert(Object{{"a.c", Array{"Str1", "Str2"}}, {"a.d", Field(42)}, {"a.e", Tuple{Field(43), "Str3"}}});
    col_object.insert(Object{{"a.c", Array{"Str1", "Str2"}}, {"a", Tuple{Field(43), "Str3"}}, {"a.b.c", Field(42)}, {"a.b.e", Field(43)}, {"b.c.d.e", Field(42)}, {"b.c.d.g", Field(43)}, {"b.c.h.r", Field(44)}, {"c.g.h.t", Array{Field("Str"), Field("Str2")}}, {"h", Field("Str")}, {"j", Field("Str")}});
    WriteBufferFromOwnString buf1;
    serialization->serializeTextJSON(col_object, 1, buf1, FormatSettings());
    ASSERT_EQ(buf1.str(), R"({"a":[43,"Str3"],"a":{"b":0,"b":{"c":42,"e":43},"c":["Str1","Str2"]},"b":{"c":{"d":{"e":42,"g":43},"h":{"r":44}}},"c":{"g":{"h":{"t":["Str","Str2"]}}},"h":"Str","j":"Str"})");
    WriteBufferFromOwnString buf2;
    serialization->serializeTextJSONPretty(col_object, 1, buf2, FormatSettings(), 0);
    ASSERT_EQ(buf2.str(), R"({
    "a": [
        43,
        "Str3"
    ],
    "a": {
        "b": 0,
        "b": {
            "c": 42,
            "e": 43
        },
        "c": ["Str1","Str2"]
    },
    "b": {
        "c": {
            "d": {
                "e": 42,
                "g": 43
            },
            "h": {
                "r": 44
            }
        }
    },
    "c": {
        "g": {
            "h": {
                "t": ["Str","Str2"]
            }
        }
    },
    "h": "Str",
    "j": "Str"
})");

}

/// flattenAndBucketSharedDataPaths must densify each shared-data path into a column that has one
/// entry per row (the stored value where the path is present, a default where it is absent), for an
/// arbitrary subrange [start, end). This exercises rows with different, overlapping and missing paths,
/// empty rows and trailing gaps -- the exact shape the merge/serialization path produces.
namespace
{
using DensePath = std::vector<std::optional<UInt64>>;

void checkFlattenedSharedData(
    const ColumnObject & col_object,
    size_t start,
    size_t end,
    const std::map<String, DensePath> & expected)
{
    auto buckets = flattenAndBucketSharedDataPaths(*col_object.getSharedDataPtr(), start, end, col_object.getDynamicType(), 1);
    ASSERT_EQ(buckets.size(), 1u);

    std::map<String, ColumnPtr> path_to_column;
    for (const auto & [path, column] : buckets[0])
        path_to_column[path] = column;

    ASSERT_EQ(path_to_column.size(), expected.size());
    for (const auto & [path, values] : expected)
    {
        auto it = path_to_column.find(path);
        ASSERT_NE(it, path_to_column.end()) << "missing path " << path;
        const auto & column = *it->second;
        /// Every path column must be densified to exactly (end - start) rows.
        ASSERT_EQ(column.size(), values.size()) << "wrong size for path " << path;
        for (size_t i = 0; i != values.size(); ++i)
        {
            Field f = column[i];
            if (values[i].has_value())
                ASSERT_EQ(f.safeGet<UInt64>(), *values[i]) << "wrong value for path " << path << " row " << i;
            else
                ASSERT_TRUE(f.isNull()) << "expected default (null) for path " << path << " row " << i;
        }
    }
}
}

TEST(ObjectSerialization, FlattenAndBucketSharedDataPaths)
{
    /// max_dynamic_paths=0 forces every path into shared data.
    auto type = DataTypeFactory::instance().get("JSON(max_dynamic_paths=0)");
    auto col = type->createColumn();
    auto & col_object = assert_cast<ColumnObject &>(*col);

    /// Row 0: a, b ; row 1: {} ; row 2: b, c ; row 3: a ; row 4: {} ; row 5: a, b, c
    col_object.insert(Object{{"a", Field(UInt64(10))}, {"b", Field(UInt64(11))}});
    col_object.insert(Object{});
    col_object.insert(Object{{"b", Field(UInt64(21))}, {"c", Field(UInt64(22))}});
    col_object.insert(Object{{"a", Field(UInt64(30))}});
    col_object.insert(Object{});
    col_object.insert(Object{{"a", Field(UInt64(50))}, {"b", Field(UInt64(51))}, {"c", Field(UInt64(52))}});

    ASSERT_EQ(col_object.size(), 6u);

    /// Full range. std::nullopt = default (path absent in that row).
    checkFlattenedSharedData(col_object, 0, 6, {
        {"a", {10, std::nullopt, std::nullopt, 30, std::nullopt, 50}},
        {"b", {11, std::nullopt, 21, std::nullopt, std::nullopt, 51}},
        {"c", {std::nullopt, std::nullopt, 22, std::nullopt, std::nullopt, 52}},
    });

    /// Subrange [1, 4) = rows {} , {b,c} , {a}. Densification is relative to `start`.
    checkFlattenedSharedData(col_object, 1, 4, {
        {"a", {std::nullopt, std::nullopt, 30}},
        {"b", {std::nullopt, 21, std::nullopt}},
        {"c", {std::nullopt, 22, std::nullopt}},
    });

    /// Subrange [2, 5) = rows {b,c} , {a} , {}. Trailing empty row must be padded with a default.
    checkFlattenedSharedData(col_object, 2, 5, {
        {"a", {std::nullopt, 30, std::nullopt}},
        {"b", {21, std::nullopt, std::nullopt}},
        {"c", {22, std::nullopt, std::nullopt}},
    });
}

/// The shared-data-paths statistics count in the ObjectStructure prefix is only read when
/// `object_and_dynamic_read_statistics` is enabled -- the MergeTree part read path, which the
/// `Native` input format (covered by 04350_json_native_too_many_paths) never reaches, since it
/// always leaves statistics disabled. A corrupted count there must be rejected with a clean
/// `INCORRECT_DATA` error instead of escaping as an uncaught `std::bad_alloc` /
/// `std::length_error` / `std::bad_array_new_length` from the hash-table `reserve`.
TEST(ObjectSerialization, TooManySharedDataPathsStatistics)
{
    auto type = DataTypeFactory::instance().get("JSON");
    auto serialization = type->getDefaultSerialization();

    /// Hand-crafted V1 (version = 0) ObjectStructure prefix:
    ///   [UInt64 LE version = 0]
    ///   [VarUInt max_dynamic_paths = 0]                            (V1 only)
    ///   [VarUInt number of dynamic paths = 0]
    ///   [VarUInt shared-data-paths statistics count = SIZE_MAX]    <- corrupted
    WriteBufferFromOwnString structure;
    writeBinaryLittleEndian(static_cast<UInt64>(0), structure); /// SerializationVersion::V1
    writeVarUInt(static_cast<UInt64>(0), structure);            /// max_dynamic_paths
    writeVarUInt(static_cast<UInt64>(0), structure);            /// number of dynamic paths
    writeVarUInt(std::numeric_limits<size_t>::max(), structure);/// shared-data-paths count
    std::string structure_bytes = structure.str();
    ReadBufferFromString structure_stream(structure_bytes);

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.object_and_dynamic_read_statistics = true;
    settings.getter = [&](const ISerialization::SubstreamPath & path) -> ReadBuffer *
    {
        if (!path.empty() && path.back().type == ISerialization::Substream::ObjectStructure)
            return &structure_stream;
        return nullptr;
    };

    ISerialization::DeserializeBinaryBulkStatePtr state;
    try
    {
        serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);
        FAIL() << "Expected INCORRECT_DATA for a corrupted shared-data-paths statistics count";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::INCORRECT_DATA) << e.message();
        ASSERT_NE(e.message().find("too many paths"), std::string::npos) << e.message();
    }
}

/// A large-but-representable dynamic-paths count (far below `max_size()`, so it passes the
/// container-capacity guard) must not drive a huge up-front allocation: the reader reserves only a
/// capped hint and appends paths one by one, so a corrupted count that the stream cannot back trips
/// a normal read error at end of stream (a `DB::Exception`), not a `std::bad_alloc` / OOM. This is
/// the below-`max_size()` companion to `04350_json_native_too_many_paths`, which covers the
/// `SIZE_MAX` corner (rejected up front as `INCORRECT_DATA` "too many paths").
TEST(ObjectSerialization, LargeButRepresentablePathCountFailsCleanly)
{
    auto type = DataTypeFactory::instance().get("JSON");
    auto serialization = type->getDefaultSerialization();

    /// Hand-crafted V1 (version = 0) ObjectStructure prefix:
    ///   [UInt64 LE version = 0]
    ///   [VarUInt max_dynamic_paths = 0]                (V1 only)
    ///   [VarUInt number of dynamic paths = 100000000]  <- large but representable, no path bytes follow
    WriteBufferFromOwnString structure;
    writeBinaryLittleEndian(static_cast<UInt64>(0), structure); /// SerializationVersion::V1
    writeVarUInt(static_cast<UInt64>(0), structure);            /// max_dynamic_paths
    writeVarUInt(static_cast<UInt64>(100000000), structure);    /// number of dynamic paths
    std::string structure_bytes = structure.str();
    ReadBufferFromString structure_stream(structure_bytes);

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](const ISerialization::SubstreamPath & path) -> ReadBuffer *
    {
        if (!path.empty() && path.back().type == ISerialization::Substream::ObjectStructure)
            return &structure_stream;
        return nullptr;
    };

    ISerialization::DeserializeBinaryBulkStatePtr state;
    try
    {
        serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);
        FAIL() << "Expected a read error for a corrupted dynamic-paths count the stream cannot back";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF) << e.message();
    }
}

/// `shared_data_buckets` in a V3 `Object` prefix is a raw count later used to size per-bucket state
/// vectors and `Columns`. A value outside the legitimate on-wire range `[1, MAX_OBJECT_SHARED_DATA_BUCKETS]`
/// must be rejected up front with a clean `INCORRECT_DATA` error instead of propagating a huge
/// allocation into the bucketed shared-data readers. Legitimate counts come from a small MergeTree
/// setting, so not just the `SIZE_MAX` corner but also a large-but-representable count (e.g. `100000`,
/// far below the container's `max_size()`) and `0` are all corruption.
static void expectInvalidNumberOfBucketsRejected(size_t num_buckets)
{
    auto type = DataTypeFactory::instance().get("JSON");
    auto serialization = type->getDefaultSerialization();

    /// Hand-crafted V3 (version = 4) ObjectStructure prefix:
    ///   [UInt64 LE version = 4]
    ///   [VarUInt number of dynamic paths = 0]
    ///   [VarUInt shared data serialization version = 1]   (MAP_WITH_BUCKETS, so a bucket count follows)
    ///   [VarUInt shared_data_buckets = num_buckets]        <- corrupted
    WriteBufferFromOwnString structure;
    writeBinaryLittleEndian(static_cast<UInt64>(4), structure); /// SerializationVersion::V3
    writeVarUInt(static_cast<UInt64>(0), structure);            /// number of dynamic paths
    writeVarUInt(static_cast<UInt64>(1), structure);            /// shared data serialization version = MAP_WITH_BUCKETS
    writeVarUInt(num_buckets, structure);                       /// shared_data_buckets
    std::string structure_bytes = structure.str();
    ReadBufferFromString structure_stream(structure_bytes);

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](const ISerialization::SubstreamPath & path) -> ReadBuffer *
    {
        if (!path.empty() && path.back().type == ISerialization::Substream::ObjectStructure)
            return &structure_stream;
        return nullptr;
    };

    ISerialization::DeserializeBinaryBulkStatePtr state;
    try
    {
        serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);
        FAIL() << "Expected INCORRECT_DATA for an invalid shared_data_buckets count " << num_buckets;
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::INCORRECT_DATA) << e.message();
        ASSERT_NE(e.message().find("invalid number of shared data buckets"), std::string::npos) << e.message();
    }
}

TEST(ObjectSerialization, InvalidNumberOfSharedDataBuckets)
{
    /// The `SIZE_MAX` corner (above the container's `max_size()`).
    expectInvalidNumberOfBucketsRejected(std::numeric_limits<size_t>::max());
    /// A large-but-representable count: far below `max_size()`, but far above the writer-side cap of
    /// MAX_OBJECT_SHARED_DATA_BUCKETS, so it must not reach the per-bucket sizing.
    expectInvalidNumberOfBucketsRejected(100000);
    /// Just past the maximum.
    expectInvalidNumberOfBucketsRejected(MAX_OBJECT_SHARED_DATA_BUCKETS + 1);
    /// Zero buckets is impossible on the wire (the writer always writes at least one).
    expectInvalidNumberOfBucketsRejected(0);
}

/// The per-granule `num_paths` count in the `ADVANCED` (V3) shared-data structure stream is another
/// raw count read from a possibly-untrusted on-disk part and used to size `all_paths` before any path
/// bytes are read (`SerializationObjectSharedData::deserializeStructureGranulePrefix`). Unlike the
/// outer-prefix counts (covered above and by `04350_json_native_too_many_paths`), this one is reached
/// only when reading a MergeTree part, not through the `Native` input format, so it needs its own
/// coverage. A `SIZE_MAX`-family count must be rejected up front as `INCORRECT_DATA` "too many paths";
/// a large-but-representable count the stream cannot back must fail as a normal read error at end of
/// stream (a `DB::Exception`), not a `std::bad_alloc` / OOM.
static void expectGranulePathCountRejected(size_t num_paths, int expected_error_code)
{
    /// A single-bucket ADVANCED shared-data serialization, driven directly through its public bulk
    /// read API in Compact-part mode (the whole-column path that sets `need_all_paths = true`).
    auto dynamic_type = DataTypeFactory::instance().get("Dynamic");
    auto serialization = SerializationObjectSharedData::create(
        SerializationObjectSharedData::SerializationVersion(SerializationObjectSharedData::SerializationVersion::ADVANCED),
        dynamic_type,
        dynamic_type->getDefaultSerialization(),
        /*buckets=*/1);

    /// Hand-crafted ObjectSharedDataStructurePrefix stream for the single granule:
    ///   [VarUInt num_rows = 1]
    ///   [VarUInt num_paths = num_paths]   <- corrupted; no path bytes follow
    WriteBufferFromOwnString structure_prefix;
    writeVarUInt(static_cast<UInt64>(1), structure_prefix); /// num_rows
    writeVarUInt(num_paths, structure_prefix);              /// num_paths
    std::string structure_prefix_bytes = structure_prefix.str();
    ReadBufferFromString structure_prefix_stream(structure_prefix_bytes);

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.data_part_type = MergeTreeDataPartType::Compact;
    settings.use_specialized_prefixes_and_suffixes_substreams = true;
    settings.getter = [&](const ISerialization::SubstreamPath & path) -> ReadBuffer *
    {
        if (!path.empty() && path.back().type == ISerialization::Substream::ObjectSharedDataStructurePrefix)
            return &structure_prefix_stream;
        return nullptr;
    };

    ISerialization::DeserializeBinaryBulkStatePtr state;
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

    ColumnPtr column = DataTypeObject::getTypeOfSharedData()->createColumn();
    try
    {
        serialization->deserializeBinaryBulkWithMultipleStreams(column, /*rows_offset=*/0, /*limit=*/1, settings, state, nullptr);
        FAIL() << "Expected an exception for a corrupted granule num_paths count " << num_paths;
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), expected_error_code) << e.message();
        if (expected_error_code == ErrorCodes::INCORRECT_DATA)
            ASSERT_NE(e.message().find("too many paths"), std::string::npos) << e.message();
    }
}

TEST(ObjectSerialization, InvalidGranulePathCount)
{
    /// The `SIZE_MAX` corner (above the container's `max_size()`) is rejected up front.
    expectGranulePathCountRejected(std::numeric_limits<size_t>::max(), ErrorCodes::INCORRECT_DATA);
    /// A large-but-representable count the stream cannot back trips a normal read error at end of
    /// stream, not a huge allocation.
    expectGranulePathCountRejected(100000000, ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
}
