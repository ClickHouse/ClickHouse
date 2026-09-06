#include <Columns/ColumnObject.h>
#include <Core/MergeTreeSerializationEnums.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/Serializations/SerializationObjectHelpers.h>
#include <DataTypes/Serializations/PrefixReadCancellationChecker.h>
#include <DataTypes/Serializations/SerializationObjectSharedData.h>
#include <IO/ReadBufferFromString.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/IAST.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#if defined(OS_LINUX) || defined(OS_FREEBSD)
#include <Common/MemoryStatisticsOS.h>
#endif
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <base/defines.h> // ADDRESS_SANITIZER, MEMORY_SANITIZER, THREAD_SANITIZER

#include <gtest/gtest.h>

#include <chrono>
#include <iostream>
#include <limits>
#include <thread>

using namespace DB;

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int QUERY_WAS_CANCELLED;
    extern const int NETWORK_ERROR;
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

/// SharedDataBucketsSplitter::flattenBucket must densify each shared-data path into a column that has one
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
    SharedDataBucketsSplitter splitter(*col_object.getSharedDataPtr(), start, end, 1);
    auto bucket = splitter.flattenBucket(0, col_object.getDynamicType());

    std::map<String, ColumnPtr> path_to_column;
    for (const auto & [path, column] : bucket)
        path_to_column[String(path)] = column;

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

    auto column = DataTypeObject::getTypeOfSharedData()->createColumn();
    try
    {
        serialization->deserializeBinaryBulkWithMultipleStreams(*column, /*limit=*/1, settings, state, nullptr);
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

/// A structure-prefix read consumes its whole path list before producing a single row, and both the
/// path count and each individual path length come from the stream. The pipeline only observes
/// cancellation between `work()` calls, so the read polls the query's cancellation predicate from
/// inside its loops (`PrefixReadCancellationChecker`). The tests below pin that the predicate is
/// actually reached in the two places where the work between two polls would otherwise be unbounded:
/// across the path list, and inside one long path name.
///
/// The property under test is "a cancellation predicate is polled inside this loop", which is not a
/// query result, so it cannot be asserted from SQL: the SQL-observable form is a latency comparison,
/// exactly the timing-dependent shape that makes a flaky stateless test. Hence a unit test.
namespace
{

/// Drives a real `QueryStatus` so the cancellation travels the production channel: `ThreadGroup`'s
/// constructor installs a predicate that calls `QueryStatus::throwIfKilled` on the query context's
/// process-list element, and `CurrentThread::checkIfNotCancelled` invokes it.
struct CancellableQueryFixture
{
    ContextMutablePtr query_context;
    QueryStatusPtr query_status;
    ThreadGroupPtr thread_group;

    CancellableQueryFixture()
    {
        query_context = Context::createCopy(getContext().context);
        ClientInfo client_info;
        client_info.current_query_id = "gtest_prefix_read_cancellation";
        Settings settings;
        query_status = std::make_shared<QueryStatus>(
            query_context,
            "SELECT 1",
            /*normalized_query_hash_*/ 0,
            client_info,
            /*priority_handle_*/ QueryPriorities::Handle{},
            /*query_slot_*/ nullptr,
            /*memory_reservation_*/ nullptr,
            /*thread_group_*/ nullptr,
            IAST::QueryKind::Select,
            settings,
            /*watch_start_nanoseconds*/ 0,
            /*is_internal*/ false);
        query_context->setProcessListElement(query_status);
        thread_group = ThreadGroup::createForQuery(query_context);
        CurrentThread::attachToGroupIfDetached(thread_group);
    }

    ~CancellableQueryFixture()
    {
        CurrentThread::detachFromGroupIfNotDetached();
    }

    void cancel() const { query_status->cancelQuery(CancelReason::CANCELLED_BY_USER); }
};

/// One `ADVANCED` shared-data granule prefix: `[VarUInt num_rows][VarUInt num_paths]` followed by
/// `num_paths` names, each `[VarUInt length][bytes]`. Same on-wire shape
/// `expectGranulePathCountRejected` above builds, with real path bytes appended.
std::string makeGranulePrefixWithPaths(const std::vector<std::string> & paths)
{
    WriteBufferFromOwnString structure_prefix;
    writeVarUInt(static_cast<UInt64>(1), structure_prefix); /// num_rows
    writeVarUInt(paths.size(), structure_prefix);           /// num_paths
    for (const auto & path : paths)
        writeStringBinary(path, structure_prefix);
    return structure_prefix.str();
}

/// Reads such a prefix through the public bulk read API of an isolated one-bucket `ADVANCED`
/// shared-data serialization in Compact-part mode - the whole-column path that sets
/// `need_all_paths = true`.
///
/// This granule-level loop is reached for a part WRITTEN with `advanced`, which is not every part:
/// `MergeTreeIOSettings` picks `object_shared_data_serialization_version_for_zero_level_parts`
/// (default `map_with_buckets`) when `IMergeTreeDataPart::isZeroLevel()` holds and
/// `object_shared_data_serialization_version` (default `advanced`) otherwise, and the reader uses the
/// version stored in the part rather than the current setting. `isZeroLevel` compares block numbers
/// rather than the part level, so a merge of several parts writes `advanced` while a fresh insert or a
/// single-part merge does not. Default bucket counts are 8 (Compact) and 32 (Wide), so one bucket is a
/// simplification of the fixture, not of the code path. The outer `SerializationObject`
/// structure-prefix loops that `PrefixReadObjectStructureObservesCancellation` drives are
/// version-independent and always run.
///
/// `seek_stream_to_current_mark_callback` is supplied so the read stops right after the granule
/// prefix (`SerializationObjectSharedData.cpp` skips the flattened data / marks / substream-metadata
/// streams when it can seek). That keeps the harness scoped to the function under test instead of
/// having to fabricate five more unrelated streams.
///
/// Serves the prefix in small chunks with a short delay per chunk, standing in for the compressed
/// on-disk stream the failure was observed on (a `CachedCompressedReadBuffer` refilling at well under
/// 1 MB/s). Without it the assertions below would depend on an in-memory read being slow enough to
/// cross the checkpoint's throttle, which holds only for a multi-megabyte fixture in a debug build:
/// measured on this tree, 100k short paths (1.1 MB) read in 4 ms and reached no checkpoint, while 300k
/// (3.5 MB) took 10 ms and did. Pacing the stream keeps the fixtures small and the result independent
/// of build type and host speed.
/// It also counts the bytes it has served, which is what makes the interruption POINT observable and
/// not just the fact that an exception arrived: a read that stops at the first checkpoint inside a long
/// name has consumed only a few chunks of that name, whereas one that consumes the name in a single
/// `readStrict` has served all of it before anything can throw.
///
/// `cancel_after_bytes` cancels the query from inside the read, once that many bytes have been served.
/// That is what keeps the assertions about the in-loop checkpoints honest: the checkpoint also polls
/// once at prefix entry, so a fixture that cancels beforehand is interrupted there and would pass with
/// no in-loop checkpoint at all.
///
/// `on_chunk` is called after each chunk has been served, with the number of bytes served so far. It is
/// what lets a cell observe the read's own state WHILE the read is running rather than after it has
/// returned and unwound: it is called from inside `buf.read`, so every buffer and string the read is
/// filling is still alive.
class SlowReadBuffer : public ReadBuffer
{
public:
    SlowReadBuffer(
        std::string data_,
        size_t chunk_size_,
        std::chrono::microseconds delay_,
        std::function<void()> cancel_ = {},
        size_t cancel_after_bytes_ = 0,
        std::function<void(size_t)> on_chunk_ = {})
        : ReadBuffer(nullptr, 0)
        , data(std::move(data_))
        , chunk_size(chunk_size_)
        , delay(delay_)
        , cancel(std::move(cancel_))
        , cancel_after_bytes(cancel_after_bytes_)
        , on_chunk(std::move(on_chunk_))
    {
    }

    size_t servedBytes() const { return offset; }

private:
    bool nextImpl() override
    {
        if (offset >= data.size())
            return false;

        /// The first chunk is served immediately, so a read that never needs a second chunk is not
        /// slowed down at all.
        if (offset != 0)
            std::this_thread::sleep_for(delay);

        const size_t size = std::min(chunk_size, data.size() - offset);
        working_buffer = Buffer(data.data() + offset, data.data() + offset + size);
        offset += size;

        if (cancel && !cancelled && offset >= cancel_after_bytes)
        {
            cancelled = true;
            cancel();
        }

        if (on_chunk)
            on_chunk(offset);

        return true;
    }

    std::string data;
    size_t offset = 0;
    size_t chunk_size;
    std::chrono::microseconds delay;
    std::function<void()> cancel;
    size_t cancel_after_bytes = 0;
    bool cancelled = false;
    std::function<void(size_t)> on_chunk;
};

/// Returns how many bytes of the prefix the stream had to serve; also writes that count to
/// `served_bytes` on the way out so a caller can read it after an exception.
size_t readGranulePrefix(
    const std::string & structure_prefix_bytes,
    size_t & served_bytes,
    std::function<void()> cancel = {},
    size_t cancel_after_bytes = 0,
    std::function<void(size_t)> on_chunk = {})
{
    auto dynamic_type = DataTypeFactory::instance().get("Dynamic");
    auto serialization = SerializationObjectSharedData::create(
        SerializationObjectSharedData::SerializationVersion(SerializationObjectSharedData::SerializationVersion::ADVANCED),
        dynamic_type,
        dynamic_type->getDefaultSerialization(),
        /*buckets=*/1);

    /// 4 KiB chunks at 2 ms each, so ~8 chunks (32 KiB of prefix) already exceed the checkpoint's 10 ms
    /// throttle while the whole test still runs in well under a second.
    SlowReadBuffer structure_prefix_stream(
        structure_prefix_bytes, 4096, std::chrono::milliseconds(2), std::move(cancel), cancel_after_bytes, std::move(on_chunk));

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.data_part_type = MergeTreeDataPartType::Compact;
    settings.use_specialized_prefixes_and_suffixes_substreams = true;
    /// The trailing copy-sizes / indexes / values streams are read after the prefix; an empty buffer
    /// for each is enough to reach the end of the read cleanly (the granule holds no rows of data).
    static const std::string empty;
    ReadBufferFromString empty_stream(empty);
    settings.getter = [&](const ISerialization::SubstreamPath & path) -> ReadBuffer *
    {
        if (path.empty())
            return nullptr;
        if (path.back().type == ISerialization::Substream::ObjectSharedDataStructurePrefix)
            return &structure_prefix_stream;
        return &empty_stream;
    };
    settings.seek_stream_to_current_mark_callback = [](const ISerialization::SubstreamPath &) {};

    ISerialization::DeserializeBinaryBulkStatePtr state;
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

    ColumnPtr column = DataTypeObject::getTypeOfSharedData()->createColumn();
    try
    {
        serialization->deserializeBinaryBulkWithMultipleStreams(column, /*rows_offset=*/0, /*limit=*/1, settings, state, nullptr);
    }
    catch (...)
    {
        served_bytes = structure_prefix_stream.servedBytes();
        throw;
    }
    return structure_prefix_stream.servedBytes();
}

/// Asserts that the read was interrupted by the query's cancellation, and that it stopped before
/// consuming more than `max_served_bytes` of the prefix - the read must not merely throw eventually,
/// it must throw from inside the loop it is supposed to be interruptible in.
///
/// The cancellation is raised from inside the read, after `cancel_after_bytes`, so the checkpoint's
/// entry poll cannot satisfy the assertion on its own.
void expectCancelledBefore(
    const std::string & structure_prefix_bytes,
    size_t max_served_bytes,
    std::function<void()> cancel,
    size_t cancel_after_bytes)
{
    size_t served_bytes = 0;
    try
    {
        const size_t served = readGranulePrefix(structure_prefix_bytes, served_bytes, std::move(cancel), cancel_after_bytes);
        FAIL() << "a cancelled query read the whole prefix to completion (" << served << " bytes)";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::QUERY_WAS_CANCELLED) << e.message();
        ASSERT_GE(served_bytes, cancel_after_bytes)
            << "the cancellation was raised before the read reached it, so nothing in the loop was tested";
        ASSERT_LT(served_bytes, max_served_bytes)
            << "a cancelled query consumed " << served_bytes << " of " << structure_prefix_bytes.size()
            << " prefix bytes, i.e. it reached no interruption point inside the loop";
    }
}

std::vector<std::string> makeShortPaths(size_t count)
{
    std::vector<std::string> paths;
    paths.reserve(count);
    for (size_t i = 0; i != count; ++i)
        paths.push_back("path_" + std::to_string(i));
    return paths;
}

/// One V2 `ObjectStructure` prefix - the outer structure prefix of a JSON column, which is what the
/// changelog entry names and which is read for every part regardless of the shared-data serialization
/// version: `[UInt64 LE version][VarUInt num dynamic paths]` followed by that many
/// `[VarUInt length][bytes]` names. V2 rather than V1, so no `max_dynamic_paths` field precedes the
/// list, and rather than V3, so no shared-data version follows it.
std::string makeObjectStructurePrefixWithPaths(const std::vector<std::string> & paths)
{
    WriteBufferFromOwnString structure;
    writeBinaryLittleEndian(static_cast<UInt64>(SerializationObject::SerializationVersion::V2), structure);
    writeVarUInt(paths.size(), structure);
    for (const auto & path : paths)
        writeStringBinary(path, structure);
    return structure.str();
}

/// Drives `SerializationObject::deserializeObjectStructureStatePrefix` over such a prefix, on the same
/// paced stream the granule-prefix harness uses. Only the `ObjectStructure` substream is served: the
/// structure prefix is read first, so a cancellation inside it stops the read before any other stream
/// is asked for.
size_t readObjectStructurePrefix(
    const std::string & structure_bytes,
    size_t & served_bytes,
    std::function<void()> cancel = {},
    size_t cancel_after_bytes = 0)
{
    auto type = DataTypeFactory::instance().get("JSON");
    auto serialization = type->getDefaultSerialization();

    SlowReadBuffer structure_stream(
        structure_bytes, 4096, std::chrono::milliseconds(2), std::move(cancel), cancel_after_bytes);

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
    }
    catch (...)
    {
        served_bytes = structure_stream.servedBytes();
        throw;
    }
    served_bytes = structure_stream.servedBytes();
    return structure_stream.servedBytes();
}

/// Same assertion as `expectCancelledBefore`, for the outer `Object` structure prefix.
void expectObjectStructureCancelledBefore(
    const std::string & structure_bytes,
    size_t max_served_bytes,
    std::function<void()> cancel,
    size_t cancel_after_bytes)
{
    size_t served_bytes = 0;
    try
    {
        const size_t served = readObjectStructurePrefix(structure_bytes, served_bytes, std::move(cancel), cancel_after_bytes);
        FAIL() << "a cancelled query read the whole Object structure prefix to completion (" << served << " bytes)";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::QUERY_WAS_CANCELLED) << e.message();
        ASSERT_GE(served_bytes, cancel_after_bytes)
            << "the cancellation was raised before the read reached it, so nothing in the loop was tested";
        ASSERT_LT(served_bytes, max_served_bytes)
            << "a cancelled query consumed " << served_bytes << " of " << structure_bytes.size()
            << " prefix bytes, i.e. it reached no interruption point inside the loop";
    }
}

/// One V2 `DynamicStructure` prefix whose shared-variant STATISTICS list carries the names. That list is
/// the one with no count bound at all (`num_dynamic_types` is capped by `MAX_DYNAMIC_TYPES_LIMIT`, the
/// statistics count by nothing), and it is read on the same MergeTree path: reading a JSON column
/// deserializes a `Dynamic` structure prefix per dynamic path.
///   `[UInt64 LE version][VarUInt num_dynamic_types = 0]`
///   `[VarUInt count]`                      one per regular variant; with `num_dynamic_types = 0` the
///                                          variant list still holds the shared variant, so exactly one
///   `[VarUInt statistics_size]`
/// then that many `[VarUInt length][bytes][VarUInt count]` pairs.
std::string makeDynamicStructurePrefixWithStatistics(const std::vector<std::string> & names)
{
    WriteBufferFromOwnString structure;
    writeBinaryLittleEndian(static_cast<UInt64>(SerializationDynamic::SerializationVersion::V2), structure);
    writeVarUInt(static_cast<UInt64>(0), structure); /// num_dynamic_types
    writeVarUInt(static_cast<UInt64>(0), structure); /// statistics for the shared variant
    writeVarUInt(names.size(), structure);
    for (const auto & name : names)
    {
        writeStringBinary(name, structure);
        writeVarUInt(static_cast<UInt64>(1), structure);
    }
    return structure.str();
}

/// One binary-encoded `Tuple(String, ..., String)` description with `num_elements` elements:
/// `[0x1F UnnamedTuple][VarUInt num_elements]` then that many `[0x15 String]` bytes. Unnamed rather
/// than named, because `DataTypeTuple`'s named constructor rejects empty and duplicate names, so a
/// named tuple would need distinct name bytes per element.
std::string makeLongBinaryTypeDescription(size_t num_elements)
{
    WriteBufferFromOwnString description;
    writeBinaryLittleEndian(static_cast<UInt8>(BinaryTypeIndex::UnnamedTuple), description);
    writeVarUInt(num_elements, description);
    for (size_t i = 0; i != num_elements; ++i)
        writeBinaryLittleEndian(static_cast<UInt8>(BinaryTypeIndex::String), description);
    return description.str();
}

/// One V3 `DynamicStructure` prefix whose variant list is BINARY-ENCODED rather than a list of type
/// names. This is the branch every default part takes: `dynamic_serialization_version` DECLAREs `v3`
/// (`MergeTreeSettings.cpp`) and `merge_tree_use_v1_object_and_dynamic_serialization` defaults false,
/// so `MergeTreeIOSettings` selects V3 for every new `Dynamic`/JSON part, and the read takes the
/// `decodeDataType` branch whenever the stored version is V3, regardless of `native_format`.
///   `[UInt64 LE version = V3][VarUInt num_dynamic_types]`
/// then one binary type description per type, then `[bool has_statistics = false]` (V3 only).
///
/// Statistics are switched off so the fixture stops right after the variant list, which is the loop
/// under test.
std::string makeDynamicStructurePrefixV3(const std::vector<std::string> & type_descriptions)
{
    WriteBufferFromOwnString structure;
    writeBinaryLittleEndian(static_cast<UInt64>(SerializationDynamic::SerializationVersion::V3), structure);
    writeVarUInt(type_descriptions.size(), structure); /// num_dynamic_types
    for (const auto & description : type_descriptions)
        structure.write(description.data(), description.size());
    writeBinary(false, structure); /// has_statistics
    return structure.str();
}

/// Drives `SerializationDynamic::deserializeDynamicStructureStatePrefix` over such a prefix.
/// `object_and_dynamic_read_statistics` is what both MergeTree readers set, and it is what makes the
/// statistics list be read at all.
///
/// `chunk_size` / `delay` are the stream's pacing. The default 4 KiB / 2 ms is what makes a read span
/// the checkpoint's period; one cell below deliberately serves the prefix unpaced instead, so that the
/// checkpoint AFTER the variant list is the first one whose throttle has expired.
size_t readDynamicStructurePrefix(
    const std::string & structure_bytes,
    size_t & served_bytes,
    std::function<void()> cancel = {},
    size_t cancel_after_bytes = 0,
    size_t chunk_size = 4096,
    std::chrono::microseconds delay = std::chrono::milliseconds(2))
{
    auto type = DataTypeFactory::instance().get("Dynamic");
    auto serialization = type->getDefaultSerialization();

    SlowReadBuffer structure_stream(
        structure_bytes, chunk_size, delay, std::move(cancel), cancel_after_bytes);

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.object_and_dynamic_read_statistics = true;
    settings.getter = [&](const ISerialization::SubstreamPath & path) -> ReadBuffer *
    {
        if (!path.empty() && path.back().type == ISerialization::Substream::DynamicStructure)
            return &structure_stream;
        return nullptr;
    };

    ISerialization::DeserializeBinaryBulkStatePtr state;
    try
    {
        serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);
    }
    catch (...)
    {
        served_bytes = structure_stream.servedBytes();
        throw;
    }
    served_bytes = structure_stream.servedBytes();
    return structure_stream.servedBytes();
}

/// Same assertion as `expectCancelledBefore`, for the `Dynamic` structure prefix.
void expectDynamicStructureCancelledBefore(
    const std::string & structure_bytes,
    size_t max_served_bytes,
    std::function<void()> cancel,
    size_t cancel_after_bytes)
{
    size_t served_bytes = 0;
    try
    {
        const size_t served = readDynamicStructurePrefix(structure_bytes, served_bytes, std::move(cancel), cancel_after_bytes);
        FAIL() << "a cancelled query read the whole Dynamic structure prefix to completion (" << served << " bytes)";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::QUERY_WAS_CANCELLED) << e.message();
        ASSERT_GE(served_bytes, cancel_after_bytes)
            << "the cancellation was raised before the read reached it, so nothing in the loop was tested";
        ASSERT_LT(served_bytes, max_served_bytes)
            << "a cancelled query consumed " << served_bytes << " of " << structure_bytes.size()
            << " prefix bytes, i.e. it reached no interruption point inside the loop";
    }
}

/// Runs `body` on a dedicated thread so `current_thread` starts null, independent of whatever
/// `ThreadStatus` / thread group other tests in `unit_tests_dbms` left behind (the reason
/// gtest_thread_group_switcher.cpp uses a dedicated thread too).
void runInFreshThread(std::function<void()> body)
{
    std::thread thread(std::move(body));
    thread.join();
}

}

/// Many path names: without a checkpoint in the loop the whole list is read uninterruptibly, so the
/// read must stop early instead of consuming the list. Asserting WHERE it stopped is what establishes
/// that, which "an exception arrived" on its own does not.
///
/// The second cell uses empty names, and it is the one that pins the loop's own checkpoint: for a
/// zero-length name `readPathNameCancellable`'s chunk loop runs zero iterations, so its per-chunk
/// checkpoint never fires and only the checkpoint at the end of the loop body can interrupt the read.
/// (For a non-empty name the two are redundant, which is why the first cell alone cannot pin either.)
/// Empty path names are a real on-wire shape: a JSON key of "" produces one.
TEST(ObjectSerialization, PrefixReadObservesCancellation)
{
    runInFreshThread([]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        /// ~79 KB of prefix, i.e. 20 chunks of the paced stream and roughly 38 ms of reading against the
        /// checkpoint's 10 ms throttle - crossed with a wide margin while the fixture stays small.
        const std::string short_names = makeGranulePrefixWithPaths(makeShortPaths(8000));

        /// Cancelled from inside the read, one chunk past the granule header, so the interruption has to
        /// come from a checkpoint in the loop: the header is read before the checker is constructed, so
        /// its entry poll runs while the query is still live.
        expectCancelledBefore(short_names, short_names.size() / 2, [&] { query.cancel(); }, 2 * 4096);
    });

    /// A fresh query for the second cell. The cancellation has to be raised by THIS read; a query left
    /// cancelled by the previous cell would be observed at prefix entry, and then the assertion would
    /// say nothing about the loop.
    runInFreshThread([]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        /// ~100 KB of prefix in the same shape, all of it name-count and length headers.
        const std::string empty_names = makeGranulePrefixWithPaths(std::vector<std::string>(100000, ""));

        expectCancelledBefore(empty_names, empty_names.size() / 2, [&] { query.cancel(); }, 2 * 4096);
    });
}

/// One path name spanning many read chunks. `readStringBinary` consumes such a name in a single
/// `readStrict`, so a per-name checkpoint alone leaves the delay bounded by the largest name rather
/// than by a constant - and, because a name read is followed immediately by the loop's checkpoint,
/// a throw-or-not assertion cannot tell the two apart. Asserting how much of the name was served can:
/// the chunked read stops a few chunks in, a single `readStrict` serves all of it first.
TEST(ObjectSerialization, LongPathNameObservesCancellation)
{
    runInFreshThread([]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        /// One 1 MiB name, and only one name, so the only place a checkpoint can be reached before the
        /// whole name has been consumed is inside the name read itself.
        const size_t name_size = 1024 * 1024;
        const std::string bytes = makeGranulePrefixWithPaths({std::string(name_size, 'x')});

        /// Cancelled one chunk past the granule header, i.e. already inside the name, so the entry poll
        /// cannot satisfy the bound. The read consumes the name in 64 KiB steps and only checks between
        /// them, so the interruption lands on a 64 KiB boundary; a quarter of the name is far inside
        /// that granularity while still proving the name was not consumed whole.
        expectCancelledBefore(bytes, name_size / 4, [&] { query.cancel(); }, 2 * 4096);
    });
}

/// A path name whose DECLARED length is far larger than the bytes the stream actually holds. What this
/// pins is that the name's ALLOCATION is chunked as well as its read: `std::string::resize`
/// value-initializes, so resizing once to the declared length writes all of it before the loop's first
/// checkpoint can run, and the declared length is a `VarUInt` the stream chose (capped only at
/// `DEFAULT_MAX_STRING_SIZE` = 1 GiB). Measured on this tree, that single `resize` costs 26 ms at
/// 64 MiB and 400 ms at 1 GiB, against the checkpoint's 10 ms period.
///
/// The observable is PEAK RESIDENT MEMORY, not `served_bytes` and not elapsed time:
///   - `served_bytes` cannot see this at all. The allocation consumes no stream bytes and no checkpoint
///     sits between it and the first `buf.read`, so both behaviours reach the first checkpoint at the
///     same stream position; measured across four fixture shapes, both serve exactly 65536 bytes.
///   - elapsed time does separate them (59.7 ms vs 31.0 ms measured), but it is load-sensitive on a
///     shared host, so asserting on it would make this cell flaky.
///   - `MemoryTracker` cannot see it either: it counts REQUESTED bytes, and `reserve` requests exactly
///     as much as `resize` does. Only the touched-page count differs.
/// Chunked growth touches one chunk at a time, so the peak stays at the chunk size; a single `resize`
/// touches the whole declared length. Measured 192 KiB vs 64 MiB of growth, a margin wide enough that
/// the bound below cannot be reached by allocator or test-harness noise.
///
/// The samples are taken from INSIDE the read, through the stream's per-chunk callback, so the string
/// under test is alive and already grown when its pages are counted. Sampling after the read returned
/// would be unsound: the string is a local of `deserializeStructureGranulePrefix`, so the
/// `CANNOT_READ_ALL_DATA` throw destroys it on the way out, and whether the freed pages are still
/// resident at that point is an allocator-decay question rather than a property of this code.
/// `MemoryStatisticsOS::get` reads current `/proc/self/statm` and has no peak field, hence the running
/// `max` across samples. The sampled series is printed because it is the whole evidence this cell
/// rests on: the assertion message alone is visible only when the cell fails.
///
/// No cancellation is involved: the read ends in `CANNOT_READ_ALL_DATA` either way, which keeps the
/// cell about the allocation alone.
TEST(ObjectSerialization, LargeDeclaredPathNameIsNotAllocatedWhole)
{
#if !defined(OS_LINUX) && !defined(OS_FREEBSD)
    /// The oracle is the resident set, which only `MemoryStatisticsOS` reports, and that class exists
    /// only where /proc/self/statm does.
    GTEST_SKIP() << "the resident-growth oracle needs MemoryStatisticsOS, which is Linux/FreeBSD-only";
#elif defined(ADDRESS_SANITIZER) || defined(MEMORY_SANITIZER) || defined(THREAD_SANITIZER)
    /// The oracle counts touched pages, and a sanitizer touches shadow and origin pages in proportion to
    /// the DECLARED length whether or not the growth is chunked: the allocator poisons the whole
    /// `reserve`d block (1:1 shadow under MSan, 1:8 under ASan), so 64 MiB of capacity costs 64 MiB or
    /// 8 MiB of shadow before this code runs at all. An absolute byte bound is therefore meaningful only
    /// on a build with no shadow memory.
    GTEST_SKIP() << "an absolute resident-growth bound cannot hold on a build with shadow memory";
#else
    runInFreshThread([]
    {
        ThreadStatus thread_status;

        /// 64 MiB is the smallest declared length whose single `resize` already exceeds the 10 ms period
        /// by more than 2x. A 1 GiB declaration would bound it far more loudly, but allocating a
        /// gigabyte in a unit test is not worth the resource.
        static constexpr size_t declared_name_size = 64 * 1024 * 1024;

        /// Exactly three 64 KiB chunk-loop iterations' worth of the declared name, so the read grows the
        /// string three times and then fails on the fourth, and dozens of stream chunks are served - and
        /// hence dozens of samples taken - while the string is alive and growing.
        static constexpr size_t supplied_name_size = 3 * 64 * 1024;

        /// A granule prefix that DECLARES the full length and then supplies only that much of it, so the
        /// fixture stays small. `makeGranulePrefixWithPaths` cannot express this - it writes a name whose
        /// declared length matches its bytes - so the header is written by hand.
        WriteBufferFromOwnString truncated_name_prefix;
        writeVarUInt(static_cast<UInt64>(1), truncated_name_prefix);   /// num_rows
        writeVarUInt(static_cast<UInt64>(1), truncated_name_prefix);   /// num_paths
        writeVarUInt(declared_name_size, truncated_name_prefix);       /// the declared name length
        truncated_name_prefix.write(std::string(supplied_name_size, 'x').data(), supplied_name_size);

        const std::string bytes = truncated_name_prefix.str();

        MemoryStatisticsOS memory_statistics;
        const size_t resident_before = memory_statistics.get().resident;
        size_t peak_resident = resident_before;
        size_t peak_offset = 0;

        /// How many samples were taken past the granule header, i.e. from inside the name read. The
        /// header is a handful of `VarUInt`s, so it cannot span two 4 KiB stream chunks: every sample
        /// beyond the second chunk is necessarily one where `path` exists and is being grown. This count
        /// is fixed by the fixture geometry (49 stream chunks) and does not depend on the allocator,
        /// which is what makes it usable as an assertion about the sampling itself.
        size_t samples_inside_name_read = 0;

        /// The read must fail: the stream ends long before the declared length.
        size_t served_bytes = 0;
        EXPECT_THROW(
            readGranulePrefix(
                bytes,
                served_bytes,
                /*cancel=*/{},
                /*cancel_after_bytes=*/0,
                [&](size_t offset)
                {
                    if (offset > 2 * 4096)
                        ++samples_inside_name_read;

                    const size_t resident = memory_statistics.get().resident;
                    /// Strictly greater, so the offset recorded is where the peak was FIRST reached.
                    if (resident > peak_resident)
                    {
                        peak_resident = resident;
                        peak_offset = offset;
                    }
                }),
            Exception);

        const size_t resident_growth = peak_resident - resident_before;

        std::cout << "LargeDeclaredPathNameIsNotAllocatedWhole: declared " << declared_name_size
                  << " bytes, supplied " << supplied_name_size << "; peak resident growth "
                  << resident_growth << " bytes, first reached at stream offset " << peak_offset << " of "
                  << bytes.size() << "; " << samples_inside_name_read << " samples taken inside the name read"
                  << std::endl;

        /// The samples have to come from inside the name read for the growth above to mean anything. This
        /// is not a property of the allocator but of where the callback fires, so it is checked exactly.
        ASSERT_GE(samples_inside_name_read, 3u)
            << "only " << samples_inside_name_read << " samples were taken while the name was being read, "
            << "so the measured growth says nothing about the allocation under test";

        /// A generous multiple of one 64 KiB chunk, and still ~1/8 of the declared length: reached only
        /// by a build that allocates the whole declared name up front.
        static constexpr size_t max_resident_growth = 8 * 1024 * 1024;
        ASSERT_LT(resident_growth, max_resident_growth)
            << "reading a path name that declares " << declared_name_size << " bytes grew the resident "
            << "set by " << resident_growth << " bytes, i.e. the declared length was allocated in one "
            << "step instead of being grown in chunks";
    });
#endif
}

/// A path name of exactly one `read_chunk_size` (64 KiB), which is the case that pins WHEN the
/// checkpoint's throttle is armed. Such a name is consumed in a single step of
/// `readPathNameCancellable`'s chunk loop, so the read reaches only the FIRST `check()` calls on the
/// thread - the one after that step and the loop-body one right behind it. The throttle is
/// thread-local state, so if it were armed lazily BY the first `check()` that call would read ~0
/// elapsed and skip its own poll, and this read would run to the end of the prefix however long it
/// took; arming it in the checker's constructor is what makes the first poll happen.
///
/// TWO names, because with one the read consumes the whole prefix either way and only the throw
/// differs, so `served_bytes` could not tell the two behaviours apart. With two, name 1 takes ~32 ms
/// against the 10 ms period: interrupted after name 1 the read serves ~68 KiB, uninterrupted it serves
/// all ~128 KiB.
TEST(ObjectSerialization, SingleChunkPathNameObservesCancellationOnTheFirstPrefix)
{
    runInFreshThread([]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        const size_t name_size = 64 * 1024;
        const std::string bytes
            = makeGranulePrefixWithPaths({std::string(name_size, 'x'), std::string(name_size, 'x')});

        /// Cancelled two chunks in, i.e. inside name 1, so neither the entry poll nor the cancellation's
        /// own timing can satisfy the bound. Three quarters of the prefix sits between "stopped after
        /// name 1" (~68 KiB) and "consumed both names" (~128 KiB).
        expectCancelledBefore(bytes, bytes.size() * 3 / 4, [&] { query.cancel(); }, 2 * 4096);
    });
}

/// The outer `Object` structure prefix - the loop the changelog entry names, and the one that runs for
/// every part whatever its shared-data serialization version is. The granule-prefix tests above cover a
/// different function, so without this cell the outer loops are pinned by nothing.
TEST(ObjectSerialization, PrefixReadObjectStructureObservesCancellation)
{
    runInFreshThread([]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        /// ~79 KB of dynamic-path names, i.e. about 20 chunks of the paced stream against the
        /// checkpoint's 10 ms throttle.
        const std::string short_names = makeObjectStructurePrefixWithPaths(makeShortPaths(8000));

        /// Cancelled from inside the read, as in the granule-prefix tests.
        expectObjectStructureCancelledBefore(short_names, short_names.size() / 2, [&] { query.cancel(); }, 2 * 4096);
    });

    /// A fresh query, for the same reason as the granule-prefix test.
    runInFreshThread([]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        /// One 1 MiB name, so the only checkpoint that can be reached before the whole name has been
        /// consumed is the one inside the name read.
        const size_t name_size = 1024 * 1024;
        const std::string one_long_name = makeObjectStructurePrefixWithPaths({std::string(name_size, 'x')});

        expectObjectStructureCancelledBefore(one_long_name, name_size / 4, [&] { query.cancel(); }, 2 * 4096);
    });

    /// Empty names, which is what isolates the loop's OWN checkpoint here: `readPathNameCancellable`'s
    /// chunk loop runs zero iterations for a zero-length name, so its per-chunk checkpoint never fires
    /// and only the checkpoint at the end of the loop body can interrupt the read. For a non-empty name
    /// the two are redundant, so the cell above cannot pin either on its own.
    runInFreshThread([]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        const std::string empty_names = makeObjectStructurePrefixWithPaths(std::vector<std::string>(100000, ""));

        expectObjectStructureCancelledBefore(empty_names, empty_names.size() / 2, [&] { query.cancel(); }, 2 * 4096);
    });
}

/// The `Dynamic` structure prefix. Reading a JSON column deserializes one of these per dynamic path, and
/// its shared-variant statistics list has no count bound at all, so it is the same unbounded
/// uninterruptible read as the `Object` path lists.
///
/// Empty names, for the reason the `Object` cell above uses them: only then does the loop's own
/// checkpoint have to be the one that fires.
TEST(ObjectSerialization, PrefixReadDynamicStructureObservesCancellation)
{
    runInFreshThread([]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        const std::string empty_names = makeDynamicStructurePrefixWithStatistics(std::vector<std::string>(100000, ""));

        expectDynamicStructureCancelledBefore(empty_names, empty_names.size() / 2, [&] { query.cancel(); }, 2 * 4096);
    });

    runInFreshThread([]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        /// One 1 MiB name, isolating the checkpoint inside the name read.
        const size_t name_size = 1024 * 1024;
        const std::string one_long_name = makeDynamicStructurePrefixWithStatistics({std::string(name_size, 'x')});

        expectDynamicStructureCancelledBefore(one_long_name, name_size / 4, [&] { query.cancel(); }, 2 * 4096);
    });
}

/// The V3 variant list, which is the branch every DEFAULT part takes (see
/// `makeDynamicStructurePrefixV3`) and which reads each type as a binary-encoded DESCRIPTION rather
/// than as one length-prefixed name. The checkpoint sits BETWEEN descriptions, so these cells
/// DOCUMENT today's granularity rather than asserting full interruptibility.
///
/// They pin a known gap on purpose: `decodeDataTypeImpl` (`DataTypesBinaryEncoding.cpp`) polls no
/// cancellation predicate, its element loops are bounded only by `MAX_ARRAY_SIZE`, and each element
/// name is read with a plain `readStringBinary`. Giving that decoder a cancellation hook, or bounding
/// its name reads, is tracked separately; when that lands these assertions must be TIGHTENED rather
/// than deleted - the read would then stop inside a description instead of after one.
///
/// Fixture arithmetic, because both cells depend on it. Each description is
/// `1 + 3 + elements_per_type` bytes and the stream is served in 4 KiB chunks, so one description takes
/// ~32 ms of the paced stream against the checkpoint's 10 ms period. The throttle is armed by the
/// checker's constructor, so the loop's FIRST `check()` polls; the V3 loop calls `check()` once per
/// description, so the read stops after the FIRST description - never inside one.
TEST(ObjectSerialization, PrefixReadDynamicStructureV3DocumentsDecoderGranularity)
{
    /// ~65 KiB per description, so one description spans ~16 chunks of the paced stream and the
    /// cancellation below (2 chunks in) lands well inside the FIRST one.
    static constexpr size_t elements_per_type = 64 * 1024;

    /// Interrupted BETWEEN descriptions rather than inside one: cancelled inside description 1, the
    /// read stops at a loop `check()` having consumed whole descriptions, and well before the end.
    runInFreshThread([]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        const std::string description = makeLongBinaryTypeDescription(elements_per_type);
        const std::string four_types
            = makeDynamicStructurePrefixV3({description, description, description, description});

        size_t served_bytes = 0;
        try
        {
            const size_t served = readDynamicStructurePrefix(
                four_types, served_bytes, [&] { query.cancel(); }, 2 * 4096);
            FAIL() << "a cancelled query read the whole V3 variant list to completion (" << served << " bytes)";
        }
        catch (const Exception & e)
        {
            ASSERT_EQ(e.code(), ErrorCodes::QUERY_WAS_CANCELLED) << e.message();
            ASSERT_LT(served_bytes, four_types.size())
                << "a cancelled query consumed the whole variant list, i.e. it reached no interruption "
                << "point between descriptions";
            /// At least one whole description: the interruption came from the loop's `check()`, not
            /// from inside `decodeDataType`, which polls nothing. This is the honest upper bound on
            /// today's granularity, and it is what must be tightened when the decoder gains a hook.
            ASSERT_GE(served_bytes, description.size())
                << "the read stopped inside a description, which `decodeDataType` cannot do - if the "
                << "decoder now polls cancellation, tighten this bound instead of deleting the cell";
        }
    });

    /// The single-description form. With ONE description the whole prefix is that description plus a
    /// 10-byte trailer, so `served_bytes` reaches the same value whether the read stops at the loop's
    /// `check()` or at the very end - the recorded mutant number is the same 65550. So this cell cannot
    /// separate those two positions; what it pins is (a) that the read THROWS at all where a build
    /// without the checkpoint completes it, and (b) the honest lower bound below. The DISCRIMINATING
    /// upper bound (stopped before the end) lives in the four-description arm above.
    runInFreshThread([]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        const std::string description = makeLongBinaryTypeDescription(elements_per_type);
        const std::string one_type = makeDynamicStructurePrefixV3({description});

        size_t served_bytes = 0;
        try
        {
            const size_t served = readDynamicStructurePrefix(
                one_type, served_bytes, [&] { query.cancel(); }, 2 * 4096);
            FAIL() << "a cancelled query read the whole V3 variant list to completion (" << served << " bytes)";
        }
        catch (const Exception & e)
        {
            ASSERT_EQ(e.code(), ErrorCodes::QUERY_WAS_CANCELLED) << e.message();
            /// The whole description, i.e. the interruption came from the loop's `check()` after it and
            /// not from inside `decodeDataType`. This is the honest LOWER bound on `served_bytes`, which
            /// is the coarsest granularity the read can have today, and it is what must be tightened when
            /// the decoder gains a hook - if the decoder now polls cancellation, tighten this bound
            /// instead of deleting the cell.
            ASSERT_GE(served_bytes, description.size())
                << "the read stopped inside a description, which `decodeDataType` cannot do";
        }
    });
}

/// The checkpoint that bounds `DataTypeVariant`'s constructor, which is the last stream-sized pass of a
/// V3 prefix and the one no other cell can reach. That constructor canonicalizes: it calls `getName`
/// once per DECODED TYPE into a `std::map`, `getName` is not memoized, and `DataTypeTuple::doGetName`
/// walks every element of a tuple whose length the stream chose - so its cost is bounded by the stream,
/// not by the number of variants that survive squashing.
///
/// The fixture shape is forced by a measured cost ratio. Per tuple element, decoding costs ~0.24 us and
/// `getName` ~0.013 us, i.e. decoding is ~18x more expensive. So ONE description wide enough for its
/// canonicalization to span the checkpoint's 10 ms period spends ~180 ms being decoded first, and the
/// per-description checkpoint after it always polls before the constructor is even reached (measured:
/// one 1e6-element description decodes in 240 ms and its own checkpoint is what throws).
///
/// MANY NARROW descriptions escape that: the constructor's cost is the SUM over all decoded types while
/// each per-description checkpoint only ever sees ONE description's decode. 250 descriptions of 4000
/// elements put each decode at ~1 ms, comfortably inside the period, while the constructor's ~13 ms of
/// `getName` crosses it. They are identical types, so they squash to one variant plus the shared one and
/// `checkVariantsNotEmptyAndNotTooMany` is satisfied; the pre-ctor bound that matters is
/// `MAX_DYNAMIC_TYPES_LIMIT` = 254 on the declared count.
TEST(ObjectSerialization, PrefixReadDynamicStructureV3BoundsVariantCanonicalization)
{
    runInFreshThread([]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        /// Just inside `MAX_DYNAMIC_TYPES_LIMIT`, at a width whose own decode stays well inside the
        /// period while 250 of them make the canonicalization cross it.
        static constexpr size_t num_types = 250;
        static constexpr size_t elements_per_type = 4000;

        const std::string description = makeLongBinaryTypeDescription(elements_per_type);
        const std::vector<std::string> descriptions(num_types, description);
        const std::string prefix = makeDynamicStructurePrefixV3(descriptions);

        /// Served unpaced, so the only thing that can move the throttle is the work itself rather than
        /// the stream: that is what makes the canonicalization the pass that crosses the period.
        ///
        /// The cancellation is delivered on the LAST bytes, so every per-description checkpoint has
        /// already run while the query was live. The chunk size has to keep it off the FIRST chunk - the
        /// version word is read before the checker is constructed, so a cancellation delivered on chunk 1
        /// would be seen by the checker's own entry poll and the cell would pass without reaching the
        /// loop at all.
        static constexpr size_t chunk_size = 64 * 1024;
        size_t served_bytes = 0;
        try
        {
            const size_t served = readDynamicStructurePrefix(
                prefix,
                served_bytes,
                [&] { query.cancel(); },
                prefix.size(),
                chunk_size,
                /*delay=*/ std::chrono::microseconds(0));
            FAIL() << "a cancelled query completed a V3 prefix whose variant canonicalization alone "
                   << "spans the checkpoint's period (" << served << " bytes served)";
        }
        catch (const Exception & e)
        {
            ASSERT_EQ(e.code(), ErrorCodes::QUERY_WAS_CANCELLED) << e.message();
            /// The whole variant list, which is what says the interruption came from a checkpoint at or
            /// after the end of that list rather than from one between descriptions.
            ASSERT_GE(served_bytes, num_types * description.size())
                << "the read stopped before the end of the variant list, so it was interrupted by a "
                << "checkpoint inside the list rather than by the one bounding the canonicalization";
        }
    });
}

/// Many prefixes, each individually below the checkpoint's throttle period, aggregating well above it -
/// the shape a real read has (a Compact part reads a prefix per column per mark, a Wide part one per
/// granule, a Compact shared-data read one per bucket).
///
/// This pins the BOUND, not the throttle state's lifetime: it cannot distinguish thread-lived state
/// from per-prefix state, because the constructor's entry poll throws on a prefix that starts already
/// cancelled either way. The lifetime's rationale is in `PrefixReadCancellationChecker.h`.
TEST(ObjectSerialization, CumulativeSubThresholdPrefixesObserveCancellation)
{
    runInFreshThread([]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        /// 3 chunks of the paced stream each, i.e. ~4 ms per prefix against the 10 ms period, so no
        /// single prefix can cross it on its own.
        const std::string small = makeGranulePrefixWithPaths(makeShortPaths(1000));

        /// Cancelled from inside the FIRST prefix, one chunk in, and every later prefix then starts
        /// already cancelled. The entry poll therefore interrupts prefix 2 at the latest, so what this
        /// cell pins is not "some prefix is interrupted" but the bound: the read must stop within a
        /// couple of prefixes rather than running to 40.
        size_t served_bytes = 0;
        size_t completed = 0;
        auto cancel = [&] { query.cancel(); };
        for (size_t i = 0; i != 40; ++i)
        {
            try
            {
                /// Only the first prefix carries the cancel hook; the rest are plain reads.
                if (i == 0)
                    readGranulePrefix(small, served_bytes, cancel, 2 * 4096);
                else
                    readGranulePrefix(small, served_bytes);
                ++completed;
            }
            catch (const Exception & e)
            {
                ASSERT_EQ(e.code(), ErrorCodes::QUERY_WAS_CANCELLED) << e.message();
                ASSERT_LE(completed, 2u)
                    << completed << " sub-threshold prefixes completed after the cancellation, i.e. the "
                    << "checkpoint's grace period restarts for each prefix instead of bounding the whole span";
                return;
            }
        }

        FAIL() << "40 consecutive sub-threshold prefix reads were never interrupted, i.e. the "
                  "checkpoint's throttle restarts for each prefix";
    });
}

/// The `reportBroken` suppression the readers apply is keyed on `isCancelledPrefixRead`, so its
/// directions are what keep that suppression narrow: true when the prefix-read checkpoint raised the
/// in-flight exception, false for every other failure - including one raised while the query IS
/// cancelled, which is exactly the race a "is the query cancelled right now" test would mishandle.
///
/// It must also be non-consuming and survive being re-thrown, because guarded handlers nest:
/// `MergeTreeSequentialSource::generate`'s function-try encloses `MergeTreeReaderWide::readRows`' catch,
/// which asks first, calls `addMessage` for diagnostics and rethrows. If the answer were consumed or
/// the derived type lost on the way, the outer handler would decide a healthy part is broken.
TEST(ObjectSerialization, CancelledPrefixReadIsKeyedOnTheExceptionAndDoesNotChangeOnAsking)
{
    runInFreshThread([]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        ASSERT_FALSE(isCancelledPrefixRead()) << "reported a cancellation with no exception in flight";

        /// A read failure that is not a cancellation: a truncated prefix, whose declared path count the
        /// stream cannot back.
        WriteBufferFromOwnString truncated;
        writeVarUInt(static_cast<UInt64>(1), truncated);  /// num_rows
        writeVarUInt(static_cast<UInt64>(10), truncated); /// num_paths, with no path bytes following
        size_t served_bytes = 0;
        try
        {
            readGranulePrefix(truncated.str(), served_bytes);
            FAIL() << "a truncated prefix was read successfully";
        }
        catch (const Exception &)
        {
            ASSERT_FALSE(isCancelledPrefixRead()) << "a read failure was mistaken for a cancellation";
        }

        /// The same read failure, raised while the query IS cancelled, must still answer false: this is
        /// the window in which a "is the query cancelled right now" test would wrongly suppress a
        /// genuine corruption report. Hand-built rather than driven through a read, because the
        /// checkpoint polls once at prefix entry, so a read started after `cancel()` is interrupted
        /// there and never reaches the truncated bytes - a real cancellation, correctly reported as one.
        query.cancel();
        try
        {
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read all data.");
        }
        catch (const Exception &)
        {
            ASSERT_FALSE(isCancelledPrefixRead())
                << "a read failure raised while the query is cancelled was mistaken for a cancellation";
        }

        /// And a real cancellation from inside the loop must be recognised - every time it is asked,
        /// and still after the readers' diagnostic `addMessage` + bare rethrow.
        const std::string bytes = makeGranulePrefixWithPaths(makeShortPaths(8000));
        try
        {
            readGranulePrefix(bytes, served_bytes);
            FAIL() << "a cancelled query read the whole prefix to completion";
        }
        catch (const Exception &)
        {
            ASSERT_TRUE(isCancelledPrefixRead()) << "the checkpoint's own exception was not recognised";
            ASSERT_TRUE(isCancelledPrefixRead()) << "asking twice changed the answer";

            /// Exactly what MergeTreeReaderWide::readRows does between the two guarded handlers.
            try
            {
                try
                {
                    rethrow_exception(std::current_exception());
                }
                catch (Exception & e)
                {
                    e.addMessage("(while reading prefix of column json.a)");
                }
                throw;
            }
            catch (const Exception & e)
            {
                ASSERT_TRUE(isCancelledPrefixRead())
                    << "the derived type was lost across addMessage and a bare rethrow: " << e.message();
                /// The type must stay invisible to users: `Poco::Exception::displayText` prepends
                /// `name()`, and nine in-tree tests assert the literal "DB::Exception: Query was
                /// cancelled". Overriding `name()` here would change every cancellation message a
                /// client sees for a JSON column read.
                ASSERT_STREQ(e.name(), "DB::Exception");
                ASSERT_EQ(e.displayText().find("DB::Exception: "), 0u) << e.displayText();
                ASSERT_NE(e.message().find("while reading prefix of column json.a"), std::string::npos)
                    << e.message();
                ASSERT_EQ(e.code(), ErrorCodes::QUERY_WAS_CANCELLED) << e.message();
            }
        }
    });
}

/// The must-not-regress control for every test above: with the same fixtures and an attached but
/// uncancelled query the reads must complete and consume the whole prefix. Without it, all of them
/// would also pass on a build that always throws.
TEST(ObjectSerialization, PrefixReadNotCancelledCompletesNormally)
{
    runInFreshThread([]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;
        size_t served_bytes = 0;

        const std::string short_names = makeGranulePrefixWithPaths(makeShortPaths(8000));
        ASSERT_EQ(readGranulePrefix(short_names, served_bytes), short_names.size());

        const std::string empty_names = makeGranulePrefixWithPaths(std::vector<std::string>(100000, ""));
        ASSERT_EQ(readGranulePrefix(empty_names, served_bytes), empty_names.size());

        const std::string one_long_name = makeGranulePrefixWithPaths({std::string(1024 * 1024, 'x')});
        ASSERT_EQ(readGranulePrefix(one_long_name, served_bytes), one_long_name.size());

        /// The outer `Object` structure prefix, same two fixtures its cancellation test uses.
        const std::string structure_short = makeObjectStructurePrefixWithPaths(makeShortPaths(8000));
        ASSERT_EQ(readObjectStructurePrefix(structure_short, served_bytes), structure_short.size());

        const std::string structure_long = makeObjectStructurePrefixWithPaths({std::string(1024 * 1024, 'x')});
        ASSERT_EQ(readObjectStructurePrefix(structure_long, served_bytes), structure_long.size());

        const std::string structure_empty = makeObjectStructurePrefixWithPaths(std::vector<std::string>(100000, ""));
        ASSERT_EQ(readObjectStructurePrefix(structure_empty, served_bytes), structure_empty.size());

        /// The `Dynamic` structure prefix, same two fixtures its cancellation test uses.
        const std::string dynamic_empty = makeDynamicStructurePrefixWithStatistics(std::vector<std::string>(100000, ""));
        ASSERT_EQ(readDynamicStructurePrefix(dynamic_empty, served_bytes), dynamic_empty.size());

        const std::string dynamic_long = makeDynamicStructurePrefixWithStatistics({std::string(1024 * 1024, 'x')});
        ASSERT_EQ(readDynamicStructurePrefix(dynamic_long, served_bytes), dynamic_long.size());

        /// And the cumulative-throttle fixture: 40 consecutive sub-threshold prefixes must all complete
        /// when the query is not cancelled, so that test cannot pass merely because the throttle fires.
        const std::string small = makeGranulePrefixWithPaths(makeShortPaths(1000));
        for (size_t i = 0; i != 40; ++i)
            ASSERT_EQ(readGranulePrefix(small, served_bytes), small.size()) << "iteration " << i;
    });
}

namespace
{

/// The dynamic-path prefixes of one `Object` column are deserialized in parallel over a thread pool,
/// and the collector afterwards keeps ONE of the failures. This drives the real
/// `SerializationObject::deserializeBinaryBulkStatePrefix` through that pool with two dynamic paths
/// whose per-path streams fail differently: an EARLY path whose `Dynamic` structure prefix is corrupt,
/// and a LATER path served slowly enough that it is still reading when the query is cancelled.
struct ObjectPrefixFanOutResult
{
    int code = 0;
    bool is_cancellation = false;
    bool threw = false;
};

/// Serves one chunk and then fails with a RETRYABLE error, which `isRetryableException` recognises by
/// code (`checkDataPart.cpp`, the `catch (const Exception &)` arm). Stands in for a remote or S3 read
/// dropping mid-prefix, without a socket: what the collector has to get right is only that such a
/// failure is a genuine one, so it must not be replaced by another task's cancellation and must not
/// itself displace another task's corruption.
class RetryableFailureReadBuffer : public ReadBuffer
{
public:
    RetryableFailureReadBuffer(std::string data_, size_t chunk_size_)
        : ReadBuffer(nullptr, 0), data(std::move(data_)), chunk_size(chunk_size_)
    {
    }

private:
    bool nextImpl() override
    {
        if (served)
            throw Exception(ErrorCodes::NETWORK_ERROR, "Simulated connection reset mid-prefix");

        served = true;
        const size_t size = std::min(chunk_size, data.size());
        working_buffer = Buffer(data.data(), data.data() + size);
        return true;
    }

    std::string data;
    size_t chunk_size;
    bool served = false;
};

ObjectPrefixFanOutResult readObjectPrefixWithTwoPathStreams(
    const std::string & path_a_prefix,
    const std::string & path_b_prefix,
    const std::function<void()> & cancel,
    size_t cancel_after_bytes,
    bool path_a_fails_retryable = false)
{
    /// Sorted, because the structure prefix declares a SORTED path list and the fan-out batches it in
    /// that order: "a" is read by the EARLIER task, which is the case that only passes when the
    /// collector prefers its corruption over the later task's cancellation.
    ///
    /// Written out here rather than via `makeObjectStructurePrefixWithPaths` because statistics are
    /// enabled below, so the V2 prefix also carries one count per dynamic path plus the shared-data
    /// statistics list.
    WriteBufferFromOwnString structure;
    writeBinaryLittleEndian(static_cast<UInt64>(SerializationObject::SerializationVersion::V2), structure);
    writeVarUInt(static_cast<UInt64>(2), structure); /// number of dynamic paths
    writeStringBinary(std::string("a"), structure);
    writeStringBinary(std::string("b"), structure);
    writeVarUInt(static_cast<UInt64>(1), structure); /// statistics for "a"
    writeVarUInt(static_cast<UInt64>(1), structure); /// statistics for "b"
    writeVarUInt(static_cast<UInt64>(0), structure); /// no shared-data paths statistics
    const std::string structure_bytes = structure.str();

    auto type = DataTypeFactory::instance().get("JSON");
    auto serialization = type->getDefaultSerialization();

    ReadBufferFromString structure_stream(structure_bytes);
    ReadBufferFromString path_a_stream(path_a_prefix);
    RetryableFailureReadBuffer path_a_retryable_stream(path_a_prefix, 8);
    ReadBuffer * path_a_buffer = path_a_fails_retryable
        ? static_cast<ReadBuffer *>(&path_a_retryable_stream)
        : static_cast<ReadBuffer *>(&path_a_stream);
    /// Only path "b" is paced, so the cancellation lands while "b" is mid-read.
    SlowReadBuffer path_b_stream(path_b_prefix, 4096, std::chrono::milliseconds(2), cancel, cancel_after_bytes);

    /// One thread per path, so both tasks really run concurrently.
    ThreadPool pool(
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        /*max_threads=*/2, /*max_free_threads=*/2, /*queue_size=*/2);

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.prefixes_deserialization_thread_pool = &pool;
    /// As both production readers do (`MergeTreeReaderWide`, `MergeTreeReaderCompact`). Without it a
    /// `Dynamic` prefix stops after its few header bytes, so the paced path would finish before the
    /// cancellation could land and the cells below would pass without testing anything.
    settings.object_and_dynamic_read_statistics = true;
    settings.getter = [&](const ISerialization::SubstreamPath & path) -> ReadBuffer *
    {
        if (path.empty())
            return nullptr;
        if (path.back().type == ISerialization::Substream::ObjectStructure)
            return &structure_stream;
        if (path.back().type == ISerialization::Substream::DynamicStructure)
        {
            /// The dynamic path name is carried by the enclosing `ObjectDynamicPath` element.
            for (const auto & element : path)
            {
                if (element.type == ISerialization::Substream::ObjectDynamicPath)
                {
                    if (element.object_path_name == "a")
                        return path_a_buffer;
                    return &path_b_stream;
                }
            }
        }
        return nullptr;
    };

    ObjectPrefixFanOutResult result;
    ISerialization::DeserializeBinaryBulkStatePtr state;
    try
    {
        serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);
    }
    catch (const Exception & e)
    {
        result.threw = true;
        result.code = e.code();
        result.is_cancellation = isPrefixReadCancelled(std::current_exception());
    }
    return result;
}

}

/// A cancellation is observed by every prefix task still reading once the query is cancelled, so when
/// several tasks fail it must not be the one that surfaces: the readers key their `reportBroken`
/// suppression on that type, so a cancellation from one task would hide another task's genuine read
/// failure and leave a corrupt part unchecked. A cancellation is surfaced only when EVERY failing task
/// produced one.
TEST(ObjectSerialization, PrefixFanOutPrefersReadFailureOverCancellation)
{
    /// A `Dynamic` structure prefix with an invalid serialization version, rejected up front by
    /// `SerializationVersion::checkVersion`. Deliberately a failure this task raises on its own, so it
    /// cannot itself be a cancellation.
    WriteBufferFromOwnString corrupt;
    writeBinaryLittleEndian(static_cast<UInt64>(9999), corrupt);
    const std::string corrupt_prefix = corrupt.str();

    /// Long enough that this path is still reading when the cancellation lands.
    const std::string slow_prefix = makeDynamicStructurePrefixWithStatistics(std::vector<std::string>(100000, ""));
    /// The same shape with no names, which is read without ever reaching a checkpoint.
    const std::string healthy_prefix = makeDynamicStructurePrefixWithStatistics({});

    runInFreshThread([&]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        const auto result = readObjectPrefixWithTwoPathStreams(
            corrupt_prefix, slow_prefix, [&] { query.cancel(); }, 2 * 4096);

        ASSERT_TRUE(result.threw) << "a corrupt dynamic-path prefix was read successfully";
        ASSERT_FALSE(result.is_cancellation)
            << "one task's cancellation hid another task's read failure, so the readers would skip "
               "reporting a genuinely corrupt part; the surfaced code was " << result.code;
        ASSERT_EQ(result.code, ErrorCodes::INCORRECT_DATA) << "unexpected failure code " << result.code;
    });

    /// The control that keeps the preference from degenerating into "never surface a cancellation":
    /// when the cancelling task is the ONLY one that failed, a cancellation must still be what
    /// surfaces, otherwise the readers would report a part broken for a plain cancelled read - the
    /// very bug this change fixes.
    runInFreshThread([&]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        const auto result = readObjectPrefixWithTwoPathStreams(
            healthy_prefix, slow_prefix, [&] { query.cancel(); }, 2 * 4096);

        ASSERT_TRUE(result.threw) << "a cancelled query read both dynamic-path prefixes to completion";
        ASSERT_TRUE(result.is_cancellation)
            << "the only failure was a cancellation, but it did not surface as one; code was " << result.code;
    });

    /// A single failing task must surface its own exception unchanged, whichever kind it is: the
    /// preference only chooses between SEVERAL failures. Here the only failure is the corruption, with
    /// no cancellation anywhere.
    runInFreshThread([&]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        const auto result = readObjectPrefixWithTwoPathStreams(corrupt_prefix, healthy_prefix, {}, 0);

        ASSERT_TRUE(result.threw) << "a corrupt dynamic-path prefix was read successfully";
        ASSERT_FALSE(result.is_cancellation) << "a lone read failure was reported as a cancellation";
        ASSERT_EQ(result.code, ErrorCodes::INCORRECT_DATA) << "unexpected failure code " << result.code;
    });

    /// And with nothing cancelled and nothing corrupt the fan-out must simply succeed, so no cell
    /// above can pass on a build that always throws.
    runInFreshThread([&]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        const auto result = readObjectPrefixWithTwoPathStreams(healthy_prefix, healthy_prefix, {}, 0);

        ASSERT_FALSE(result.threw)
            << "an uncancelled read of two healthy dynamic-path prefixes failed with code " << result.code;
    });

    /// The preference is only over cancellations: among GENUINE failures the last one still wins, as
    /// it did before this change. That matters because the readers also suppress `reportBroken` for a
    /// RETRYABLE failure (`isRetryableException`), so preferring an earlier task's retryable error
    /// over a later task's corruption would leave a corrupt part unchecked - a part that would have
    /// been reported before this change. Here the EARLIER task's stream drops with a retryable
    /// network error and the LATER one is corrupt, so only the last-wins ordering surfaces the
    /// corruption.
    runInFreshThread([&]
    {
        ThreadStatus thread_status;
        CancellableQueryFixture query;

        const auto result = readObjectPrefixWithTwoPathStreams(
            healthy_prefix, corrupt_prefix, {}, 0, /*path_a_fails_retryable=*/true);

        ASSERT_TRUE(result.threw) << "neither a retryable read failure nor a corrupt prefix was raised";
        ASSERT_EQ(result.code, ErrorCodes::INCORRECT_DATA)
            << "an earlier task's retryable failure displaced a later task's corruption, so the "
               "readers would skip reporting a genuinely corrupt part; the surfaced code was "
            << result.code;
        ASSERT_FALSE(result.is_cancellation) << "a retryable read failure was reported as a cancellation";
    });
}
