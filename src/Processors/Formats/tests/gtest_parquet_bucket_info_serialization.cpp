#include <gtest/gtest.h>
#include "config.h"

#if USE_PARQUET

#include <IO/ReadBufferFromMemory.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/ParquetV3BlockInputFormat.h>
#include <Core/ProtocolDefines.h>

#include <cstring>

using namespace DB;

namespace
{

/// An older protocol version that already understands the file-bucket payload, but predates the
/// `file_num_row_groups` field. Used to model an old peer in a mixed-version cluster.
constexpr auto OLD_VERSION = DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_FILE_STATS;
constexpr auto NEW_VERSION = DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_PARQUET_FILE_ROW_GROUP_COUNT;

String serializeBucket(const std::vector<size_t> & row_group_ids, size_t file_num_row_groups, size_t protocol_version)
{
    ParquetFileBucketInfo info(row_group_ids, file_num_row_groups);
    String str;
    {
        WriteBufferFromString out(str);
        info.serialize(out, protocol_version);
    }
    return str;
}

}

/// At the new protocol version both the row-group ids and the row-group count round-trip.
TEST(ParquetFileBucketInfoSerialization, NewVersionRoundTripsRowGroupCount)
{
    const std::vector<size_t> row_group_ids = {0, 2, 5};
    const String str = serializeBucket(row_group_ids, /*file_num_row_groups=*/7, NEW_VERSION);

    ParquetFileBucketInfo restored;
    ReadBufferFromMemory in(str);
    restored.deserialize(in, NEW_VERSION);

    ASSERT_TRUE(in.eof());
    EXPECT_EQ(restored.row_group_ids, row_group_ids);
    EXPECT_EQ(restored.file_num_row_groups, 7u);
}

/// At an older protocol version the count is neither written nor read; it stays 0 ("unknown"),
/// which disables the read-path row-group-count check rather than failing.
TEST(ParquetFileBucketInfoSerialization, OldVersionOmitsRowGroupCount)
{
    const std::vector<size_t> row_group_ids = {0, 2, 5};
    const String str = serializeBucket(row_group_ids, /*file_num_row_groups=*/7, OLD_VERSION);

    ParquetFileBucketInfo restored;
    ReadBufferFromMemory in(str);
    restored.deserialize(in, OLD_VERSION);

    ASSERT_TRUE(in.eof());
    EXPECT_EQ(restored.row_group_ids, row_group_ids);
    EXPECT_EQ(restored.file_num_row_groups, 0u);
}

/// The version gate keeps a mixed-version cluster task stream aligned. The bucket payload written at
/// the older version is exactly the new-version payload without the trailing row-group-count and
/// footer-digest varints, so a peer reading at the older version stops right after the ids: the
/// following field on the wire
/// (e.g. `iceberg_info`, modelled here by a sentinel varint) is read at the correct offset instead
/// of being misparsed as the extra count.
TEST(ParquetFileBucketInfoSerialization, VersionGateKeepsStreamAligned)
{
    const std::vector<size_t> row_group_ids = {1, 4};
    const size_t file_num_row_groups = 9;

    const String old_str = serializeBucket(row_group_ids, file_num_row_groups, OLD_VERSION);
    const String new_str = serializeBucket(row_group_ids, file_num_row_groups, NEW_VERSION);

    /// The old payload is a strict prefix of the new payload (only the count and digest varints are
    /// appended).
    EXPECT_LT(old_str.size(), new_str.size());
    EXPECT_EQ(old_str, new_str.substr(0, old_str.size()));

    /// Model the next field on the wire after the bucket payload.
    const UInt64 sentinel = 0x1234;
    String sentinel_bytes;
    {
        WriteBufferFromString out(sentinel_bytes);
        writeVarUInt(sentinel, out);
    }

    const String wire = old_str + sentinel_bytes;
    ParquetFileBucketInfo restored;
    ReadBufferFromMemory in(wire);
    restored.deserialize(in, OLD_VERSION);

    EXPECT_EQ(restored.row_group_ids, row_group_ids);
    EXPECT_EQ(restored.file_num_row_groups, 0u);

    /// The reader must be positioned exactly at the sentinel, proving the stream stayed aligned.
    UInt64 read_sentinel = 0;
    readVarUInt(read_sentinel, in);
    EXPECT_EQ(read_sentinel, sentinel);
    EXPECT_TRUE(in.eof());
}

/// A bucket that knows the file's row-group count requires the newer protocol so the worker can run
/// the `checkFileMatchesBucketAssignment` overwrite guard. This is what makes
/// `ClusterFunctionReadTaskResponse::serialize` fail closed rather than downgrade the task to an older
/// worker that would drop the count and silently disable the guard. A bucket with an unknown count (0)
/// has no guard to lose and only needs the base file-bucket protocol version.
TEST(ParquetFileBucketInfoSerialization, MinProtocolVersionRequiresRowGroupCountWhenKnown)
{
    ParquetFileBucketInfo with_count({0, 1}, /*file_num_row_groups=*/7);
    EXPECT_EQ(with_count.getMinProtocolVersion(), static_cast<UInt64>(NEW_VERSION));

    ParquetFileBucketInfo without_count({0, 1}, /*file_num_row_groups=*/0);
    EXPECT_EQ(
        without_count.getMinProtocolVersion(),
        static_cast<UInt64>(DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_FILE_BUCKETS_INFO));

    /// The footer digest is the stronger of the two guards, so it alone also requires the newer
    /// protocol - an older worker would drop it and read a bucket unguarded against an overwrite that
    /// keeps the row-group count.
    ParquetFileBucketInfo with_digest_only({0, 1}, /*file_num_row_groups=*/0);
    with_digest_only.footer_digest = 0xdeadbeef;
    EXPECT_EQ(with_digest_only.getMinProtocolVersion(), static_cast<UInt64>(NEW_VERSION));
}

/// A trivial one-bucket split covering every row group of the file may be stripped from a task for
/// an older worker (`ClusterFunctionReadTaskResponse::serialize`): reading the plain path once
/// returns exactly the same rows. Anything less than proven whole-file coverage must answer false,
/// so the fail-close protocol gate stays in force.
TEST(ParquetFileBucketInfoSerialization, CoversWholeFile)
{
    EXPECT_TRUE(ParquetFileBucketInfo({0, 1, 2}, /*file_num_row_groups=*/3).coversWholeFile());
    EXPECT_TRUE(ParquetFileBucketInfo({0}, /*file_num_row_groups=*/1).coversWholeFile());

    /// A strict subset of the row groups.
    EXPECT_FALSE(ParquetFileBucketInfo({0, 1}, /*file_num_row_groups=*/3).coversWholeFile());
    /// Unknown total: coverage cannot be proven.
    EXPECT_FALSE(ParquetFileBucketInfo({0, 1, 2}, /*file_num_row_groups=*/0).coversWholeFile());
    /// Right size but not the full 0..n-1 range.
    EXPECT_FALSE(ParquetFileBucketInfo({1, 2, 3}, /*file_num_row_groups=*/3).coversWholeFile());
    EXPECT_FALSE(ParquetFileBucketInfo({0, 0, 2}, /*file_num_row_groups=*/3).coversWholeFile());
}

/// `footer_digest` is the exact generation token of the file the assignment describes, so it must
/// reach the worker that reads a bucket - otherwise a distributed bucketed read could only check the
/// row-group count and would miss an overwrite that keeps it. At the new protocol version it
/// round-trips; an older peer neither receives nor reads it (and never gets such a bucket at all,
/// because `getMinProtocolVersion` fails the task closed instead).
TEST(ParquetFileBucketInfoSerialization, NewVersionRoundTripsFooterDigest)
{
    const std::vector<size_t> row_group_ids = {0, 2, 5};
    ParquetFileBucketInfo info(row_group_ids, /*file_num_row_groups=*/7);
    info.footer_digest = 0xdeadbeef;

    String str;
    {
        WriteBufferFromString out(str);
        info.serialize(out, NEW_VERSION);
    }

    ParquetFileBucketInfo restored;
    ReadBufferFromMemory in(str);
    restored.deserialize(in, NEW_VERSION);
    ASSERT_TRUE(in.eof());
    EXPECT_EQ(restored.row_group_ids, row_group_ids);
    EXPECT_EQ(restored.file_num_row_groups, 7u);
    EXPECT_EQ(restored.footer_digest, 0xdeadbeefu);

    EXPECT_EQ(info.getMinProtocolVersion(), static_cast<UInt64>(NEW_VERSION));

    /// At the older version the digest is not on the wire, so it stays 0 ("unknown") and the read-path
    /// footer check is disabled rather than comparing against garbage.
    String old_str;
    {
        WriteBufferFromString out(old_str);
        info.serialize(out, OLD_VERSION);
    }
    ParquetFileBucketInfo restored_old;
    ReadBufferFromMemory in_old(old_str);
    restored_old.deserialize(in_old, OLD_VERSION);
    ASSERT_TRUE(in_old.eof());
    EXPECT_EQ(restored_old.footer_digest, 0u);
}

/// Narrowing a bucket to the row groups that survived the query condition cache must keep the
/// footer digest: the narrowed bucket still describes the same file generation, and dropping the
/// digest would silently disable the fail-close guard on the read path.
TEST(ParquetFileBucketInfoSerialization, FilterByMatchingRowGroupsKeepsFooterDigest)
{
    ParquetFileBucketInfo info({0, 2, 5}, /*file_num_row_groups=*/7);
    info.footer_digest = 0xdeadbeef;

    auto filtered = std::dynamic_pointer_cast<ParquetFileBucketInfo>(
        info.filterByMatchingRowGroups({2, 5}, /*file_num_row_groups=*/7));
    ASSERT_NE(filtered, nullptr);
    EXPECT_EQ(filtered->row_group_ids, (std::vector<size_t>{2, 5}));
    EXPECT_EQ(filtered->footer_digest, 0xdeadbeefu);

    /// The prototype path (empty ids = "keep everything that matched") keeps it as well.
    ParquetFileBucketInfo prototype;
    prototype.footer_digest = 0xdeadbeef;
    auto from_prototype = std::dynamic_pointer_cast<ParquetFileBucketInfo>(
        prototype.filterByMatchingRowGroups({1, 3}, /*file_num_row_groups=*/4));
    ASSERT_NE(from_prototype, nullptr);
    EXPECT_EQ(from_prototype->footer_digest, 0xdeadbeefu);
}

/// An assignment derived from the query condition cache omits row groups that were pruned, not
/// handed to another reader, so it must be marked as such: the read path attributes the
/// `ParquetPrunedRowGroups` event to the whole file for it, and to the bucket alone for a split.
TEST(ParquetFileBucketInfoSerialization, CacheFilteredPrototypeMarksOmittedAsPruned)
{
    ParquetFileBucketInfo prototype;
    auto from_prototype = prototype.filterByMatchingRowGroups({0, 2}, /*file_num_row_groups=*/4);
    ASSERT_NE(from_prototype, nullptr);
    EXPECT_TRUE(from_prototype->omitted_row_groups_are_pruned);

    /// A split bucket stays accountable for its own row groups only.
    EXPECT_FALSE(ParquetFileBucketInfo({0, 1}, /*file_num_row_groups=*/4).omitted_row_groups_are_pruned);
}

/// The footer digest must not read the footer's thrift enums: a malformed or future-writer file can
/// carry an out-of-range enumerator there (`encoding_stats` is advisory metadata that
/// `Reader::columnChunkCanUseDictionaryFilter` deliberately reads through `isValidThriftEnum`), and
/// loading it as an enumerator is undefined behavior that `-fsanitize=enum` turns into an aborted
/// query. Digesting such a footer must be well-defined and stable.
TEST(ParquetFileBucketInfoSerialization, FooterDigestIgnoresOutOfRangeThriftEnums)
{
    parquet::format::FileMetaData metadata;
    metadata.num_rows = 200;
    metadata.schema.resize(1);
    metadata.schema[0].name = "root";
    metadata.schema[0].__set_num_children(1);

    metadata.row_groups.resize(1);
    auto & row_group = metadata.row_groups[0];
    row_group.num_rows = 200;
    row_group.total_byte_size = 1000;
    row_group.columns.resize(1);
    auto & column = row_group.columns[0];
    column.file_offset = 4;
    column.__isset.meta_data = true;
    auto & meta = column.meta_data;
    meta.num_values = 200;
    meta.total_compressed_size = 500;
    meta.total_uncompressed_size = 900;
    meta.data_page_offset = 100;
    meta.path_in_schema = {"s"};

    /// An out-of-range `page_type` / `encoding`, written through `memcpy` so this test itself does
    /// not create the enumerator value it is guarding against.
    meta.encoding_stats.resize(1);
    meta.__isset.encoding_stats = true;
    meta.encoding_stats[0].count = 1;
    const Int32 invalid = -64;
    memcpy(&meta.encoding_stats[0].page_type, &invalid, sizeof(invalid));
    memcpy(&meta.encoding_stats[0].encoding, &invalid, sizeof(invalid));

    const UInt64 digest = computeParquetFooterDigest(metadata);
    EXPECT_NE(digest, 0u);
    EXPECT_EQ(digest, computeParquetFooterDigest(metadata));

    /// A different generation of the file (here: a different row-group size) digests differently.
    parquet::format::FileMetaData other = metadata;
    other.row_groups[0].total_byte_size = 1001;
    EXPECT_NE(computeParquetFooterDigest(other), digest);
}

#endif
