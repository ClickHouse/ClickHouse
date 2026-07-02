#include <gtest/gtest.h>
#include "config.h"

#if USE_PARQUET

#include <IO/ReadBufferFromMemory.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/ParquetV3BlockInputFormat.h>
#include <Core/ProtocolDefines.h>

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
/// the older version is exactly the new-version payload without the trailing row-group-count varint,
/// so a peer reading at the older version stops right after the ids: the following field on the wire
/// (e.g. `iceberg_info`, modelled here by a sentinel varint) is read at the correct offset instead
/// of being misparsed as the extra count.
TEST(ParquetFileBucketInfoSerialization, VersionGateKeepsStreamAligned)
{
    const std::vector<size_t> row_group_ids = {1, 4};
    const size_t file_num_row_groups = 9;

    const String old_str = serializeBucket(row_group_ids, file_num_row_groups, OLD_VERSION);
    const String new_str = serializeBucket(row_group_ids, file_num_row_groups, NEW_VERSION);

    /// The old payload is a strict prefix of the new payload (only the count varint is appended).
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

#endif
