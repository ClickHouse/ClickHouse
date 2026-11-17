#pragma once

namespace DB
{

/// Versions of the parallel replicas coordination protocol (read requests and announcements).
static constexpr auto DBMS_MIN_SUPPORTED_PARALLEL_REPLICAS_PROTOCOL_VERSION = 3;
static constexpr auto DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_MARK_SEGMENT_SIZE_FIELD = 4;
static constexpr auto DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_PROJECTION = 5;
static constexpr auto DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_MIN_MARKS_PER_TASK = 6;
static constexpr auto DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_STREAM_ID = 7;
static constexpr auto DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_ANNOUNCEMENT_RESPONSE = 8;
static constexpr auto DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_ANALYZED_RANGES_REQUEST = 9;
static constexpr auto DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_TOTAL_MARKS_IN_PART = 10;
static constexpr auto DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_PART_FINGERPRINT = 11;
static constexpr auto DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION = 11;

/// The `read_bucket` task parameter of a distributed read plan (`make_distributed_plan`) embeds a
/// `RangesInDataPartsDescription` blob, but - unlike every other consumer of that serializer - it
/// travels as an opaque query parameter with no handshake to negotiate a parallel replicas protocol
/// version on. Both producer (`ReadFromMergeTree::serializeDistributedReadBuckets`) and consumer
/// (the `read_bucket` reader) must therefore agree on one layout that never changes, otherwise a
/// rolling upgrade where the two peers were compiled with different
/// `DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION` values silently misparses the rest of the blob.
/// Pin the layout to 8 - the version in effect when the blob was introduced - and keep it pinned:
/// the marks it carries do not need any field added later. Never change this value; if the blob
/// ever needs a new field, give it its own explicit version prefix instead.
static constexpr auto DBMS_PARALLEL_REPLICAS_DISTRIBUTED_READ_BUCKET_VERSION = 8;

}
