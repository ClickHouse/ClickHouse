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
static constexpr auto DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION = 9;

}
