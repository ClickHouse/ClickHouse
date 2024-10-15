#pragma once

namespace DB
{

static constexpr auto DBMS_MIN_REVISION_WITH_CLIENT_INFO = 54032;
static constexpr auto DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE = 54058;
static constexpr auto DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO = 54060;
static constexpr auto DBMS_MIN_REVISION_WITH_TABLES_STATUS = 54226;
static constexpr auto DBMS_MIN_REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME_DATA_TYPE = 54337;
static constexpr auto DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME = 54372;
static constexpr auto DBMS_MIN_REVISION_WITH_VERSION_PATCH = 54401;
static constexpr auto DBMS_MIN_REVISION_WITH_SERVER_LOGS = 54406;
/// Minimum revision with exactly the same set of aggregation methods and rules to select them.
/// Two-level (bucketed) aggregation is incompatible if servers are inconsistent in these rules
/// (keys will be placed in different buckets and result will not be fully aggregated).
static constexpr auto DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD = 54448;
static constexpr auto DBMS_MIN_MAJOR_VERSION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD = 21;
static constexpr auto DBMS_MIN_MINOR_VERSION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD = 4;
static constexpr auto DBMS_MIN_REVISION_WITH_COLUMN_DEFAULTS_METADATA = 54410;

static constexpr auto DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE = 54405;
static constexpr auto DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO = 54420;

/// Minimum revision supporting SettingsBinaryFormat::STRINGS.
static constexpr auto DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS = 54429;
static constexpr auto DBMS_MIN_REVISION_WITH_SCALARS = 54429;

/// Minimum revision supporting OpenTelemetry
static constexpr auto DBMS_MIN_REVISION_WITH_OPENTELEMETRY = 54442;

static constexpr auto DBMS_MIN_REVISION_WITH_AGGREGATE_FUNCTIONS_VERSIONING = 54452;

static constexpr auto DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION = 1;

static constexpr auto DBMS_MIN_SUPPORTED_PARALLEL_REPLICAS_PROTOCOL_VERSION = 3;
static constexpr auto DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_MARK_SEGMENT_SIZE_FIELD = 4;
static constexpr auto DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION = 4;
static constexpr auto DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS = 54453;

static constexpr auto DBMS_MERGE_TREE_PART_INFO_VERSION = 1;

static constexpr auto DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET = 54441;

static constexpr auto DBMS_MIN_REVISION_WITH_X_FORWARDED_FOR_IN_CLIENT_INFO = 54443;
static constexpr auto DBMS_MIN_REVISION_WITH_REFERER_IN_CLIENT_INFO = 54447;

static constexpr auto DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH = 54448;

static constexpr auto DBMS_MIN_PROTOCOL_VERSION_WITH_INCREMENTAL_PROFILE_EVENTS = 54451;

static constexpr auto DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION = 54454;

static constexpr auto DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME = 54449;

static constexpr auto DBMS_MIN_PROTOCOL_VERSION_WITH_PROFILE_EVENTS_IN_INSERT = 54456;

static constexpr auto DBMS_MIN_PROTOCOL_VERSION_WITH_VIEW_IF_PERMITTED = 54457;

static constexpr auto DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM = 54458;

static constexpr auto DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY = 54458;

static constexpr auto DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS = 54459;

/// The server will send query elapsed run time in the Progress packet.
static constexpr auto DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS = 54460;

static constexpr auto DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES = 54461;

static constexpr auto DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2 = 54462;

static constexpr auto DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS = 54463;

static constexpr auto DBMS_MIN_PROTOCOL_VERSION_WITH_TIMEZONE_UPDATES = 54464;

static constexpr auto DBMS_MIN_REVISION_WITH_SPARSE_SERIALIZATION = 54465;

static constexpr auto DBMS_MIN_REVISION_WITH_SSH_AUTHENTICATION = 54466;

/// Send read-only flag for Replicated tables as well
static constexpr auto DBMS_MIN_REVISION_WITH_TABLE_READ_ONLY_CHECK = 54467;

static constexpr auto DBMS_MIN_REVISION_WITH_SYSTEM_KEYWORDS_TABLE = 54468;

static constexpr auto DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION = 54469;

/// Packets size header
static constexpr auto DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS = 54470;

static constexpr auto DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL = 54471;

/// Version of ClickHouse TCP protocol.
///
/// Should be incremented manually on protocol changes.
///
/// NOTE: DBMS_TCP_PROTOCOL_VERSION has nothing common with VERSION_REVISION,
/// later is just a number for server version (one number instead of commit SHA)
/// for simplicity (sometimes it may be more convenient in some use cases).
static constexpr auto DBMS_TCP_PROTOCOL_VERSION = 54471;

}
