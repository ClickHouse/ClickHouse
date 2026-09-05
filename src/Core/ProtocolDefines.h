#pragma once

namespace DB
{

/// In the names below, "REVISION" and "PROTOCOL_VERSION" are synonyms.

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
/// Compare by protocol revision rather than major/minor version: the aggregation method can change
/// within a single release (same major.minor) and only the revision distinguishes a pre-change
/// server from a post-change one.
/// 54489: the method for a single `String` key follows `enable_packed_string_keys_in_aggregation`.
/// A peer below this revision always uses the packed method and does not know the setting, so it
/// cannot follow a query that disables it.
static constexpr auto DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD = 54489;
static constexpr auto DBMS_MIN_REVISION_WITH_COLUMN_DEFAULTS_METADATA = 54410;

static constexpr auto DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE = 54405;
static constexpr auto DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO = 54420;

/// Minimum revision supporting SettingsBinaryFormat::STRINGS.
static constexpr auto DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS = 54429;
static constexpr auto DBMS_MIN_REVISION_WITH_SCALARS = 54429;

/// Minimum revision supporting OpenTelemetry
static constexpr auto DBMS_MIN_REVISION_WITH_OPENTELEMETRY = 54442;

static constexpr auto DBMS_MIN_REVISION_WITH_AGGREGATE_FUNCTIONS_VERSIONING = 54452;

static constexpr auto DBMS_CLUSTER_INITIAL_PROCESSING_PROTOCOL_VERSION = 1;
static constexpr auto DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_DATA_LAKE_METADATA = 2;
static constexpr auto DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_METADATA = 3;
static constexpr auto DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_FILE_BUCKETS_INFO = 4;
static constexpr auto DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_EXCLUDED_ROWS = 5;
static constexpr auto DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_FILE_STATS = 6;
/// Version 7 is reserved for a feature implemented in the private repository (Iceberg compaction).
/// It is kept here, unused, so that the cluster-function protocol version number space stays identical
/// between the open-source and the private repositories and a given number never has two meanings.
static constexpr auto DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_COMPACTION = 7;
static constexpr auto DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_READ_SOURCE_INDEX = 8;
static constexpr auto DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_IDENTITY_PARTITION_COLUMNS = 9;
static constexpr auto DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_CDC_READING = 10;
static constexpr auto DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION = DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_CDC_READING;

static constexpr auto DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION = 1;

static constexpr auto DBMS_MIN_SUPPORTED_PARALLEL_REPLICAS_PROTOCOL_VERSION = 3;
static constexpr auto DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_MARK_SEGMENT_SIZE_FIELD = 4;
static constexpr auto DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_PROJECTION = 5;
static constexpr auto DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_MIN_MARKS_PER_TASK = 6;
static constexpr auto DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_STREAM_ID = 7;
static constexpr auto DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_ANNOUNCEMENT_RESPONSE = 8;
static constexpr auto DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION = 8;
static constexpr auto DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS = 54453;
static constexpr auto DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS = 54475;

static constexpr auto DBMS_MERGE_TREE_PART_INFO_VERSION = 1;

/// Version 2 serializes a bucketed `ReadFromMergeTree` leaf read as just its bucket count; the per-bucket
/// marks travel in the `read_bucket` task parameter. The deserializer rejects a version-1 bucketed step (its
/// trailing part-name payload would desync the plan), so all `make_distributed_plan` nodes need one version.
/// Version 3 adds the parallel-replicas flag (bit 32) on a serialized `ReadFromMergeTree`, telling the
/// replica to rebuild the read in parallel-reading mode. An older replica would ignore the bit and do a
/// full non-parallel read, so the serializer fails closed when this flag is set below version 3.
/// Version 4 adds `WindowStep` to the set of serializable steps. An older worker does not register a
/// "Window" step at all (`QueryPlanStepRegistry::createStep` would throw `UNKNOWN_IDENTIFIER` on it), so
/// the serializer fails closed instead when talking to a peer below version 4.
/// Version 5 registers the `enable_packed_string_keys_in_aggregation` plan setting. A peer at this version
/// receives it on every aggregation step whenever the legacy method is requested. An older peer rejects the
/// unknown name (`QueryPlanSerializationSettings::readBinary` throws), so towards such a peer the name is
/// written only when omitting it could corrupt two-level distributed merging - failing closed on an explicit
/// error instead of silently mixing the two hash methods, whose two-level bucket numbering differs.
/// Version 6 lets a `PARTITION BY` window's feeding sort be shipped under `make_distributed_plan`:
/// `SortingStep` now serializes a non-empty `partition_by_description` plus a trailing flags byte that
/// carries a `FinishSorting` conversion, and `GatherSendStep` now serializes `maintain_sort_description`
/// and merge-sorts its input streams instead of an unordered `resize(1)` when set. Both steps check the
/// version in their serialize and deserialize, since an older peer would misparse the stream, not merely
/// reject an unknown step name as with version 4.
/// Version 7 registers the `enable_adaptive_aggregator`, `adaptive_aggregator_freeze_threshold` and
/// `adaptive_aggregator_freeze_threshold_bytes` plan settings. As with version 5, an older peer rejects the
/// unknown names, so they are written only towards a peer at this version or above; a peer below it has no
/// adaptive aggregation to drive anyway.
/// Version 8 adds the distributed-plan payloads: the bounded-sort limit on `SortingStep`, the
/// narrowing flag on `UnionStep`, the bucketed-read task parameter name on `ReadFromMergeTree`,
/// and the in-order aggregation payload on `AggregatingStep`. Only the sort limit has a
/// per-field version gate; the rest rely on the whole stream being rejected by its leading version.
/// Version 9 registers the `Rollup` and `Cube` steps, so a plan with `GROUP BY ... WITH ROLLUP`
/// or `WITH CUBE` can be shipped under `make_distributed_plan`.
/// Version 10 adds the part storage-type tag (with the blob-list manifest payload) to the worker read
/// step, in the slot of the former `is_packed` flag. The values 0/1 are wire-compatible with the flag;
/// only the new tag 2 requires this version, and the serializer refuses to emit it towards older peers.
/// Version 11 serializes the plan-level `max_threads` and `concurrency_control` fields. They are not
/// properties of individual steps, so a remote plan fragment would otherwise execute with its default
/// execution limits after deserialization.
/// Version 12 adds the ReadInOrder info in the reading step in the plan
/// Version 13 adds the `only_merge` flag (bit 128) on `AggregatingStep`, set on the merge step
/// synthesized by the Cascades aggregation-pushdown transformation. Both sides gate the flag on
/// the version, so a mixed-version cluster fails at plan time instead of at runtime.
/// Version 14 registers the `IntersectOrExcept` step, so a plan with `INTERSECT` or `EXCEPT`
/// can be shipped under `make_distributed_plan`.
static constexpr auto DBMS_QUERY_PLAN_SERIALIZATION_VERSION = 14;
/// The parallel-replicas remote plan is serialized once (at DBMS_QUERY_PLAN_SERIALIZATION_VERSION) and
/// that one blob is reused for every replica, so a replica below this version must be excluded up front
/// rather than sent a blob it cannot parse. Tied to DBMS_QUERY_PLAN_SERIALIZATION_VERSION itself so a
/// future bump can't silently leave this gate behind.
static constexpr auto DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_PARALLEL_REPLICAS = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
/// First query-plan serialization version that registers a "Window" step. Used to gate serializing a
/// `WindowStep` for `make_distributed_plan`.
static constexpr auto DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_WINDOW_STEP = 4;
/// First query-plan serialization version that knows the `enable_packed_string_keys_in_aggregation`
/// plan setting name. Gates writing it in `AggregatingStep::serializeSettings` /
/// `MergingAggregatedStep::serializeSettings`.
static constexpr auto DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_PACKED_STRING_KEYS_SETTING = 5;
/// First query-plan serialization version that knows the `enable_adaptive_aggregator` and
/// `adaptive_aggregator_freeze_threshold` plan setting names. Gates writing them in
/// `AggregatingStep::serializeSettings`.
static constexpr auto DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ADAPTIVE_AGGREGATOR = 7;
static constexpr auto DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_BLOBS_LIST_PARTS = 10;
/// First query-plan serialization version that preserves plan-level `max_threads` and `concurrency_control`.
static constexpr auto DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_EXECUTION_LIMITS = 11;
/// First query-plan serialization version that carries the ReadInOrder info
static constexpr auto DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_READ_IN_ORDER = 12;
/// First query-plan serialization version with the `only_merge` flag (bit 128) on `AggregatingStep`,
/// set on the merge step synthesized by the Cascades aggregation pushdown. Gated on both sides so a
/// mixed-version cluster fails at plan time.
static constexpr auto DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ONLY_MERGE_AGGREGATION = 13;
/// Version 1 added the initiator's settings changes to the task.
/// Version 2 added per-stream streaming-exchange ports to exchange_stream_sources.
static constexpr auto DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION = 2;

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

/// Upper bound on the number of password-complexity rules carried in the server
/// Hello packet. Real-world configurations declare a handful of rules at most.
/// The server rejects configurations that exceed this when constructing its Hello
/// so the operator sees the limit at its own boundary; the client enforces the
/// same bound on receive so a hostile server cannot force a huge `reserve`.
static constexpr auto DBMS_MAX_PASSWORD_COMPLEXITY_RULES = 256;

/// Upper bound on every server-supplied display string carried in the Hello packet
/// (server name, time zone, display name, password-rule patterns and messages) and
/// in post-handshake updates of the same fields. Legitimate values are short; a
/// high cap is purely an attack surface. Enforced symmetrically: the server refuses
/// to construct an oversized Hello (pointing at the misconfigured field), the
/// client refuses to read one (defending against a hostile server).
static constexpr auto DBMS_MAX_HELLO_STRING_SIZE = 4096;

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

/// Push externally granted roles to other nodes
static constexpr auto DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES = 54472;

static constexpr auto DBMS_MIN_REVISION_WITH_V2_DYNAMIC_AND_JSON_SERIALIZATION = 54473;

static constexpr auto DBMS_MIN_REVISION_WITH_SERVER_SETTINGS = 54474;

static constexpr auto DBMS_MIN_REVISON_WITH_JWT_IN_INTERSERVER = 54476;

static constexpr auto DBMS_MIN_REVISION_WITH_QUERY_PLAN_SERIALIZATION = 54477;

static constexpr auto DBMS_MIN_REVISON_WITH_PARALLEL_BLOCK_MARSHALLING = 54478;

static constexpr auto DBMS_MIN_REVISION_WITH_VERSIONED_CLUSTER_FUNCTION_PROTOCOL = 54479;

static constexpr auto DBMS_MIN_REVISION_WITH_OUT_OF_ORDER_BUCKETS_IN_AGGREGATION = 54480;
static constexpr auto DBMS_MIN_REVISION_WITH_COMPRESSED_LOGS_PROFILE_EVENTS_COLUMNS = 54481;

static constexpr auto DBMS_MIN_REVISION_WITH_REPLICATED_SERIALIZATION = 54482;

static constexpr auto DBMS_MIN_REVISION_WITH_NULLABLE_SPARSE_SERIALIZATION = 54483;

static constexpr auto DBMS_MIN_PROTOCOL_VERSION_WITH_PROGRESS_IN_ASYNC_INSERT = 54484;

static constexpr auto DBMS_MIN_REVISION_WITH_CLIENT_AGENT_IN_CLIENT_INFO = 54485;

static constexpr auto DBMS_MIN_PROTOCOL_VERSION_WITH_INTERNAL_QUERY_FLAG = 54486;

/// Authenticate interserver `TablesStatusRequest` with a cluster-secret hash
/// (sent right after the request, validated before the response).
static constexpr auto DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_TABLES_STATUS = 54487;

/// Push the initiator's current roles to other nodes for consistent role-scoped access.
static constexpr auto DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_CURRENT_ROLES = 54488;

static constexpr auto DBMS_MIN_REVISION_WITH_HTTP_HANDLER_IN_CLIENT_INFO = 54490;

/// Serialize the skip degree of a `quantileDeterministic` state, so that merging states thinned out
/// to different degrees does not depend on how the rows were distributed between them.
static constexpr auto DBMS_MIN_REVISION_WITH_QUANTILE_DETERMINISTIC_SKIP_DEGREE = 54491;

/// Send String columns in the native protocol with a separate stream of cumulative byte offsets.
static constexpr auto DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION = 54492;


/// Version of ClickHouse TCP protocol.
///
/// Should be incremented manually on protocol changes.
///
/// NOTE: DBMS_TCP_PROTOCOL_VERSION has nothing common with VERSION_REVISION,
/// later is just a number for server version (one number instead of commit SHA)
/// for simplicity (sometimes it may be more convenient in some use cases).
static constexpr auto DBMS_TCP_PROTOCOL_VERSION = 54492;
}
