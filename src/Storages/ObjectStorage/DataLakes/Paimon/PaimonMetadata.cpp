#include "config.h"

#if USE_AVRO

#include <cstddef>
#include <filesystem>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Core/NamesAndTypes.h>
#include <Core/Settings.h>
#include <Disks/IStoragePolicy.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonClient.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PartitionPruner.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Utils.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeTableStateSnapshot.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <base/defines.h>
#include <base/MemorySanitizer.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/Macros.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <fmt/format.h>


namespace DB
{

using namespace Paimon;

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INVALID_STATE;
extern const int LOGICAL_ERROR;
extern const int NO_ZOOKEEPER;
extern const int REPLICA_IS_ALREADY_ACTIVE;
}

namespace Setting
{
extern const SettingsBool use_paimon_partition_pruning;
extern const SettingsBool use_paimon_metadata_files_cache;
extern const SettingsInt64 paimon_target_snapshot_id;
extern const SettingsUInt64 max_consume_snapshots;
}

namespace FailPoints
{
extern const char paimon_incremental_read_pause_before_processing_lock[];
extern const char paimon_incremental_read_pause_before_watermark_commit[];
extern const char paimon_incremental_read_pause_after_watermark_commit[];
}

namespace DataLakeStorageSetting
{
extern const DataLakeStorageSettingsBool paimon_incremental_read;
extern const DataLakeStorageSettingsInt64 paimon_metadata_refresh_interval_sec;
extern const DataLakeStorageSettingsString paimon_keeper_path;
extern const DataLakeStorageSettingsString paimon_replica_name;
}

namespace
{
/// Build a cache-key prefix that uniquely identifies a Paimon table instance.
///
/// Paimon's own table UUID defaults to the table name, so it cannot
/// distinguish two tables that once shared the same path (e.g. after
/// DROP + re-CREATE).  Instead we hash three orthogonal components:
///   - data-source description  (host:port/bucket for S3, URL/container
///     for Azure, full URL for HDFS — isolates different storage backends)
///   - table path               (isolates different tables on the same backend)
///   - schema-0 timeMillis      (isolates DROP-then-recreate of the same path,
///     because every CREATE writes a fresh schema-0 with a new timestamp)
String buildPaimonCacheKeyPrefix(
    const StorageObjectStorageConfigurationPtr & configuration,
    const String & table_name,
    Int64 schema0_time_millis)
{
    const String link_identity = configuration->getDataSourceDescription();
    /// Feed each component into SipHash with its length prefix to avoid
    /// ambiguity when a component itself contains the delimiter character.
    /// For example, link="a|b" + table="c" must differ from link="a" + table="b|c".
    SipHash hash;
    hash.update(link_identity.size());
    hash.update(link_identity);
    hash.update(table_name.size());
    hash.update(table_name);
    hash.update(schema0_time_millis);
    return fmt::format("{:016x}", hash.get64());
}
}

DataLakeMetadataPtr PaimonMetadata::create(
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfigurationWeakPtr & configuration,
    const ContextPtr & local_context)
{
    auto configuration_ptr = configuration.lock();
    if (!configuration_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration expired");

    auto log = getLogger("PaimonMetadata");
    LOG_TEST(
        log,
        "path: {} raw path: {}",
        configuration_ptr->getPathForRead().path,
        configuration_ptr->getRawPath().path);

    const String table_path = configuration_ptr->getPathForRead().path;

    auto global_context = local_context->getGlobalContext();

    /// Create table client
    PaimonTableClientPtr table_client = std::make_shared<PaimonTableClient>(object_storage, table_path, global_context);

    /// Always latch schema-0 timeMillis before loading any schema from the table.
    /// This anchors the whole initialization to one table identity: if the upstream
    /// table is `DROP` + `CREATE`d while `create` is reading metadata, the post-load
    /// `validateTableIdentity` in the constructor will fail before publishing state.
    auto schema0_info = table_client->getTableSchemaInfoById(0);
    auto schema0_json = table_client->getTableSchemaJSON(schema0_info);
    Int64 schema0_time_millis = 0;
    Paimon::getValueFromJSON(schema0_time_millis, schema0_json, "timeMillis");

    /// Get and validate schema
    auto schema_info = table_client->getLatestTableSchemaInfo();
    auto schema_json = table_client->getTableSchemaJSON(schema_info);

    Int32 version = -1;
    Paimon::getValueFromJSON(version, schema_json, "version");
    if (version != 3)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Paimon table schema version {} is unsupported.", version);
    }

    /// Create schema processor and add initial schema
    auto schema_processor = std::make_shared<PaimonSchemaProcessor>();
    auto schema = schema_processor->addSchema(schema_json);

    /// Get partition default name from options
    String partition_default_name = PARTITION_DEFAULT_VALUE;
    auto it = schema->options.find(PAIMON_DEFAULT_PARTITION_NAME);
    if (it != schema->options.end())
        partition_default_name = it->second;

    /// Check if incremental read is enabled
    const auto & data_lake_settings = configuration_ptr->getDataLakeSettings();
    bool incremental_read_enabled = data_lake_settings[DataLakeStorageSetting::paimon_incremental_read].value;
    Int64 metadata_refresh_interval_sec = data_lake_settings[DataLakeStorageSetting::paimon_metadata_refresh_interval_sec].value;

    /// The settings framework accesses field values through a pointer-to-member
    /// dereference (impl.get()->*t). MSan cannot track initialization across this
    /// indirection, so values read via operator[] appear tainted. Unpoison at the
    /// source to prevent taint from propagating through PaimonPersistentComponents
    /// into PaimonMetadata members used on background threads.
    __msan_unpoison(&incremental_read_enabled, sizeof(incremental_read_enabled));
    __msan_unpoison(&metadata_refresh_interval_sec, sizeof(metadata_refresh_interval_sec));

    PaimonStreamStatePtr stream_state = nullptr;

    if (incremental_read_enabled)
    {
        if (!local_context->hasZooKeeper())
            throw Exception(ErrorCodes::NO_ZOOKEEPER, "Incremental read requires Keeper but ZooKeeper is not configured");

        String keeper_path = data_lake_settings[DataLakeStorageSetting::paimon_keeper_path].value;
        String replica_name = data_lake_settings[DataLakeStorageSetting::paimon_replica_name].value;
        if (keeper_path.empty() || replica_name.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "To use Paimon incremental read both paimon_keeper_path and paimon_replica_name must be specified");

        auto keeper = local_context->getZooKeeper();
        auto stream_log = getLogger("PaimonStreamState");
        stream_state = std::make_shared<PaimonStreamState>(keeper, keeper_path, replica_name, stream_log);
        stream_state->initializeKeeperNodes();
        if (!stream_state->activate())
            LOG_WARNING(stream_log, "Replica {} not activated for Paimon incremental read (maybe already active elsewhere)", replica_name);
    }

    /// Only the session-level setting use_paimon_metadata_files_cache is latched here at
    /// metadata construction time: PaimonPersistentComponents is immutable and shared across
    /// queries, so whether this table is bound to the global cache is decided once.  This is
    /// the same pattern used by IcebergMetadata (and all DataLake metadata that returns
    /// supportsUpdate() == true).  To change the session-level decision for an existing table,
    /// the user must DROP + re-CREATE the table with the desired setting.
    ///
    /// The server-level capacity (paimon_metadata_files_cache_size) is intentionally NOT
    /// latched: it is a runtime setting that can be changed via SYSTEM RELOAD CONFIG, so the
    /// pointer to the global cache is always kept and the actual usability is evaluated
    /// dynamically at read time via isMetadataCacheActive().  This way raising/lowering the
    /// size takes effect immediately even for already-created tables.
    PaimonMetadataFilesCachePtr cache_ptr = nullptr;
    if (local_context->getSettingsRef()[Setting::use_paimon_metadata_files_cache])
        cache_ptr = local_context->getPaimonMetadataFilesCache();
    else
        LOG_TRACE(
            log,
            "Not using in-memory cache for paimon metadata files, because the setting use_paimon_metadata_files_cache is false.");

    String table_cache_key_prefix;
    if (cache_ptr)
    {
        const String table_name = configuration_ptr->getRawPath().path.empty()
            ? table_path
            : configuration_ptr->getRawPath().path;
        table_cache_key_prefix = buildPaimonCacheKeyPrefix(configuration_ptr, table_name, schema0_time_millis);
    }

    /// Create persistent components
    PaimonPersistentComponents persistent_components(
        schema_processor,
        cache_ptr,
        stream_state,
        configuration_ptr->getPathForRead().path,
        table_path,
        table_cache_key_prefix,
        partition_default_name,
        incremental_read_enabled,
        metadata_refresh_interval_sec,
        schema0_time_millis);

    return std::make_unique<PaimonMetadata>(
        object_storage, configuration_ptr, global_context, std::move(persistent_components), table_client);
}

PaimonMetadata::PaimonMetadata(
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr /*configuration_*/,
    const DB::ContextPtr & context_,
    Paimon::PaimonPersistentComponents persistent_components_,
    PaimonTableClientPtr table_client_)
    : WithContext(context_)
    , persistent_components(std::move(persistent_components_))
    , table_client(std::move(table_client_))
    , object_storage(std::move(object_storage_))
    , log(getLogger("PaimonMetadata"))
    , refresh_in_progress(false)
{
    /// The settings framework reads field values through a pointer-to-member
    /// dereference across an inheritance chain (impl.get()->*t). MSan cannot
    /// track initialization through this pattern, tainting all downstream values.
    /// Even though the source value is already unpoisoned in create(), compiler
    /// optimizations (register reuse, cmov) may re-introduce shadow taint through
    /// intermediate computations. Unpoison the member directly to guarantee the
    /// background thread always reads a clean value.
    refresh_interval_sec = persistent_components.metadata_refresh_interval_sec > 0
        ? static_cast<size_t>(persistent_components.metadata_refresh_interval_sec) : 0;
    __msan_unpoison(&refresh_interval_sec, sizeof(refresh_interval_sec));

    /// Load initial state
    auto initial_state = loadLatestState();
    validateTableIdentity();
    if (initial_state)
    {
        std::atomic_store_explicit(&current_state, initial_state, std::memory_order_release);
        LOG_TRACE(log, "PaimonMetadata initialized with snapshot_id={}, schema_id={}",
                  initial_state->snapshot_id, initial_state->schema_id);
    }
    else
    {
        LOG_WARNING(log, "PaimonMetadata initialized without snapshots (no snapshot files found yet)");
    }

    /// Validate configuration
    checkSupportedConfiguration();

    /// Schedule background refresh if enabled
    scheduleBackgroundRefresh();
}

void PaimonMetadata::checkSupportedConfiguration() const
{
    auto state = getCurrentState();
    if (!state)
        return;

    auto options = persistent_components.schema_processor->getOptions(state->schema_id);
    auto it = options.find(PAIMON_SCAN_MODE);
    if (it != options.end())
    {
        const String & mode = it->second;
        if (mode != "latest" && mode != "latest-full" && mode != "default")
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Paimon scan mode '{}' is unsupported.", mode);
        }
    }
}

PaimonTableStatePtr PaimonMetadata::getCurrentState() const
{
    return std::atomic_load_explicit(&current_state, std::memory_order_acquire);
}

PaimonTableStatePtr PaimonMetadata::loadLatestState() const
{
    /// Get latest snapshot info
    auto snapshot_info_opt = table_client->getLatestTableSnapshotInfo();
    if (!snapshot_info_opt)
    {
        LOG_WARNING(log, "Paimon table has no snapshots yet");
        return nullptr;
    }

    auto snapshot = table_client->getSnapshot(*snapshot_info_opt);

    /// Ensure schema for this snapshot is cached in processor (use schema_id, not "latest")
    if (!persistent_components.schema_processor->hasSchema(snapshot.schema_id))
    {
        auto schema_info = table_client->getTableSchemaInfoById(static_cast<Int32>(snapshot.schema_id));
        auto schema_json = table_client->getTableSchemaJSON(schema_info);
        persistent_components.schema_processor->addSchema(schema_json);
    }

    /// Register snapshot-schema relationship
    persistent_components.schema_processor->registerSnapshotSchema(snapshot.id, snapshot.schema_id);

    return std::make_shared<PaimonTableState>(
        snapshot.id,
        snapshot.schema_id,
        snapshot.base_manifest_list,
        snapshot.delta_manifest_list,
        snapshot.commit_kind,
        snapshot.time_millis,
        snapshot.total_record_count,
        snapshot.delta_record_count,
        snapshot.changelog_record_count,
        snapshot.watermark);
}

void PaimonMetadata::validateTableIdentity() const
{
    /// Detect external table recreation by checking schema-0 timeMillis.
    /// This guard is intentionally NOT tied to isMetadataCacheActive(): the schema_processor
    /// caches schemas by id regardless of the metadata files cache capacity, so a stale schema
    /// could otherwise be silently reused after an external DROP + re-CREATE at the same path
    /// when paimon_metadata_files_cache_size is reloaded to 0.  Run it whenever the table
    /// identity was latched at create time.
    if (persistent_components.schema0_time_millis == 0)
        return;

    auto schema0_info = table_client->getTableSchemaInfoById(0);
    auto schema0_json = table_client->getTableSchemaJSON(schema0_info);
    Int64 current_schema0_time_millis = 0;
    Paimon::getValueFromJSON(current_schema0_time_millis, schema0_json, "timeMillis");

    /// Not LOGICAL_ERROR: this is a disagreement with external state, not a broken
    /// internal invariant, and LOGICAL_ERROR is treated as a bug by stress/fuzzer runs.
    if (current_schema0_time_millis != persistent_components.schema0_time_millis)
        throw Exception(
            ErrorCodes::INVALID_STATE,
            "Underlying Paimon table was recreated (schema-0 timeMillis changed: {} -> {}). "
            "Please DROP and re-CREATE the ClickHouse external table to pick up the new table identity.",
            persistent_components.schema0_time_millis,
            current_schema0_time_millis);
}

void PaimonMetadata::update(const ContextPtr & /*local_context*/)
{
    /// NOTE: This method only refreshes the snapshot state.  It does NOT re-evaluate
    /// use_paimon_metadata_files_cache because cache_ptr lives in the immutable
    /// PaimonPersistentComponents (same design as IcebergMetadata::update).

    /// Validate table identity after loading the snapshot, so a DROP + re-CREATE
    /// during loadLatestState() cannot publish a new table instance while reusing
    /// schemas cached by schema_id.

    /// 1. Load new state outside any lock (I/O operations)
    auto new_state = loadLatestState();
    validateTableIdentity();
    if (!new_state)
    {
        LOG_WARNING(log, "Paimon table has no snapshots yet, skip update");
        return;
    }

    /// 2. Quick check if update is needed
    auto old_state = getCurrentState();
    if (old_state && *old_state == *new_state)
    {
        LOG_TRACE(log, "Paimon table state unchanged, snapshot_id={}", new_state->snapshot_id);
        return;
    }

    /// 3. Atomically replace state (very short critical section)
    {
        std::lock_guard lock(update_mutex);
        std::atomic_store_explicit(&current_state, new_state, std::memory_order_release);
    }

    LOG_DEBUG(
        log,
        "Paimon table state updated: snapshot_id {} -> {}",
        old_state ? old_state->snapshot_id : -1,
        new_state->snapshot_id);
}

NamesAndTypesList PaimonMetadata::getTableSchema(ContextPtr /*local_context*/) const
{
    auto state = getCurrentState();
    if (!state)
        return {};

    auto schema = persistent_components.schema_processor->getClickHouseSchema(state->schema_id);
    return schema ? *schema : NamesAndTypesList{};
}

std::optional<DataLakeTableStateSnapshot> PaimonMetadata::getTableStateSnapshot(ContextPtr /*local_context*/) const
{
    auto state = getCurrentState();
    if (!state)
        return std::nullopt;

    return DataLakeTableStateSnapshot{*state};
}

std::unique_ptr<StorageInMemoryMetadata> PaimonMetadata::buildStorageMetadataFromState(
    const DataLakeTableStateSnapshot & state, ContextPtr /*local_context*/) const
{
    chassert(std::holds_alternative<Paimon::TableStateSnapshot>(state));

    const auto * paimon_state = std::get_if<Paimon::TableStateSnapshot>(&state);
    if (!paimon_state)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Paimon::TableStateSnapshot in DataLakeTableStateSnapshot");

    if (!persistent_components.schema_processor->hasSchema(paimon_state->schema_id))
    {
        auto schema_info = table_client->getTableSchemaInfoById(static_cast<Int32>(paimon_state->schema_id));
        auto schema_json = table_client->getTableSchemaJSON(schema_info);
        persistent_components.schema_processor->addSchema(schema_json);
    }

    auto columns = persistent_components.schema_processor->getClickHouseSchema(paimon_state->schema_id);
    if (!columns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to get ClickHouse schema for schema_id={}", paimon_state->schema_id);

    auto result = std::make_unique<StorageInMemoryMetadata>();
    result->setColumns(ColumnsDescription{*columns});
    result->setDataLakeTableState(state);
    return result;
}

bool PaimonMetadata::shouldReloadSchemaForConsistency(ContextPtr /*local_context*/) const
{
    return true;
}

bool PaimonMetadata::operator==(const IDataLakeMetadata & other) const
{
    const auto * paimon_other = dynamic_cast<const PaimonMetadata *>(&other);
    if (!paimon_other)
        return false;

    auto this_state = getCurrentState();
    auto other_state = paimon_other->getCurrentState();

    if (!this_state && !other_state)
        return true;
    if (!this_state || !other_state)
        return false;

    return *this_state == *other_state;
}

PaimonTableStatePtr PaimonMetadata::extractTableState(StorageMetadataPtr storage_metadata)
{
    if (!storage_metadata || !storage_metadata->datalake_table_state.has_value())
        return nullptr;

    const auto * paimon_state = std::get_if<Paimon::TableStateSnapshot>(&*storage_metadata->datalake_table_state);
    if (!paimon_state)
        return nullptr;

    return std::make_shared<PaimonTableState>(*paimon_state);
}

ManifestListConstPtr PaimonMetadata::getManifestList(const String & manifest_list_path) const
{
    if (manifest_list_path.empty())
        return std::make_shared<const std::vector<PaimonManifestFileMeta>>();

    if (persistent_components.isMetadataCacheActive())
    {
        String cache_key = PaimonMetadataFilesCache::makeKey(persistent_components.table_cache_key_prefix, manifest_list_path);
        auto log_ptr = log;
        auto client = table_client;
        auto load_manifest_list = [log_ptr, client, manifest_list_path]()
        {
            LOG_TRACE(log_ptr, "Loading manifest list (cache miss): {}", manifest_list_path);
            return client->getManifestMeta(manifest_list_path, /*disable_filesystem_cache=*/true);
        };
        return persistent_components.metadata_cache->getOrSetManifestList(cache_key, load_manifest_list);
    }

    LOG_TRACE(log, "Loading manifest list (no cache): {}", manifest_list_path);
    auto [manifest_list, _] = table_client->getManifestMeta(manifest_list_path);
    return std::make_shared<const std::vector<PaimonManifestFileMeta>>(std::move(manifest_list));
}

ManifestConstPtr PaimonMetadata::getManifest(const String & manifest_path, Int64 schema_id) const
{
    auto schema = persistent_components.schema_processor->getSchemaById(schema_id);
    if (!schema)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Schema with id {} not found", schema_id);

    if (persistent_components.isMetadataCacheActive())
    {
        String cache_key = PaimonMetadataFilesCache::makeKey(persistent_components.table_cache_key_prefix, manifest_path + ":" + toString(schema_id));
        auto log_ptr = log;
        auto client = table_client;
        auto partition_default_name = persistent_components.partition_default_name;
        auto load_manifest = [log_ptr, client, manifest_path, schema, partition_default_name]()
        {
            LOG_TRACE(log_ptr, "Loading manifest (cache miss): {}", manifest_path);
            return client->getDataManifest(manifest_path, *schema, partition_default_name, /*disable_filesystem_cache=*/true);
        };
        return persistent_components.metadata_cache->getOrSetManifest(cache_key, load_manifest);
    }

    LOG_TRACE(log, "Loading manifest (no cache): {}", manifest_path);
    return std::make_shared<const PaimonManifest>(
        table_client->getDataManifest(manifest_path, *schema, persistent_components.partition_default_name));
}

ObjectIterator PaimonMetadata::iterate(
    const ActionsDAG * filter_dag,
    FileProgressCallback callback,
    size_t /* list_batch_size */,
    StorageMetadataPtr storage_metadata,
    ContextPtr query_context) const
{
    /// 1. Try to extract state from storage_metadata for snapshot isolation
    auto state = extractTableState(storage_metadata);
    if (!state)
    {
        /// fallback to current, then try lazy load once
        state = getCurrentState();
        if (!state)
        {
            state = loadLatestState();
            /// Publishing before the consumption-point validateTableIdentity() below is safe: that
            /// check re-validates identity before any data is served, so a state from a recreated
            /// table can only fail-close, never cause a silent stale read.
            if (state)
                std::atomic_store_explicit(&current_state, state, std::memory_order_release);
        }
    }

    if (!state)
        return createKeysIterator({}, object_storage, callback); /// still no snapshot: return empty

    /// Detect external table recreation before consuming the pinned/current/loaded state.
    /// The normal path pins datalake_table_state during analysis (updateExternalDynamicMetadataIfExists)
    /// and reaches here via extractTableState, so this is the only place that closes the
    /// analysis->execution window where an external DROP + re-CREATE would otherwise be reused stale.
    /// iterate() runs once per query read (the iterator is shared across streams), so this single
    /// remote schema-0 check is negligible relative to the data scan it guards (fail-close).
    validateTableIdentity();

    /// 2. Get schema from processor (cached)
    auto schema = persistent_components.schema_processor->getSchemaById(state->schema_id);
    if (!schema)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Schema with id {} not found", state->schema_id);

    /// 3. Build partition pruner if needed
    std::optional<PartitionPruner> partition_pruner;
    if (filter_dag && query_context->getSettingsRef()[Setting::use_paimon_partition_pruning])
    {
        auto filter_dag_copy = filter_dag->clone();
        partition_pruner.emplace(*schema, filter_dag_copy, getContext());
    }

    /// 4. Collect data files based on read mode
    Strings data_files;

    /// 4.a Query-level targeted snapshot (only when incremental is enabled)
    const Int64 target_snapshot_id = query_context->getSettingsRef()[Setting::paimon_target_snapshot_id];
    if (persistent_components.incremental_read_enabled && target_snapshot_id > 0)
    {
        auto target_state = loadStateForSnapshot(target_snapshot_id);
        if (target_state->isCompact())
            LOG_WARNING(log, "Target snapshot_id={} is a COMPACT snapshot. "
                "Its delta manifest contains compaction output (not incremental changes). "
                "Consider using a non-COMPACT snapshot for accurate incremental semantics.",
                target_snapshot_id);
        data_files = collectDeltaFilesForSnapshot(target_state, partition_pruner);
    }
    /// 4.b Regular incremental mode
    else if (isIncrementalReadEnabled())
    {
        auto stream_state = persistent_components.stream_state;
        if (stream_state->needsNewKeeper())
        {
            auto keeper = getContext()->getZooKeeper();
            stream_state->setKeeper(keeper);
            stream_state->initializeKeeperNodes();
            if (!stream_state->activate())
                throw Exception(
                    ErrorCodes::REPLICA_IS_ALREADY_ACTIVE,
                    "Failed to activate Paimon replica after Keeper reconnection. "
                    "Another server may be using the same replica_name.");
        }

        /// The snapshot this read is pinned to was resolved during analysis; the watermark
        /// below is read now. Logging the pinned id makes that gap visible when a read
        /// reports no new data that a concurrent consumer had already taken.
        LOG_TRACE(log, "Paimon incremental read of '{}' pinned snapshot_id={}, acquiring the processing lock",
            persistent_components.table_path, state->snapshot_id);

        /// Test-only pause in exactly that gap, so tests can let another consumer finish a
        /// whole poll here and pin down that finding the watermark ahead of the pinned state
        /// is not by itself reported as a rewound warehouse. Pauses once, so the competing
        /// poll is not blocked by it too.
        FailPointInjection::pauseFailPoint(
            FailPoints::paimon_incremental_read_pause_before_processing_lock);

        /// The handle owns the session it acquired the lock with and releases the lock
        /// on destruction, so every exit path below is fenced and leaves no orphan lock.
        PaimonProcessingLockPtr processing_lock = stream_state->acquireProcessingLock();

        std::optional<Int64> last_consumed_snapshot_id;
        const UInt64 max_consume_snapshots = query_context->getSettingsRef()[Setting::max_consume_snapshots];
        data_files = collectIncrementalDataFiles(state, partition_pruner, max_consume_snapshots, last_consumed_snapshot_id);

        if (last_consumed_snapshot_id)
        {
            /// Test-only pause before the watermark is advanced: the batch is collected but
            /// nothing is committed yet. Tests use this window to take the processing lock
            /// away and pin down that the commit below then fails instead of overwriting
            /// another consumer's progress.
            FailPointInjection::pauseFailPoint(
                FailPoints::paimon_incremental_read_pause_before_watermark_commit);

            stream_state->setCommittedSnapshot(*processing_lock, *last_consumed_snapshot_id);
            /// Test-only pause inside the at-most-once window: the watermark is
            /// committed, the collected batch has not been delivered yet. A crash
            /// here loses the batch; the failpoint lets tests pin that semantics
            /// deterministically.
            FailPointInjection::pauseFailPoint(
                FailPoints::paimon_incremental_read_pause_after_watermark_commit);
        }
    }
    else
    {
        data_files = collectFullScanDataFiles(state, partition_pruner);
    }

    LOG_DEBUG(log, "Collected {} data files for snapshot_id={} (incremental={})",
              data_files.size(), state->snapshot_id, isIncrementalReadEnabled());

    return createKeysIterator(std::move(data_files), object_storage, callback);
}

bool PaimonMetadata::isIncrementalReadEnabled() const
{
    return persistent_components.hasStreamState();
}

std::optional<Int64> PaimonMetadata::getCommittedSnapshotId() const
{
    if (!persistent_components.hasStreamState())
        return std::nullopt;
    return persistent_components.stream_state->getCommittedSnapshotId();
}

void PaimonMetadata::scheduleBackgroundRefresh()
{
    if (refresh_interval_sec == 0)
        return;

    auto schedule_pool = getContext()->getSchedulePool();
    refresh_task = schedule_pool->createTask(
        StorageID::createEmpty(), "PaimonMetadataRefresh/" + persistent_components.table_path,
        [this]()
        {
            runBackgroundRefresh();
        });
    refresh_task->scheduleAfter(refresh_interval_sec * 1000);
}

void PaimonMetadata::runBackgroundRefresh()
{
    if (!refresh_task)
        return;

    /// Prevent overlapping runs
    bool expected = false;
    if (!refresh_in_progress.compare_exchange_strong(expected, true))
    {
        refresh_task->scheduleAfter(refresh_interval_sec * 1000);
        return;
    }

    try
    {
        update(getContext());
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Paimon background refresh failed");
    }

    refresh_in_progress.store(false);
    refresh_task->scheduleAfter(refresh_interval_sec * 1000);
}

PaimonTableStatePtr PaimonMetadata::loadStateForSnapshot(Int64 snapshot_id) const
{
    /// Get snapshot by ID
    /// Build snapshot file path: `table_location/snapshot/snapshot-<id>`
    const String snapshot_path = (std::filesystem::path(persistent_components.table_location)
        / PAIMON_SNAPSHOT_DIR
        / fmt::format("{}{}", PAIMON_SNAPSHOT_PREFIX, snapshot_id));
    auto snapshot = table_client->getSnapshot({snapshot_id, snapshot_path});

    /// Ensure schema is cached for this snapshot_id
    if (!persistent_components.schema_processor->hasSchema(snapshot.schema_id))
    {
        auto schema_info = table_client->getTableSchemaInfoById(static_cast<Int32>(snapshot.schema_id));
        auto schema_json = table_client->getTableSchemaJSON(schema_info);
        persistent_components.schema_processor->addSchema(schema_json);
    }

    return std::make_shared<PaimonTableState>(
        snapshot.id,
        snapshot.schema_id,
        snapshot.base_manifest_list,
        snapshot.delta_manifest_list,
        snapshot.commit_kind,
        snapshot.time_millis,
        snapshot.total_record_count,
        snapshot.delta_record_count,
        snapshot.changelog_record_count,
        snapshot.watermark);
}

std::vector<PaimonTableStatePtr> PaimonMetadata::getSnapshotsBetween(
    Int64 from_snapshot_id,
    Int64 to_snapshot_id,
    UInt64 max_snapshots_to_load,
    bool skip_compact,
    std::optional<Int64> & last_scanned_snapshot_id) const
{
    std::vector<PaimonTableStatePtr> snapshots;
    last_scanned_snapshot_id.reset();

    if (to_snapshot_id <= from_snapshot_id)
        return snapshots;

    size_t snapshots_to_reserve = static_cast<size_t>(to_snapshot_id - from_snapshot_id);
    if (max_snapshots_to_load > 0 && snapshots_to_reserve > max_snapshots_to_load)
        snapshots_to_reserve = static_cast<size_t>(max_snapshots_to_load);
    snapshots.reserve(snapshots_to_reserve);

    /// Paimon only ever deletes snapshots from the ends of the id range - expiration removes a
    /// prefix (ExpireSnapshotsImpl always expires starting at `earliest`), rollback removes a
    /// suffix - so the surviving ids are always contiguous. A snapshot below `earliest` was
    /// therefore expired and the watermark may move past it, while anything at or above it must
    /// still be readable. Any other failure to load a snapshot is real and must propagate:
    /// skipping it would let a single transient object storage error advance the watermark and
    /// permanently drop a committed snapshot. The watermark stays put on that path, so the read
    /// can simply be retried.
    ///
    /// `earliest` is resolved on the failure path rather than before the loop so that a healthy
    /// read pays nothing for it - the EARLIEST hint may be absent, in which case resolving it
    /// means listing the whole snapshot directory.
    auto was_expired_by_paimon = [&](Int64 id)
    {
        const auto earliest_snapshot_id = table_client->getEarliestSnapshotId();
        if (!earliest_snapshot_id || id >= *earliest_snapshot_id)
            return false;

        LOG_INFO(log, "Snapshot_id={} was expired by Paimon (earliest available is {}), skipping",
            id, *earliest_snapshot_id);
        return true;
    };

    /// Spelling out the cost is part of the instruction: moving the cursor past a snapshot
    /// that cannot be read is how the data in it is lost, so the operator has to opt into it.
    auto unreadable_snapshot_hint = [&](Int64 id) -> String
    {
        if (!persistent_components.hasStreamState())
            return {};

        const auto & keeper_path = persistent_components.stream_state->getKeeperPath();
        return fmt::format(
            " If this snapshot is permanently unreadable, abandon it explicitly with: "
            "clickhouse-keeper-client -q \"set '{}/committed_snapshot' '{}'\". That permanently "
            "drops every snapshot not yet consumed up to and including {}, not just this one - "
            "the read failed before delivering any of them. To keep the readable ones, first "
            "poll with SETTINGS max_consume_snapshots = 1 until the read fails again.",
            keeper_path, id, id);
    };

    for (Int64 snapshot_id = from_snapshot_id + 1; snapshot_id <= to_snapshot_id; ++snapshot_id)
    {
        if (max_snapshots_to_load > 0 && snapshots.size() >= max_snapshots_to_load)
            break;

        PaimonTableStatePtr state;
        try
        {
            state = loadStateForSnapshot(snapshot_id);
        }
        catch (Exception & e)
        {
            if (was_expired_by_paimon(snapshot_id))
            {
                last_scanned_snapshot_id = snapshot_id;
                continue;
            }

            /// The raw backend error says nothing about which table stalled or how to move on,
            /// so name both. Abandoning the snapshot is left to the operator on purpose: it
            /// drops committed data, and only they can decide that is acceptable.
            e.addMessage(
                "while reading Paimon snapshot {} of '{}' during an incremental read. "
                "The consumed-snapshot cursor was not advanced, so this read can be retried "
                "once the underlying error clears.{}",
                snapshot_id,
                persistent_components.table_path,
                unreadable_snapshot_hint(snapshot_id));
            throw;
        }
        catch (...)
        {
            if (was_expired_by_paimon(snapshot_id))
            {
                last_scanned_snapshot_id = snapshot_id;
                continue;
            }

            /// Not a DB::Exception, so there is nothing to attach the context to - a missing
            /// file on some object storage backends still surfaces as a bare std::exception.
            /// Rebuild it instead, keeping the original code and message (with stack trace).
            throw Exception(
                getCurrentExceptionCode(),
                "{}: while reading Paimon snapshot {} of '{}' during an incremental read. "
                "The consumed-snapshot cursor was not advanced, so this read can be retried "
                "once the underlying error clears.{}",
                getCurrentExceptionMessage(/*with_stacktrace=*/true),
                snapshot_id,
                persistent_components.table_path,
                unreadable_snapshot_hint(snapshot_id));
        }

        /// Track the highest snapshot_id actually resolved, whether it is used or skipped
        /// as compact. The caller uses this to advance the watermark past gaps.
        last_scanned_snapshot_id = snapshot_id;

        if (skip_compact && state->isCompact())
        {
            LOG_TRACE(log, "Skipping Compact snapshot_id={} in incremental read", snapshot_id);
            continue;
        }

        snapshots.emplace_back(std::move(state));
    }

    return snapshots;
}

Strings PaimonMetadata::collectIncrementalDataFiles(
    const PaimonTableStatePtr & state,
    const std::optional<PartitionPruner> & partition_pruner,
    UInt64 max_consume_snapshots,
    std::optional<Int64> & last_consumed_snapshot_id) const
{
    Strings data_files;
    last_consumed_snapshot_id.reset();

    /// Get last committed snapshot ID from Keeper
    auto committed_snapshot_id = getCommittedSnapshotId();

    if (!committed_snapshot_id.has_value())
    {
        /// No committed snapshot yet, this is the first read
        /// First read should include full snapshot (base + delta) to build the initial watermark.
        LOG_INFO(log, "No committed snapshot found, performing initial full read (base+delta) for snapshot_id={}",
                 state->snapshot_id);
        data_files = collectDataFilesFromManifests({state}, ManifestKind::Both, partition_pruner, true, true);
        last_consumed_snapshot_id = state->snapshot_id;
    }
    else if (*committed_snapshot_id == state->snapshot_id)
    {
        /// Already processed this snapshot, no new data
        LOG_DEBUG(log, "Snapshot {} already processed (committed={}), no new data",
                  state->snapshot_id, *committed_snapshot_id);
        return {};
    }
    else if (*committed_snapshot_id > state->snapshot_id)
    {
        /// `state` is pinned during analysis, while the watermark is read here at execution
        /// time under the processing lock, so the two come from different moments. Another
        /// consumer sharing this cursor can complete a whole poll in between and legitimately
        /// leave the watermark ahead of what this query pinned - nothing was rolled back.
        ///
        /// Resolve the warehouse head again to tell the two apart. Only a watermark ahead of
        /// the *current* head means the warehouse lost snapshots off the top; a watermark
        /// ahead of the pinned state alone means somebody else simply got there first.
        const auto latest_snapshot_info = table_client->getLatestTableSnapshotInfo();

        if (latest_snapshot_info && *committed_snapshot_id <= latest_snapshot_info->first)
        {
            /// Everything up to the watermark has been consumed by another reader. Do not
            /// consume up to the fresh head either: this query's schema was resolved from
            /// the pinned state, and Paimon schemas can change across snapshots. The next
            /// poll pins a fresh state and picks the new snapshots up.
            LOG_DEBUG(log, "Watermark {} was advanced past the pinned snapshot {} by another reader "
                "(warehouse head is {}), no new data",
                *committed_snapshot_id, state->snapshot_id, latest_snapshot_info->first);
            return {};
        }

        /// Nothing has been read and the watermark is untouched, so this failure is
        /// idempotent: every poll fails the same way until an operator resolves it.
        if (!latest_snapshot_info)
            throw Exception(
                ErrorCodes::INVALID_STATE,
                "Paimon incremental read cursor is at snapshot {}, but '{}' has no snapshots at all. "
                "The warehouse was emptied or recreated while the Keeper cursor at '{}' was not reset. "
                "No data was read and the cursor was not modified.",
                *committed_snapshot_id,
                persistent_components.table_path,
                persistent_components.stream_state->getKeeperPath());

        const Int64 warehouse_head = latest_snapshot_info->first;
        throw Exception(
            ErrorCodes::INVALID_STATE,
            "Paimon incremental read cursor is ahead of the warehouse: committed snapshot is {}, "
            "but the latest snapshot in '{}' is {}. "
            "The warehouse was rolled back, restored, or recreated while the Keeper cursor at '{}' "
            "was not reset. No data was read and the cursor was not modified. "
            "To resume from the warehouse's current head: "
            "clickhouse-keeper-client -q \"set '{}/committed_snapshot' '{}'\". "
            "Do NOT delete that node: it triggers a full re-read of the entire table.",
            *committed_snapshot_id,
            persistent_components.table_path,
            warehouse_head,
            persistent_components.stream_state->getKeeperPath(),
            persistent_components.stream_state->getKeeperPath(),
            warehouse_head);
    }
    else
    {
        /// Read delta since last committed snapshot
        LOG_INFO(log, "Reading incremental data from snapshot {} to {}",
                 *committed_snapshot_id, state->snapshot_id);

        /// In Paimon, each snapshot's delta_manifest_list contains the changes in that snapshot.
        /// We need to read all delta manifests from snapshots between committed+1 and current.
        /// Skip Compact snapshots: their delta manifests contain compaction output (not new data).
        std::optional<Int64> last_scanned_snapshot_id;
        auto snapshots = getSnapshotsBetween(
            *committed_snapshot_id, state->snapshot_id, max_consume_snapshots,
            /*skip_compact=*/true, last_scanned_snapshot_id);

        if (snapshots.empty())
        {
            /// All snapshots in the range were either compact or missing.
            /// Still advance the watermark so we don't re-scan them next time.
            if (last_scanned_snapshot_id)
                last_consumed_snapshot_id = last_scanned_snapshot_id;
            return {};
        }

        data_files = collectDataFilesFromManifests(snapshots, ManifestKind::Delta, partition_pruner, true, true);
        /// Use last_scanned (not snapshots.back()) to advance past trailing compact/missing snapshots.
        last_consumed_snapshot_id = last_scanned_snapshot_id.value_or(snapshots.back()->snapshot_id);
    }

    return data_files;
}

Strings PaimonMetadata::collectDataFilesFromManifests(
    const std::vector<PaimonTableStatePtr> & snapshots,
    ManifestKind kind,
    const std::optional<PartitionPruner> & partition_pruner,
    bool deduplicate,
    bool track_deletes) const
{
    Strings data_files;
    std::unordered_set<String> seen_files;
    std::unordered_set<String> delete_files;

    auto collect_from_manifest = [&](const PaimonTableStatePtr & snapshot_state, const String & manifest_list_path, const String & type)
    {
        if (!snapshot_state || manifest_list_path.empty())
            return;

        auto manifest_metas = getManifestList(manifest_list_path);
        for (const auto & meta : *manifest_metas)
        {
            auto manifest = getManifest(meta.file_name, snapshot_state->schema_id);
            for (const auto & entry : manifest->entries)
            {
                String file_path = (std::filesystem::path(persistent_components.table_path)
                    / entry.file.bucket_path / entry.file.file_name);

                if (entry.kind == PaimonManifestEntry::Kind::DELETE)
                {
                    if (track_deletes)
                    {
                        delete_files.emplace(file_path);
                        LOG_TEST(log, "{} delete file: {}", type, file_path);
                    }
                    continue;
                }

                if (partition_pruner && partition_pruner->canBePruned(entry))
                {
                    LOG_TEST(log, "Partition pruned {} manifest file: {}, {}",
                             type, entry.file.file_name, entry.file.bucket_path);
                    continue;
                }

                if (deduplicate && !seen_files.emplace(file_path).second)
                {
                    LOG_TEST(log, "Skip duplicated {} data file: {}", type, file_path);
                    continue;
                }

                data_files.emplace_back(std::move(file_path));
                LOG_TEST(log, "{} data file: {}", type, data_files.back());
            }
        }
    };

    for (const auto & snapshot_state : snapshots)
    {
        if (kind == ManifestKind::Base || kind == ManifestKind::Both)
            collect_from_manifest(snapshot_state, snapshot_state->base_manifest_list_path, "base");
        if (kind == ManifestKind::Delta || kind == ManifestKind::Both)
            collect_from_manifest(snapshot_state, snapshot_state->delta_manifest_list_path, "delta");
    }

    if (track_deletes && !delete_files.empty())
    {
        data_files.erase(
            std::remove_if(
                data_files.begin(),
                data_files.end(),
                [&](const String & path) { return delete_files.contains(path); }),
            data_files.end());
    }

    return data_files;
}

Strings PaimonMetadata::collectDeltaFilesForSnapshot(
    const PaimonTableStatePtr & state,
    const std::optional<PartitionPruner> & partition_pruner) const
{
    return collectDataFilesFromManifests({state}, ManifestKind::Delta, partition_pruner, false, false);
}

Strings PaimonMetadata::collectFullScanDataFiles(
    const PaimonTableStatePtr & state,
    const std::optional<PartitionPruner> & partition_pruner) const
{
    /// Full scan: include base + delta, with dedup and tombstone handling.
    return collectDataFilesFromManifests({state}, ManifestKind::Both, partition_pruner, true, true);
}

}

#endif
