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
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <base/scope_guard.h>
#include <base/defines.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <fmt/format.h>


namespace DB
{

using namespace Paimon;

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int NO_ZOOKEEPER;
}

namespace Setting
{
extern const SettingsBool use_paimon_partition_pruning;
}

namespace DataLakeStorageSetting
{
extern const DataLakeStorageSettingsBool paimon_incremental_read;
extern const DataLakeStorageSettingsInt64 paimon_metadata_refresh_interval_ms;
extern const DataLakeStorageSettingsInt64 paimon_target_snapshot_id;
extern const DataLakeStorageSettingsString paimon_keeper_path;
extern const DataLakeStorageSettingsString paimon_replica_name;
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
    Int64 metadata_refresh_interval_ms = data_lake_settings[DataLakeStorageSetting::paimon_metadata_refresh_interval_ms].value;
    Int64 target_snapshot_id = data_lake_settings[DataLakeStorageSetting::paimon_target_snapshot_id].value;
    PaimonStreamStatePtr stream_state = nullptr;

    if (incremental_read_enabled)
    {
        if (!local_context->hasZooKeeper())
            throw Exception(ErrorCodes::NO_ZOOKEEPER, "Incremental read requires Keeper but ZooKeeper is not configured");

        String keeper_path = data_lake_settings[DataLakeStorageSetting::paimon_keeper_path].value;
        String replica_path = keeper_path + "/replicas/" + data_lake_settings[DataLakeStorageSetting::paimon_replica_name].value;
        if (keeper_path.empty() || replica_path.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "To use Paimon incremental read both paimon_keeper_path and paimon_replica_name must be specified");

        auto keeper = local_context->getZooKeeper();
        auto stream_log = getLogger("PaimonStreamState");
        stream_state = std::make_shared<PaimonStreamState>(keeper, keeper_path, replica_path, stream_log);
        stream_state->initializeKeeperNodes();
        if (!stream_state->activate())
            LOG_WARNING(stream_log, "Replica {} not activated for Paimon incremental read (maybe already active elsewhere)", replica_path);
    }

    /// Create persistent components
    PaimonPersistentComponents persistent_components(
        schema_processor,
        stream_state,
        configuration_ptr->getPathForRead().path,
        table_path,
        partition_default_name,
        incremental_read_enabled,
        target_snapshot_id,
        metadata_refresh_interval_ms);

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
    , refresh_interval_ms(persistent_components.metadata_refresh_interval_ms > 0
            ? std::chrono::milliseconds(persistent_components.metadata_refresh_interval_ms)
            : std::chrono::milliseconds(0))
{
    /// Validate configuration
    checkSupportedConfiguration();

    /// Load initial state
    auto initial_state = loadLatestState();
    std::atomic_store_explicit(&current_state, initial_state, std::memory_order_release);

    if (initial_state)
    {
        LOG_DEBUG(log, "PaimonMetadata initialized with snapshot_id={}, schema_id={}",
                  initial_state->snapshot_id, initial_state->schema_id);
    }
    else
    {
        LOG_WARNING(log, "PaimonMetadata initialized without snapshots (no snapshot files found yet)");
    }

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
    auto snapshot_info_opt = table_client->getLastestTableSnapshotInfo();
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
        snapshot.time_millis,
        snapshot.total_record_count,
        snapshot.delta_record_count,
        snapshot.changelog_record_count,
        snapshot.watermark);
}

void PaimonMetadata::update(const ContextPtr & /*local_context*/)
{
    /// 1. Load new state outside any lock (I/O operations)
    auto new_state = loadLatestState();
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

StorageInMemoryMetadata PaimonMetadata::getStorageSnapshotMetadata(ContextPtr /*local_context*/) const
{
    auto state = getCurrentState();
    if (!state)
    {
        /// No snapshots yet: still allow schema-based metadata (DESC, SHOW).
        auto schema_info = table_client->getLatestTableSchemaInfo();
        auto schema_json = table_client->getTableSchemaJSON(schema_info);
        auto schema = persistent_components.schema_processor->getOrAddSchema(schema_info.first, schema_json);
        auto columns = persistent_components.schema_processor->getClickHouseSchema(schema->id);

        StorageInMemoryMetadata result;
        if (columns)
            result.setColumns(ColumnsDescription{*columns});
        return result;
    }

    /// Get column definitions from schema processor
    auto columns = persistent_components.schema_processor->getClickHouseSchema(state->schema_id);
    if (!columns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to get ClickHouse schema for schema_id={}", state->schema_id);

    StorageInMemoryMetadata result;
    result.setColumns(ColumnsDescription{*columns});

    /// Attach table state to metadata for snapshot isolation
    /// The state will be used by iterate() to ensure consistent view
    /// Note: This requires adding setDataLakeTableState to StorageInMemoryMetadata
    /// For now, we store it in a custom field or use existing mechanism

    return result;
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

PaimonTableStatePtr PaimonMetadata::extractTableState(StorageMetadataPtr /*storage_metadata*/)
{
    /// TODO: Extract PaimonTableState from storage_metadata.datalake_table_state
    /// For now, return nullptr to fall back to current state
    return nullptr;
}

std::vector<PaimonManifestFileMeta> PaimonMetadata::getManifestList(const String & manifest_list_path) const
{
    if (manifest_list_path.empty())
        return {};

    /// No cache, load directly
    LOG_TRACE(log, "Loading manifest list (no cache): {}", manifest_list_path);
    return table_client->getManifestMeta(manifest_list_path);
}

PaimonManifest PaimonMetadata::getManifest(const String & manifest_path, Int64 schema_id) const
{
    auto schema = persistent_components.schema_processor->getSchemaById(schema_id);
    if (!schema)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Schema with id {} not found", schema_id);

    /// No cache, load directly
    LOG_TRACE(log, "Loading manifest (no cache): {}", manifest_path);
    return table_client->getDataManifest(manifest_path, *schema, persistent_components.partition_default_name);
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
            if (state)
                std::atomic_store_explicit(&current_state, state, std::memory_order_release);
        }
    }

    if (!state)
        return createKeysIterator({}, object_storage, callback); /// still no snapshot: return empty

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

    /// 4.a Session-level targeted snapshot (only when incremental is enabled)
    if (persistent_components.incremental_read_enabled && persistent_components.target_snapshot_id > 0)
    {
        auto target_state = loadStateForSnapshot(persistent_components.target_snapshot_id);
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
            stream_state->activate();
        }

        bool lock_acquired = false;
        SCOPE_EXIT(
        {
            if (lock_acquired)
                stream_state->releaseProcessingLock();
        });

        stream_state->acquireProcessingLock();
        lock_acquired = true;

        data_files = collectIncrementalDataFiles(state, partition_pruner);

        if (!data_files.empty())
            stream_state->setCommittedSnapshot(state->snapshot_id);
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

void PaimonMetadata::commitSnapshot(Int64 snapshot_id)
{
    if (!persistent_components.hasStreamState())
    {
        LOG_WARNING(log, "commitSnapshot called but incremental read is disabled");
        return;
    }
    persistent_components.stream_state->setCommittedSnapshot(snapshot_id);
}

void PaimonMetadata::scheduleBackgroundRefresh()
{
    if (refresh_interval_ms.count() == 0)
        return;

    auto & schedule_pool = getContext()->getSchedulePool();
    refresh_task = schedule_pool.createTask(
        StorageID::createEmpty(), "PaimonMetadataRefresh/" + persistent_components.table_path,
        [this]()
        {
            runBackgroundRefresh();
        });
    refresh_task->scheduleAfter(refresh_interval_ms.count());
}

void PaimonMetadata::runBackgroundRefresh()
{
    if (!refresh_task)
        return;

    /// Prevent overlapping runs
    bool expected = false;
    if (!refresh_in_progress.compare_exchange_strong(expected, true))
    {
        refresh_task->scheduleAfter(refresh_interval_ms.count());
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
    refresh_task->scheduleAfter(refresh_interval_ms.count());
}

PaimonTableStatePtr PaimonMetadata::loadStateForSnapshot(Int64 snapshot_id) const
{
    /// Get snapshot by ID
    /// Build snapshot file path: `table_location/snapshot/snapshot-<id>`
    const String snapshot_path = (std::filesystem::path(persistent_components.table_location)
        / PAIMON_SNAPSHOT_DIR
        / fmt::format("{}{}", PAIMON_SNAPSHOT_PRIFIX, snapshot_id));
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
        snapshot.time_millis,
        snapshot.total_record_count,
        snapshot.delta_record_count,
        snapshot.changelog_record_count,
        snapshot.watermark);
}

std::vector<PaimonTableStatePtr> PaimonMetadata::getSnapshotsBetween(
    Int64 from_snapshot_id, Int64 to_snapshot_id) const
{
    std::vector<PaimonTableStatePtr> snapshots;

    /// For simplicity, we only return the latest snapshot state
    /// In a full implementation, we would iterate through all snapshots between from and to
    /// and collect their delta manifests
    ///
    /// Paimon Java implementation uses StreamTableScan which maintains a checkpoint (snapshot id)
    /// and on each plan() call, it returns splits for the delta since the last checkpoint.
    ///
    /// For now, we just use the delta_manifest_list from the latest snapshot
    /// which contains all changes in that snapshot.

    if (to_snapshot_id > from_snapshot_id)
    {
        auto state = loadLatestState();
        if (state && state->snapshot_id == to_snapshot_id)
        {
            snapshots.push_back(state);
        }
    }

    return snapshots;
}

Strings PaimonMetadata::collectIncrementalDataFiles(
    const PaimonTableStatePtr & state,
    const std::optional<PartitionPruner> & partition_pruner) const
{
    Strings data_files;

    /// Get last committed snapshot ID from Keeper
    auto committed_snapshot_id = getCommittedSnapshotId();

    if (!committed_snapshot_id.has_value())
    {
        /// No committed snapshot yet, this is the first read
        /// First read should include full snapshot (base + delta) to build the initial watermark.
        LOG_INFO(log, "No committed snapshot found, performing initial full read (base+delta) for snapshot_id={}",
                 state->snapshot_id);

        std::unordered_set<String> seen_files;
        auto collect_manifest_list = [&](const String & manifest_list_path)
        {
            if (manifest_list_path.empty())
                return;

            auto manifest_metas = getManifestList(manifest_list_path);
            for (const auto & meta : manifest_metas)
            {
                auto manifest = getManifest(meta.file_name, state->schema_id);
                for (const auto & entry : manifest.entries)
                {
                    if (entry.kind == PaimonManifestEntry::Kind::DELETE)
                        continue;

                    if (partition_pruner && partition_pruner->canBePruned(entry))
                        continue;

                    String file_path = (std::filesystem::path(persistent_components.table_path)
                        / entry.file.bucket_path / entry.file.file_name);
                    if (!seen_files.emplace(file_path).second)
                        continue;
                    data_files.emplace_back(std::move(file_path));
                }
            }
        };

        collect_manifest_list(state->base_manifest_list_path);
        collect_manifest_list(state->delta_manifest_list_path);
    }
    else if (*committed_snapshot_id >= state->snapshot_id)
    {
        /// Already processed this snapshot, no new data
        LOG_DEBUG(log, "Snapshot {} already processed (committed={}), no new data",
                  state->snapshot_id, *committed_snapshot_id);
        return {};
    }
    else
    {
        /// Read delta since last committed snapshot
        LOG_INFO(log, "Reading incremental data from snapshot {} to {}",
                 *committed_snapshot_id, state->snapshot_id);

        /// In Paimon, each snapshot's delta_manifest_list contains the changes in that snapshot
        /// We need to read all delta manifests from snapshots between committed+1 and current
        ///
        /// For simplicity, we just read the current snapshot's delta manifest
        /// This works correctly if we process one snapshot at a time

        if (!state->delta_manifest_list_path.empty())
    {
            auto manifest_metas = getManifestList(state->delta_manifest_list_path);
            for (const auto & meta : manifest_metas)
            {
                auto manifest = getManifest(meta.file_name, state->schema_id);
                for (const auto & entry : manifest.entries)
        {
                    if (entry.kind == PaimonManifestEntry::Kind::DELETE)
                continue;

                    if (partition_pruner && partition_pruner->canBePruned(entry))
                continue;

                    data_files.emplace_back(
                        std::filesystem::path(persistent_components.table_path)
                        / entry.file.bucket_path / entry.file.file_name);
                }
            }
        }
    }

    return data_files;
}

Strings PaimonMetadata::collectDeltaFilesForSnapshot(
    const PaimonTableStatePtr & state,
    const std::optional<PartitionPruner> & partition_pruner) const
{
    Strings data_files;
    if (!state || state->delta_manifest_list_path.empty())
        return data_files;

    auto manifest_metas = getManifestList(state->delta_manifest_list_path);
    for (const auto & meta : manifest_metas)
    {
        auto manifest = getManifest(meta.file_name, state->schema_id);
        for (const auto & entry : manifest.entries)
        {
            if (entry.kind == PaimonManifestEntry::Kind::DELETE)
                continue;

            if (partition_pruner && partition_pruner->canBePruned(entry))
                continue;

            data_files.emplace_back(
                std::filesystem::path(persistent_components.table_path)
                / entry.file.bucket_path / entry.file.file_name);
        }
    }

    return data_files;
}

Strings PaimonMetadata::collectFullScanDataFiles(
    const PaimonTableStatePtr & state,
    const std::optional<PartitionPruner> & partition_pruner) const
{
    Strings data_files;
    std::unordered_set<String> seen_files;
    std::unordered_set<String> delete_files;

    auto collect_files = [&](const String & manifest_list_path, const String & type)
    {
        if (manifest_list_path.empty())
            return;

        auto manifest_metas = getManifestList(manifest_list_path);
        for (const auto & meta : manifest_metas)
        {
            auto manifest = getManifest(meta.file_name, state->schema_id);
            for (const auto & entry : manifest.entries)
            {
                String file_path = (std::filesystem::path(persistent_components.table_path)
                    / entry.file.bucket_path / entry.file.file_name);

                if (entry.kind == PaimonManifestEntry::Kind::DELETE)
                {
                    delete_files.emplace(file_path);
                    LOG_TEST(log, "{} delete file: {}", type, file_path);
                    continue;
                }

                if (partition_pruner && partition_pruner->canBePruned(entry))
                {
                    LOG_TEST(log, "Partition pruned {} manifest file: {}, {}",
                             type, entry.file.file_name, entry.file.bucket_path);
                    continue;
                }

                if (!seen_files.emplace(file_path).second)
                {
                    LOG_TEST(log, "Skip duplicated {} data file: {}", type, file_path);
                continue;
                }

                data_files.emplace_back(std::move(file_path));
                LOG_TEST(log, "{} data file: {}", type, data_files.back());
            }
        }
    };

    /// Full scan: include base + delta, with dedup and tombstone handling.
    collect_files(state->base_manifest_list_path, "base");
    collect_files(state->delta_manifest_list_path, "delta");

    /// Apply delete markers best-effort.
    data_files.erase(
        std::remove_if(
            data_files.begin(),
            data_files.end(),
            [&](const String & path) { return delete_files.contains(path); }),
        data_files.end());

    return data_files;
}

}

#endif
