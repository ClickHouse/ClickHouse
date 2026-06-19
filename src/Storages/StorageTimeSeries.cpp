#include <Storages/StorageTimeSeries.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/IBackup.h>
#include <Backups/RestorerFromBackup.h>
#include <Storages/AlterCommands.h>
#include <Storages/StorageFactory.h>
#include <Storages/TimeSeries/TimeSeriesSink.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/createTimeSeriesInnerTable.h>
#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>
#include <base/insertAtEnd.h>
#include <filesystem>
#include <boost/algorithm/string.hpp>
#include <base/EnumReflection.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_time_series_table;
}

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNEXPECTED_TABLE_ENGINE;
    extern const int UNKNOWN_TABLE;
}

namespace fs = std::filesystem;


namespace
{
    /// Normalizes the create query.
    boost::intrusive_ptr<const ASTCreateQuery> makeNormalizedCreateQuery(
        const ASTCreateQuery & query, const ContextPtr & local_context, LoadingStrictnessLevel mode, bool is_restore_from_backup)
    {
        auto copy = boost::static_pointer_cast<ASTCreateQuery>(query.clone());
        normalizeTimeSeriesDefinition(*copy, local_context, mode, is_restore_from_backup);
        return copy;
    }

    /// We allow altering only two settings: `id_generator` and `filter_by_min_time_and_max_time`.
    void checkSettingCanBeAltered(std::string_view setting_name, std::string_view storage_name)
    {
        if ((setting_name != "id_generator") && (setting_name != "filter_by_min_time_and_max_time"))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Setting '{}' of storage {} cannot be changed after the table is created", setting_name, storage_name);
    }
}


std::vector<StorageTimeSeries::Target> StorageTimeSeries::buildTargets(
    const ASTCreateQuery & create_query,
    const StorageID & table_id,
    const ContextPtr & local_context,
    LoadingStrictnessLevel mode)
{
    if (mode <= LoadingStrictnessLevel::CREATE && !local_context->getSettingsRef()[Setting::allow_experimental_time_series_table])
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                        "Experimental TimeSeries table engine "
                        "is not enabled (the setting 'allow_experimental_time_series_table')");
    }

    std::vector<Target> targets;
    for (auto target_kind : getTargetKinds())
    {
        Target target;
        target.kind = target_kind;

        if (auto target_table_id = create_query.getTargetTableID(target_kind))
        {
            /// A target table is specified.
            target.table_id = target_table_id;
        }
        else
        {
            /// An inner target table should be used.
            auto inner_table_uuid = create_query.getTargetInnerUUID(target_kind);

            target.table_id.uuid = inner_table_uuid;
            target.is_inner_table = true;

            if (mode <= LoadingStrictnessLevel::SECONDARY_CREATE)
            {
                /// Create the inner target table using the pre-computed inner columns from the create query.
                auto * inner_columns = create_query.getTargetInnerColumns(target_kind);
                chassert(inner_columns != nullptr);
                auto inner_engine = boost::static_pointer_cast<ASTStorage>(
                    create_query.getTargetInnerEngine(target_kind)
                        ? create_query.getTargetInnerEngine(target_kind)->ptr()
                        : ASTPtr{});
                createTimeSeriesInnerTable(target_kind, inner_table_uuid, *inner_columns, inner_engine, table_id, local_context);
            }
        }

        targets.emplace_back(std::move(target));
    }
    return targets;
}


StorageTimeSeries::StorageTimeSeries(
    const StorageID & table_id,
    const ContextPtr & local_context,
    LoadingStrictnessLevel mode,
    bool is_restore_from_backup,
    const ASTCreateQuery & query,
    const ColumnsDescription & /*columns*/,
    const String & comment)
    : StorageWithCommonVirtualColumns(table_id)
    , WithContext(local_context->getGlobalContext())
    , normalized_create_query(makeNormalizedCreateQuery(query, local_context, mode, is_restore_from_backup))
    , targets(buildTargets(*normalized_create_query, table_id, local_context, mode))
    , has_inner_tables(std::ranges::any_of(targets, &Target::is_inner_table))
{
    /// Load TimeSeries settings from the `SETTINGS` clause.
    auto settings = std::make_unique<TimeSeriesSettings>();
    if (normalized_create_query->storage)
        settings->loadFromQuery(*normalized_create_query->storage);
    storage_settings.set(std::move(settings));

    StorageInMemoryMetadata storage_metadata;

    /// Re-derive columns from the normalized AST rather than trusting the `columns` argument.
    /// For CREATE / RESTORE the query arrives already normalized.
    /// However for ATTACH InterpreterCreateQuery doesn't normalize the create query,
    /// so `columns` can contain prealpha outer columns which we should upgrade.
    auto normalized_columns = InterpreterCreateQuery::getColumnsDescription(
        *normalized_create_query->columns_list->columns, local_context, mode);
    storage_metadata.setColumns(normalized_columns);

    if (!comment.empty())
        storage_metadata.setComment(comment);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}


StorageTimeSeries::~StorageTimeSeries() = default;


StoragePtr StorageTimeSeries::getTargetTable(ViewTarget::Kind target_kind, const ContextPtr & local_context) const
{
    return getTargetTableImpl(target_kind, local_context, /* throw_if_not_found = */ true);
}

StoragePtr StorageTimeSeries::tryGetTargetTable(ViewTarget::Kind target_kind, const ContextPtr & local_context) const
{
    return getTargetTableImpl(target_kind, local_context, /* throw_if_not_found = */ false);
}

StoragePtr StorageTimeSeries::getTargetTableImpl(ViewTarget::Kind target_kind, const ContextPtr & local_context, bool throw_if_not_found) const
{
    /// `targets` is populated in the `getTargetKinds()` order.
    auto index = static_cast<size_t>(target_kind - ViewTarget::Samples);
    if (index >= targets.size() || targets[index].kind != target_kind)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected target kind {} (index={})", target_kind, index);
    const auto & target = targets[index];

    auto lookup = [&](const StorageID & id) -> StoragePtr
    {
        return DatabaseCatalog::instance()
            .tryGetDatabaseAndTable(local_context->tryResolveStorageID(id), local_context)
            .second;
    };

    /// For external targets `target.table_id` contains a table name.
    if (!target.table_id.table_name.empty())
    {
        auto res = lookup(target.table_id);
        if (!res && throw_if_not_found)
        {
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "The {} target table {} for TimeSeries table {} doesn't exist",
                            target_kind, target.table_id.getNameForLogs(), getStorageID().getNameForLogs());
        }
        return res;
    }

    /// For inner targets in Atomic databases `target.table_id` has a UUID but no name — look up directly by UUID.
    if (target.table_id.hasUUID())
    {
        auto res = DatabaseCatalog::instance().tryGetByUUID(target.table_id.uuid).second;
        if (!res && throw_if_not_found)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "The {} inner table {} for TimeSeries table {} doesn't exist",
                            target_kind, target.table_id.getNameForLogs(), getStorageID().getNameForLogs());
        return res;
    }

    chassert(target.table_id.empty());

    /// For inner targets in non-Atomic databases, `target.table_id` is empty and we look up the inner table by its constructed name.
    StorageID time_series_table_id = getStorageID();
    StorageID inner_table_id{time_series_table_id.getDatabaseName(), getTimeSeriesInnerTableName(target_kind, time_series_table_id)};

    if (auto res = lookup(inner_table_id))
        return res;

    /// Fallback for legacy tables created before the samples inner table was renamed
    /// from `.inner.data.*` to `.inner.samples.*`
    if (target_kind == ViewTarget::Samples)
    {
        inner_table_id.table_name = getTimeSeriesInnerTableName("data", time_series_table_id);
        if (auto res = lookup(inner_table_id))
            return res;
    }

    if (throw_if_not_found)
    {
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "The {} inner table {} for TimeSeries table {} doesn't exist",
                        target_kind, inner_table_id.getNameForLogs(), getStorageID().getNameForLogs());
    }

    return nullptr;
}


StorageID StorageTimeSeries::getTargetTableID(ViewTarget::Kind target_kind, const ContextPtr & local_context) const
{
    return getTargetTable(target_kind, local_context)->getStorageID();
}

StorageID StorageTimeSeries::tryGetTargetTableID(ViewTarget::Kind target_kind, const ContextPtr & local_context) const
{
    if (auto target_table = tryGetTargetTable(target_kind, local_context))
        return target_table->getStorageID();
    return StorageID::createEmpty();
}

bool StorageTimeSeries::isInnerTable(ViewTarget::Kind target_kind) const
{
    /// `targets` is populated in the `getTargetKinds()` order.
    auto index = static_cast<size_t>(target_kind - ViewTarget::Samples);
    if (index >= targets.size() || targets[index].kind != target_kind)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected target kind {} (index={})", target_kind, index);
    return targets[index].is_inner_table;
}


void StorageTimeSeries::drop()
{
    /// Sync flag and the setting make sense for Atomic databases only.
    /// However, with Atomic databases, IStorage::drop() can be called only from a background task in DatabaseCatalog.
    /// Running synchronous DROP from that task leads to deadlock.
    dropInnerTableIfAny(/* sync= */ false, getContext());
}

void StorageTimeSeries::dropInnerTableIfAny(bool sync, ContextPtr local_context)
{
    if (!hasInnerTables())
        return;

    for (auto target_kind : getTargetKinds())
    {
        if (isInnerTable(target_kind))
        {
            if (auto inner_table_id = tryGetTargetTableID(target_kind, local_context))
            {
                /// Best-effort to make them work: the inner table name is almost always less than the TimeSeries name (so it's safe to lock DDLGuard).
                /// (See the comment in StorageMaterializedView::dropInnerTableIfAny.)
                bool may_lock_ddl_guard = getStorageID().getQualifiedName() < inner_table_id.getQualifiedName();
                InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, getContext(), local_context, inner_table_id,
                                                    sync, /* ignore_sync_setting= */ true, may_lock_ddl_guard);
            }
        }
    }
}

void StorageTimeSeries::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr local_context, TableExclusiveLockHolder &)
{
    if (!hasInnerTables())
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "TimeSeries table {} targets only existing tables. Execute the statement directly on it.",
                        getStorageID().getNameForLogs());
    }

    for (auto target_kind : getTargetKinds())
    {
        /// We truncate only inner tables here.
        if (isInnerTable(target_kind))
        {
            auto inner_table_id = getTargetTableID(target_kind, local_context);
            InterpreterDropQuery::executeDropQuery(
                ASTDropQuery::Kind::Truncate, getContext(), local_context, inner_table_id, /* sync= */ true);
        }
    }
}


std::optional<UInt64> StorageTimeSeries::totalRows(ContextPtr query_context) const
{
    if (!hasInnerTables())
        return 0;
    UInt64 total_rows = 0;
    for (auto target_kind : getTargetKinds())
    {
        if (isInnerTable(target_kind))
        {
            auto inner_table = tryGetTargetTable(target_kind, query_context);
            if (!inner_table)
                return std::nullopt;

            auto total_rows_in_inner_table = inner_table->totalRows(query_context);
            if (!total_rows_in_inner_table)
                return std::nullopt;

            total_rows += *total_rows_in_inner_table;
        }
    }
    return total_rows;
}

std::optional<UInt64> StorageTimeSeries::totalBytes(ContextPtr query_context) const
{
    if (!hasInnerTables())
        return 0;
    UInt64 total_bytes = 0;
    for (auto target_kind : getTargetKinds())
    {
        if (isInnerTable(target_kind))
        {
            auto inner_table = tryGetTargetTable(target_kind, query_context);
            if (!inner_table)
                return std::nullopt;

            auto total_bytes_in_inner_table = inner_table->totalBytes(query_context);
            if (!total_bytes_in_inner_table)
                return std::nullopt;

            total_bytes += *total_bytes_in_inner_table;
        }
    }
    return total_bytes;
}

std::optional<UInt64> StorageTimeSeries::totalBytesUncompressed(const Settings & settings) const
{
    if (!hasInnerTables())
        return 0;
    UInt64 total_bytes = 0;
    for (auto target_kind : getTargetKinds())
    {
        if (isInnerTable(target_kind))
        {
            auto inner_table = tryGetTargetTable(target_kind, getContext());
            if (!inner_table)
                return std::nullopt;

            auto total_bytes_in_inner_table = inner_table->totalBytesUncompressed(settings);
            if (!total_bytes_in_inner_table)
                return std::nullopt;

            total_bytes += *total_bytes_in_inner_table;
        }
    }
    return total_bytes;
}

Strings StorageTimeSeries::getDataPaths() const
{
    Strings data_paths;
    for (auto target_kind : getTargetKinds())
    {
        auto table = tryGetTargetTable(target_kind, getContext());
        if (!table)
            continue;

        insertAtEnd(data_paths, table->getDataPaths());
    }
    return data_paths;
}


bool StorageTimeSeries::optimize(
    const ASTPtr & query,
    const StorageMetadataPtr &,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup,
    ContextPtr local_context)
{
    if (!hasInnerTables())
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "TimeSeries table {} targets only existing tables. Execute the statement directly on it.",
                        getStorageID().getNameForLogs());
    }

    bool optimized = false;
    for (auto target_kind : getTargetKinds())
    {
        if (isInnerTable(target_kind))
        {
            auto inner_table = getTargetTable(target_kind, local_context);
            optimized |= inner_table->optimize(query, inner_table->getInMemoryMetadataPtr(local_context, false), partition, final, deduplicate, deduplicate_by_columns, cleanup, local_context);
        }
    }

    return optimized;
}


void StorageTimeSeries::checkAlterIsPossible(const AlterCommands & commands, ContextPtr) const
{
    for (const auto & command : commands)
    {
        if (command.isCommentAlter() || command.type == AlterCommand::MODIFY_SQL_SECURITY)
            continue;
        if (command.type == AlterCommand::MODIFY_SETTING)
        {
            for (const auto & change : command.settings_changes)
                checkSettingCanBeAltered(change.name, getName());
            continue;
        }
        if (command.type == AlterCommand::RESET_SETTING)
        {
            for (const auto & name : command.settings_resets)
                checkSettingCanBeAltered(name, getName());
            continue;
        }
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}", command.type, getName());
    }
}

void StorageTimeSeries::alter(const AlterCommands & params, ContextPtr local_context, AlterLockHolder &)
{
    StorageInMemoryMetadata new_metadata = *getInMemoryMetadataPtr(local_context, false);
    params.apply(new_metadata, local_context);

    std::unique_ptr<TimeSeriesSettings> new_settings;

    bool has_settings_changes = std::any_of(
        params.begin(), params.end(), [](const AlterCommand & c) { return c.isSettingsAlter(); });

    if (has_settings_changes)
    {
        chassert(new_metadata.settings_changes);
        /// Round-trip through `TimeSeriesSettings` to validate the names/values and
        /// to drop entries that equal to the defaults.
        new_settings = std::make_unique<TimeSeriesSettings>();
        new_settings->applyChanges(new_metadata.settings_changes->as<const ASTSetQuery &>().changes);
        checkTimeSeriesSettings(*new_settings);
        auto settings_changes = new_settings->changes();

        boost::intrusive_ptr<ASTSetQuery> settings_ast;
        /// Here `settings_changes` can be empty if `RESET SETTING` removed the last override.
        if (!settings_changes.empty())
        {
            settings_ast = make_intrusive<ASTSetQuery>();
            settings_ast->is_standalone = false;
            settings_ast->changes = std::move(settings_changes);
        }
        new_metadata.settings_changes = settings_ast;
    }

    auto time_series_table_id = getStorageID();
    DatabaseCatalog::instance().getDatabase(time_series_table_id.database_name)->alterTable(
        local_context, time_series_table_id, new_metadata, /*validate_new_create_query=*/true);
    setInMemoryMetadata(new_metadata);

    if (new_settings)
        storage_settings.set(std::move(new_settings));
}


void StorageTimeSeries::renameInMemory(const StorageID & /* new_table_id */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Renaming is not supported by storage {} yet", getName());
}


void StorageTimeSeries::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> &)
{
    if (!hasInnerTables())
        return;

    for (auto target_kind : getTargetKinds())
    {
        /// We backup the target table's data only if it's inner.
        if (isInnerTable(target_kind))
        {
            auto table = getTargetTable(target_kind, backup_entries_collector.getContext());
            String kind_str{magic_enum::enum_name(target_kind)};
            boost::algorithm::to_lower(kind_str);
            table->backupData(backup_entries_collector, fs::path{data_path_in_backup} / kind_str, {});
        }
    }
}

void StorageTimeSeries::restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> &)
{
    if (!hasInnerTables())
        return;

    for (auto target_kind : getTargetKinds())
    {
        /// We restore the target table's data only if it's inner.
        if (isInnerTable(target_kind))
        {
            auto table = getTargetTable(target_kind, restorer.getContext());
            String kind_str{magic_enum::enum_name(target_kind)};
            boost::algorithm::to_lower(kind_str);
            String target_data_path = fs::path{data_path_in_backup} / kind_str;
            /// Support legacy backups where the samples folder was named "data" instead of "samples".
            if (target_kind == ViewTarget::Samples && !restorer.getBackup()->hasFiles(target_data_path))
                target_data_path = fs::path{data_path_in_backup} / "data";
            table->restoreDataFromBackup(restorer, target_data_path, {});
        }
    }
}

VirtualColumnsDescription StorageTimeSeries::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

void StorageTimeSeries::readImpl(
    QueryPlan & /* query_plan */,
    const Names & /* column_names */,
    const StorageSnapshotPtr & /* storage_snapshot */,
    SelectQueryInfo & /* query_info */,
    ContextPtr /* local_context */,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    size_t /* num_streams */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "SELECT is not supported by storage {} yet", getName());
}


SinkToStoragePtr StorageTimeSeries::write(
    const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool async_insert)
{
    Names insert_columns;
    if (const auto * insert_query = query->as<ASTInsertQuery>())
    {
        if (insert_query->columns)
            for (const auto & col : insert_query->columns->children)
                insert_columns.push_back(col->getColumnName());
    }
    return std::make_shared<TimeSeriesSink>(*this, metadata_snapshot->getSampleBlock(), insert_columns, local_context, async_insert);
}


std::shared_ptr<StorageTimeSeries> storagePtrToTimeSeries(StoragePtr storage)
{
    if (auto res = typeid_cast<std::shared_ptr<StorageTimeSeries>>(storage))
        return res;

    throw Exception(
        ErrorCodes::UNEXPECTED_TABLE_ENGINE,
        "This operation can be executed on a TimeSeries table only, the engine of table {} is not TimeSeries",
        storage->getStorageID().getNameForLogs());
}

std::shared_ptr<const StorageTimeSeries> storagePtrToTimeSeries(ConstStoragePtr storage)
{
    if (auto res = typeid_cast<std::shared_ptr<const StorageTimeSeries>>(storage))
        return res;

    throw Exception(
        ErrorCodes::UNEXPECTED_TABLE_ENGINE,
        "This operation can be executed on a TimeSeries table only, the engine of table {} is not TimeSeries",
        storage->getStorageID().getNameForLogs());
}


void registerStorageTimeSeries(StorageFactory & factory);
void registerStorageTimeSeries(StorageFactory & factory)
{
    factory.registerStorage("TimeSeries", [](const StorageFactory::Arguments & args)
    {
        /// Pass local_context here to convey setting to inner tables.
        return std::make_shared<StorageTimeSeries>(
            args.table_id, args.getLocalContext(), args.mode, args.is_restore_from_backup,
            args.query, args.columns, args.comment);
    }
    ,
    {
        .supports_settings = true,
        .supports_schema_inference = true,
        .has_builtin_setting_fn = TimeSeriesSettings::hasBuiltin,
    },
    Documentation{
        .description = R"DOCS_MD(
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# TimeSeries table engine

<ExperimentalBadge/>
<CloudNotSupportedBadge/>

A table engine storing time series, i.e. a set of values associated with timestamps and tags (or labels):

```sql
metric_name1[tag1=value1, tag2=value2, ...] = {timestamp1: value1, timestamp2: value2, ...}
metric_name2[...] = ...
```

:::info
This is an experimental feature that may change in backwards-incompatible ways in the future releases.
Enable usage of the TimeSeries table engine
with [allow_experimental_time_series_table](/operations/settings/settings#allow_experimental_time_series_table) setting.
Input the command `set allow_experimental_time_series_table = 1`.
:::

## Syntax {#syntax}

```sql
CREATE TABLE name [(columns)] ENGINE=TimeSeries
[SETTINGS var1=value1, ...]
[SAMPLES db.samples_table_name | [SAMPLES INNER COLUMNS (...)] [SAMPLES INNER ENGINE engine(arguments)]]
[TAGS db.tags_table_name | [TAGS INNER COLUMNS (...)] [TAGS INNER ENGINE engine(arguments)]]
[METRICS db.metrics_table_name | [METRICS INNER COLUMNS (...)] [METRICS INNER ENGINE engine(arguments)]]
```

:::note
The keyword `SAMPLES` has an alias `DATA` which is kept for backwards compatibility.
:::

## Usage {#usage}

It's easier to start with everything set by default (it's allowed to create a `TimeSeries` table without specifying a list of columns):

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

Then this table can be used with the following protocols (a port must be assigned in the server configuration):
- [prometheus remote-write](/interfaces/prometheus#remote-write)
- [prometheus remote-read](/interfaces/prometheus#remote-read)

### Outer columns {#outer-columns}

Columns of a TimeSeries table are generated automatically. These are outer columns, they store no data, they just provide interface for SELECT/INSERT. Actual data is stored in [target tables](#target-tables). Here is the list of the outer columns:

| Name | Type | Description |
|---|---|---|
| `metric_name` | `String` | The name of the metric |
| `tags` | `Map(String, String)` | Map of tags (labels) for the time series |
| `time_series` | `Array(Tuple(DateTime64(3), Float64))` by default | Array of (timestamp, value) pairs for a time series. The tuple's timestamp and scalar element types can be derived from the samples `INNER COLUMNS` declaration (see [Specifying outer columns](#specifying-outer-columns)) |
| `metric_family` | `String` | The name of the metric family (for metrics metadata) |
| `type` | `String` | The type of the metric (e.g. "counter", "gauge") |
| `unit` | `String` | The unit of the metric |
| `help` | `String` | The description of the metric |

Example:

```sql
INSERT INTO my_table (metric_name, tags, time_series) VALUES
    ('cpu_usage', {'job': 'node_exporter', 'instance': 'host1:9100'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5), (toDateTime64('2024-01-01 00:01:00', 3), 0.7)])
```

`metric_name` is allowed to be empty on insertion, that means the metric name is specified in `tags` under `__name__`, for example:

```sql
INSERT INTO my_table (tags, time_series) VALUES
    ({'__name__': 'cpu_usage', 'job': 'test'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5)])
```

To insert metrics metadata, insert into the `metric_family`, `type`, `unit`, and `help` columns:

```sql
INSERT INTO my_table (metric_name, tags, time_series, metric_family, type, unit, help) VALUES
    ('http_requests_total', {'method': 'GET'}, [(now64(), 100.0)],
     'http_requests_total', 'counter', 'requests', 'Total HTTP requests')
```

### Specifying outer columns {#specifying-outer-columns}

The outer `time_series` column can be listed explicitly in a `CREATE TABLE` statement to override its default `Array(Tuple(DateTime64(3), Float64))` type. ClickHouse extracts the timestamp and scalar types from the tuple and propagates them to the inner samples table:

```sql
CREATE TABLE my_table (time_series Array(Tuple(UInt32, Float32))) ENGINE=TimeSeries
```

This is equivalent to declaring the timestamp and value column types in the samples `INNER COLUMNS` clause directly:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp UInt32, value Float32)
```

If both forms are used in the same `CREATE TABLE` statement, the declared types must match.

## Target tables {#target-tables}

A `TimeSeries` table doesn't have its own data, everything is stored in its target tables.
This is similar to how a [materialized view](../../../sql-reference/statements/create/view#materialized-view) works,
with the difference that a materialized view has one target table
whereas a `TimeSeries` table has three target tables named [samples](#samples-table), [tags](#tags-table), and [metrics](#metrics-table).

The target tables can be either specified explicitly in the `CREATE TABLE` query
or the `TimeSeries` table engine can generate inner target tables automatically.

Rows inserted into a `TimeSeries` table are transformed, split into blocks, and inserted in these three target tables.

The target tables are the following:

### Samples table {#samples-table}

The _samples_ table contains time series associated with some identifier.

The _samples_ table must have columns:

| Name | Mandatory? | Default type | Possible types | Description |
|---|---|---|---|---|
| `id` | [x] | `UUID` | any | Identifies a combination of a metric names and tags |
| `timestamp` | [x] | `DateTime64(3)` | `DateTime64(X)` | A time point |
| `value` | [x] | `Float64` | `Float32` or `Float64` | A value associated with the `timestamp` |

### Tags table {#tags-table}

The _tags_ table contains identifiers calculated for each combination of a metric name and tags.

The _tags_ table must have columns:

| Name | Mandatory? | Default type | Possible types | Description |
|---|---|---|---|---|
| `id` | [x] | `UUID` | any (must match the type of `id` in the [samples](#samples-table) table) | An `id` identifies a combination of a metric name and tags. The DEFAULT expression specifies how to calculate such an identifier |
| `metric_name` | [x] | `LowCardinality(String)` | `String` or `LowCardinality(String)` | The name of a metric |
| `<tag_value_column>` | [ ] | `String` | `String` or `LowCardinality(String)` or `LowCardinality(Nullable(String))` | The value of a specific tag, the tag's name and the name of a corresponding column are specified in the [tags_to_columns](#settings) setting |
| `tags` | [x] | `Map(LowCardinality(String), String)` | `Map(String, String)` or `Map(LowCardinality(String), String)` or `Map(LowCardinality(String), LowCardinality(String))` | Map of tags excluding the tag `__name__` containing the name of a metric and excluding tags with names enumerated in the [tags_to_columns](#settings) setting |
| `all_tags` | [ ] | `Map(String, String)` | `Map(String, String)` or `Map(LowCardinality(String), String)` or `Map(LowCardinality(String), LowCardinality(String))` | Ephemeral column, each row is a map of all the tags excluding only the tag `__name__` containing the name of a metric. The only purpose of that column is to be used while calculating `id` |
| `min_time` | [ ] | `Nullable(DateTime64(3))` | `DateTime64(X)` or `Nullable(DateTime64(X))` | Minimum timestamp of time series with that `id`. The column is created if [store_min_time_and_max_time](#settings) is `true` |
| `max_time` | [ ] | `Nullable(DateTime64(3))` | `DateTime64(X)` or `Nullable(DateTime64(X))` | Maximum timestamp of time series with that `id`. The column is created if [store_min_time_and_max_time](#settings) is `true` |

### Metrics table {#metrics-table}

The _metrics_ table contains some information about metrics been collected, the types of those metrics and their descriptions.

The _metrics_ table must have columns:

| Name | Mandatory? | Default type | Possible types | Description |
|---|---|---|---|---|
| `metric_family_name` | [x] | `String` | `String` or `LowCardinality(String)` | The name of a metric family |
| `type` | [x] | `LowCardinality(String)` | `String` or `LowCardinality(String)` | The type of a metric family, one of "counter", "gauge", "summary", "stateset", "histogram", "gaugehistogram" |
| `unit` | [x] | `LowCardinality(String)` | `String` or `LowCardinality(String)` | The unit used in a metric |
| `help` | [x] | `String` | `String` or `LowCardinality(String)` | The description of a metric |

## Creation {#creation}

There are multiple ways to create a table with the `TimeSeries` table engine.
The simplest statement

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

will actually create the following table (you can see that by executing `SHOW CREATE TABLE my_table`):

```sql
CREATE TABLE my_table
(
    `metric_name` String,
    `tags` Map(String, String),
    `time_series` Array(Tuple(DateTime64(3), Float64)),
    `metric_family` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = TimeSeries
SAMPLES INNER COLUMNS
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp)
TAGS INNER COLUMNS
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
TAGS INNER ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id)
METRICS INNER COLUMNS
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
METRICS INNER ENGINE = ReplacingMergeTree ORDER BY metric_family_name
```

So the columns were generated automatically and also there are three inner target tables with their own column definitions
stored in the `INNER COLUMNS` clauses.

Inner target tables have names like `.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`,
`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, `.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
and each target table has its own set of columns:

```sql
CREATE TABLE default.`.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp)
```

```sql
CREATE TABLE default.`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
ENGINE = AggregatingMergeTree
PRIMARY KEY metric_name
ORDER BY (metric_name, id)
```

```sql
CREATE TABLE default.`.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name
```

## Creating a table AS existing table {#create-as}

Statement `CREATE TABLE new_table AS existing_table` copies from the `existing_table`:

- `SETTINGS`
- `INNER COLUMNS` for each kind
- `INNER ENGINE` for each kind

The statement is not allowed if the `existing_table` has external targets.
The outer column list is regenerated and not copied.

## Adjusting types of columns {#adjusting-column-types}

You can adjust the types of columns in the inner target tables using the `INNER COLUMNS` clause. For example, to store timestamps in microseconds and values as `Float32`:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(6), value Float32)
```

The same clause can be used to specify codecs and other column attributes:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(3) CODEC(DoubleDelta))
```

## The `id` column {#id-column}

The `id` column contains identifiers, every identifier is calculated for a combination of a metric name and tags.
The type and the `DEFAULT` expression used to generate identifiers can be customized via the `TAGS INNER COLUMNS` clause:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
TAGS INNER COLUMNS (id UInt64 DEFAULT sipHash64(metric_name, all_tags))
```

The `id` column type must be one of `UUID`, `UInt64`, `UInt128`, or `FixedString(16)`. If no `DEFAULT` expression is given, ClickHouse will choose it automatically based on the `id` type. The `id` types declared in the samples and tags inner tables must match.

The `id_generator` setting offers the same customization without using the `INNER COLUMNS` clause:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SETTINGS id_generator = 'sipHash64(metric_name, all_tags)'
```

If the setting is set, it's used to generate `id` even if the column's `DEFAULT` contains a different expression.

## The `tags` and `all_tags` columns {#tags-and-all-tags}

There are two columns containing maps of tags - `tags` and `all_tags`. In this example they mean the same, however they can be different
if setting `tags_to_columns` is used. This setting allows to specify that a specific tag should be stored in a separate column instead of storing
in a map inside the `tags` column:

```sql
CREATE TABLE my_table
ENGINE = TimeSeries
SETTINGS tags_to_columns = {'instance': 'instance', 'job': 'job'}
```

This statement will add columns `instance` and `job` to the inner [tags](#tags-table) target table.
In this case the `tags` column will not contain tags `instance` and `job`,
but the `all_tags` column will contain them. The `all_tags` column is ephemeral and its only purpose to be used in the DEFAULT expression
for the `id` column.

## Table engines of inner target tables {#inner-table-engines}

By default inner target tables use the following table engines:
- the [samples](#samples-table) table uses [MergeTree](../mergetree-family/mergetree);
- the [tags](#tags-table) table uses [AggregatingMergeTree](../mergetree-family/aggregatingmergetree) because the same data is often inserted multiple times to this table so we need a way
to remove duplicates, and also because it's required to do aggregation for columns `min_time` and `max_time`;
- the [metrics](#metrics-table) table uses [ReplacingMergeTree](../mergetree-family/replacingmergetree) because the same data is often inserted multiple times to this table so we need a way
to remove duplicates.

Other table engines also can be used for inner target tables if it's specified so:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES ENGINE=ReplicatedMergeTree
TAGS ENGINE=ReplicatedAggregatingMergeTree
METRICS ENGINE=ReplicatedReplacingMergeTree
```

## External target tables {#external-target-tables}

It's possible to make a `TimeSeries` table use a manually created table:

```sql
CREATE TABLE samples_for_my_table
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp);

CREATE TABLE tags_for_my_table ...

CREATE TABLE metrics_for_my_table ...

CREATE TABLE my_table ENGINE=TimeSeries SAMPLES samples_for_my_table TAGS tags_for_my_table METRICS metrics_for_my_table;
```

The external tables' column types (`id`, `timestamp`, `value`, and the `<tag_value_column>`s listed in [`tags_to_columns`](#settings)) must match what the `TimeSeries` table would otherwise generate internally (see [Samples table](#samples-table), [Tags table](#tags-table), and [Metrics table](#metrics-table) for the type constraints). Type mismatches are reported at `CREATE` time.

The id-generator expression for an external tags target is resolved at INSERT time in the following order: the [`id_generator`](#settings) setting (if set), then the `DEFAULT` declared on the external table's `id` column (if any), then the canonical generator derived from the `id` type. The setting therefore overrides whatever `DEFAULT` is declared on the external table — see [The `id` column](#id-column) for details.

## Altering settings {#altering-settings}

Two settings can be changed after `CREATE`:

- `id_generator`
- `filter_by_min_time_and_max_time`

```sql
ALTER TABLE my_table MODIFY SETTING id_generator = 'sipHash64(metric_name, all_tags)';
ALTER TABLE my_table MODIFY SETTING filter_by_min_time_and_max_time = 0;
```

Note that changing `id_generator` while data is already in the tags table can produce different IDs for the same metric+tag combination — old rows keep their old IDs, new rows use the new generator.

The other settings can't be changed with `ALTER ... MODIFY SETTING` because they are baked into the schema of the inner tables at `CREATE` time.

## Settings {#settings}

Here is a list of settings which can be specified while defining a `TimeSeries` table:

| Name | Type | Default | Description |
|---|---|---|---|
| `id_generator` | Expression | depends on `id` type | Expression that computes the identifier (fingerprint) of a time series from its tags. If unset, the default expression for the `id` column is used. If the default expression for the `id` column is also unset then the expression is chosen automatically |
| `tags_to_columns` | Map | {} | Map specifying which tags should be put to separate columns in the [tags](#tags-table) table. Syntax: `{'tag1': 'column1', 'tag2' : column2, ...}` |
| `use_all_tags_column_to_generate_id` | Bool | true | When generating an expression to calculate an identifier of a time series, this flag enables using the `all_tags` column in that calculation |
| `store_min_time_and_max_time` | Bool | true | If set to true then the table will store `min_time` and `max_time` for each time series |
| `aggregate_min_time_and_max_time` | Bool | true | When creating an inner target `tags` table, this flag enables using `SimpleAggregateFunction(min, Nullable(DateTime64(3)))` instead of just `Nullable(DateTime64(3))` as the type of the `min_time` column, and the same for the `max_time` column |
| `filter_by_min_time_and_max_time` | Bool | true | If set to true then the table will use the `min_time` and `max_time` columns for filtering time series |

# Functions {#functions}

Here is a list of functions supporting a `TimeSeries` table as an argument:
- [timeSeriesSamples](../../../sql-reference/table-functions/timeSeriesSamples.md)
- [timeSeriesTags](../../../sql-reference/table-functions/timeSeriesTags.md)
- [timeSeriesMetrics](../../../sql-reference/table-functions/timeSeriesMetrics.md)
)DOCS_MD",
        .syntax = "ENGINE = TimeSeries()"});
}

}
