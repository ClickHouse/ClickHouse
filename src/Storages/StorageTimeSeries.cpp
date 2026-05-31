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

    /// We allow altering only settings that do not affect inner-table schemas.
    void checkSettingCanBeAltered(std::string_view setting_name, std::string_view storage_name)
    {
        if ((setting_name != "id_generator")
            && (setting_name != "filter_by_min_time_and_max_time")
            && (setting_name != "prometheus_remote_write_dynamic_routing_enabled"))
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
    });
}

}
