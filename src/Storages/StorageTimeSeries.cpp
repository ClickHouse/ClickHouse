#include <Storages/StorageTimeSeries.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
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
    const ASTCreateQuery & query,
    const ColumnsDescription & /*columns*/,
    const String & comment)
    : StorageWithCommonVirtualColumns(table_id)
    , WithContext(local_context->getGlobalContext())
    , initial_create_query(boost::static_pointer_cast<const ASTCreateQuery>(query.clone()))
    , storage_settings(std::make_unique<const TimeSeriesSettings>(getNormalizedTimeSeriesSettings(*initial_create_query, local_context)))
    , targets(buildTargets(*initial_create_query, table_id, local_context, mode))
    , has_inner_tables(std::ranges::any_of(targets, &Target::is_inner_table))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(generateTimeSeriesColumns(*storage_settings.get()));
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
        if (!command.isCommentAlter() && command.type != AlterCommand::MODIFY_SQL_SECURITY
            && !command.isSettingsAlter())
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
        new_settings = std::make_unique<TimeSeriesSettings>(getNormalizedTimeSeriesSettings(
            *initial_create_query, local_context, new_metadata.settings_changes->as<const ASTSetQuery &>().changes));

        auto settings_ast = make_intrusive<ASTSetQuery>();
        settings_ast->is_standalone = false;
        settings_ast->changes = new_settings->changes();
        new_metadata.settings_changes = settings_ast;

        new_metadata.setColumns(generateTimeSeriesColumns(*new_settings));
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
            args.table_id, args.getLocalContext(), args.mode,
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
