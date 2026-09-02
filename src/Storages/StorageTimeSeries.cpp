#include <Storages/StorageTimeSeries.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/IBackup.h>
#include <Backups/RestorerFromBackup.h>
#include <Storages/AlterCommands.h>
#include <Storages/StorageFactory.h>
#include <Storages/TimeSeries/TimeSeriesSink.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/TimeSeries/createTimeSeriesInnerTable.h>
#include <Storages/TimeSeries/makeASTSelectFromTimeSeries.h>
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
    extern const int TABLE_ALREADY_EXISTS;
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
        /// The recent samples target exists only if the normalized create query has a RECENT SAMPLES clause.
        /// The `recent_samples_ttl_seconds` setting itself cannot be checked here instead: a table created
        /// before this feature existed has no recent samples table on disk while the setting reads as its
        /// non-zero default, and ATTACH never creates inner tables.
        if ((target_kind == ViewTarget::RecentSamples)
            && (!create_query.targets || !create_query.targets->tryGetTarget(target_kind)))
            continue;

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
                /// The normalization always sets them; a query with an inner UUID but no inner columns
                /// can only come from hand-edited metadata.
                auto * inner_columns = create_query.getTargetInnerColumns(target_kind);
                if (!inner_columns)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "The {} target of table {} has no inner columns",
                        magic_enum::enum_name(target_kind), table_id.getNameForLogs());
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


const StorageTimeSeries::Target * StorageTimeSeries::tryGetTarget(ViewTarget::Kind target_kind) const
{
    for (const auto & target : targets)
    {
        if (target.kind == target_kind)
            return &target;
    }
    return nullptr;
}


bool StorageTimeSeries::hasTarget(ViewTarget::Kind target_kind) const
{
    return tryGetTarget(target_kind) != nullptr;
}


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
    const auto * target_ptr = tryGetTarget(target_kind);
    if (!target_ptr)
    {
        /// The recent samples target is optional.
        if (target_kind == ViewTarget::RecentSamples)
        {
            if (throw_if_not_found)
                throw Exception(ErrorCodes::UNKNOWN_TABLE, "TimeSeries table {} has no {} target table",
                                getStorageID().getNameForLogs(), target_kind);
            return nullptr;
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected target kind {}", target_kind);
    }
    const auto & target = *target_ptr;

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
    const auto * target = tryGetTarget(target_kind);
    if (!target)
    {
        /// The recent samples target is optional.
        if (target_kind == ViewTarget::RecentSamples)
            return false;
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected target kind {}", target_kind);
    }
    return target->is_inner_table;
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
                /// DDLGuards must be locked in order of increasing table name, so the inner guard
                /// may be requested only when this table's name sorts first.
                bool may_lock_ddl_guard = getStorageID().getQualifiedName() < inner_table_id.getQualifiedName();
                InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, getContext(), local_context, inner_table_id,
                                                    sync, /* ignore_sync_setting= */ true, may_lock_ddl_guard);
            }
        }
    }
}

void StorageTimeSeries::checkTableSizeBelowDropLimit(ContextPtr query_context) const
{
    if (!hasInnerTables())
        return;

    for (auto target_kind : getTargetKinds())
    {
        if (!isInnerTable(target_kind))
            continue;

        if (auto inner_table = tryGetTargetTable(target_kind, query_context))
            inner_table->checkTableSizeBelowDropLimit(query_context);
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


/// TODO: Return the row count of the inner "tags" table instead of the sum over all the inner tables:
/// it matches `SELECT count()` without FINAL, allowing the trivial count optimization.
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
            const auto inner_metadata = inner_table->getInMemoryMetadataPtr(local_context, false);
            optimized |= inner_table->optimize(query, inner_metadata, partition, final, deduplicate, deduplicate_by_columns, cleanup, local_context);
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
    auto metadata_snapshot = getInMemoryMetadataPtr(local_context, false);
    StorageInMemoryMetadata new_metadata = *metadata_snapshot;
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


void StorageTimeSeries::renameInMemory(const StorageID & new_table_id)
{
    auto old_table_id = getStorageID();

    /// In an Atomic/Replicated database both ids carry a UUID; inner tables are addressed by the
    /// outer table's UUID (the `.inner_id.<kind>.<uuid>` name), which is preserved by the rename, so
    /// only the outer table id changes. In an Ordinary database the inner table names embed the old
    /// outer table name, so each inner table has to be renamed too (same as StorageMaterializedView).
    bool from_atomic_to_atomic_database = old_table_id.hasUUID() && new_table_id.hasUUID();

    if (!from_atomic_to_atomic_database && hasInnerTables())
    {
        /// Collect every inner table rename first so that all destination names can be checked
        /// before any rename is executed. The inner renames are not transactional, so renaming
        /// them one by one would leave the table half-renamed (some inner tables moved, the rest
        /// not) if a later destination name happened to be occupied.
        std::vector<std::pair<StorageID, String>> inner_renames;
        for (auto target_kind : getTargetKinds())
        {
            if (!isInnerTable(target_kind))
                continue;

            auto inner_table = tryGetTargetTable(target_kind, getContext());
            if (!inner_table)
                continue;

            auto inner_table_id = inner_table->getStorageID();
            auto new_inner_table_name = getTimeSeriesInnerTableName(target_kind, new_table_id);

            if (DatabaseCatalog::instance().isTableExist(StorageID{new_table_id.database_name, new_inner_table_name}, getContext()))
                throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table {} already exists",
                                StorageID{new_table_id.database_name, new_inner_table_name}.getNameForLogs());

            inner_renames.emplace_back(std::move(inner_table_id), std::move(new_inner_table_name));
        }

        for (const auto & [inner_table_id, new_inner_table_name] : inner_renames)
        {
            auto rename = make_intrusive<ASTRenameQuery>();
            rename->addElement(inner_table_id.database_name, inner_table_id.table_name,
                               new_table_id.database_name, new_inner_table_name);
            InterpreterRenameQuery(rename, getContext()).execute();
        }
    }

    IStorage::renameInMemory(new_table_id);
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
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & /* storage_snapshot */,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    size_t /* num_streams */)
{
    /// Run the generated read query on a child context with a few settings pinned so its results
    /// don't depend on the caller's session/profile (see getSettingsForSelectFromTimeSeries).
    auto read_context = Context::createCopy(local_context);
    read_context->applySettingsChanges(getSettingsForSelectFromTimeSeries(query_info.isFinal()));

    NameSet requested_columns{column_names.begin(), column_names.end()};
    auto select_query = makeASTSelectFromTimeSeries(*this, requested_columns, query_info, read_context);
    auto options = SelectQueryOptions(QueryProcessingStage::Complete, /* subquery_depth_ = */ 0, /* is_subquery_ = */ false,
                                      query_info.settings_limit_offset_done);
    InterpreterSelectQueryAnalyzer interpreter(select_query, read_context, options, column_names);
    interpreter.addStorageLimits(*query_info.storage_limits);
    query_plan = std::move(interpreter).extractQueryPlan();
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
with [allow_experimental_time_series_table](/reference/settings/session-settings/allow-experimental#allow_experimental_time_series_table) setting.
Input the command `set allow_experimental_time_series_table = 1`.
:::

## Syntax {#syntax}

```sql
CREATE TABLE name [(columns)] ENGINE=TimeSeries
[SETTINGS var1=value1, ...]
[SAMPLES db.samples_table_name | [SAMPLES INNER COLUMNS (...)] [SAMPLES INNER ENGINE engine(arguments)]]
[RECENT SAMPLES db.recent_samples_table_name | [RECENT SAMPLES INNER COLUMNS (...)] [RECENT SAMPLES INNER ENGINE engine(arguments)]]
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
- [prometheus remote-write](/concepts/features/interfaces/prometheus#remote-write)
- [prometheus remote-read](/concepts/features/interfaces/prometheus#remote-read)

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
SAMPLES INNER COLUMNS (timestamp UInt32 CODEC(DoubleDelta, ZSTD(1)), value Float32 CODEC(ZSTD(3)))
```

If both forms are used in the same `CREATE TABLE` statement, the declared types must match.

## Target tables {#target-tables}

A `TimeSeries` table doesn't have its own data, everything is stored in its target tables.
This is similar to how a [materialized view](/reference/statements/create/view#materialized-view) works,
with the difference that a materialized view has one target table
whereas a `TimeSeries` table has three mandatory target tables named [samples](#samples-table), [tags](#tags-table), and [metrics](#metrics-table),
and an optional [recent samples](#recent-samples-table) target table which is enabled by default
(see the [recent_samples_ttl_seconds](#settings) setting).

The target tables can be either specified explicitly in the `CREATE TABLE` query
or the `TimeSeries` table engine can generate inner target tables automatically.

Rows inserted into a `TimeSeries` table are transformed, split into blocks, and inserted in these target tables.

The target tables are the following:

### Samples table {#samples-table}

The _samples_ table contains time series associated with some identifier.

The _samples_ table must have columns:

| Name | Mandatory? | Default type | Possible types | Description |
|---|---|---|---|---|
| `id` | [x] | `Tuple(UInt64, LowCardinality(UUID))` | any | Identifies a combination of a metric names and tags |
| `timestamp` | [x] | `DateTime64(3)` | `DateTime64(X)` | A time point |
| `value` | [x] | `Float64` | `Float32` or `Float64` | A value associated with the `timestamp` |

Columns the engine creates itself get time-series compression codecs:
`timestamp CODEC(DoubleDelta, ZSTD(1))` and `value CODEC(ZSTD(3))`. Near-monotonic timestamps barely
compress under generic codecs and can otherwise dominate the on-disk size of the samples table.
See also [Adjusting types of columns](#adjusting-column-types).

### Recent samples table {#recent-samples-table}

The _recent samples_ table is optional and enabled by default (see the [recent_samples_ttl_seconds](#settings) setting;
setting it to zero disables the table). It contains a copy of the samples newer than the TTL defined by that setting,
and it must have the same columns as the [samples](#samples-table) table.

Every inserted sample is written both to the samples table and to the recent samples table.
Queries whose time range fits in the TTL window read from the recent samples table instead of the main samples table
because it's much smaller (this can be disabled with the query-level setting `time_series_prefer_recent_samples_table`).

The TTL of the inner recent samples table is always derived from the [recent_samples_ttl_seconds](#settings) setting.

### Tags table {#tags-table}

The _tags_ table contains identifiers calculated for each combination of a metric name and tags.

The _tags_ table must have columns:

| Name | Mandatory? | Default type | Possible types | Description |
|---|---|---|---|---|
| `id` | [x] | `Tuple(UInt64, LowCardinality(UUID))` | any (must match the type of `id` in the [samples](#samples-table) table) | An `id` identifies a combination of a metric name and tags. The DEFAULT expression specifies how to calculate such an identifier |
| `metric_name` | [x] | `LowCardinality(String)` | `String` or `LowCardinality(String)` | The name of a metric |
| `<tag_value_column>` | [ ] | `String` | `String` or `LowCardinality(String)` or `LowCardinality(Nullable(String))` | The value of a specific tag, the tag's name and the name of a corresponding column are specified in the [tags_to_columns](#settings) setting |
| `tags` | [x] | `Map(LowCardinality(String), String)` | `Map(String, String)` or `Map(LowCardinality(String), String)` or `Map(LowCardinality(String), LowCardinality(String))` | Map of all the tags, including the tag `__name__` containing the name of a metric and including the tags with names enumerated in the [tags_to_columns](#settings) setting. Tables created by older versions of ClickHouse stored in this column only the tags without dedicated columns and without the metric name; reading handles both cases |
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
SETTINGS recent_samples_ttl_seconds = 345600
SAMPLES INNER COLUMNS
(
    `id` Tuple(UInt64, LowCardinality(UUID)),
    `timestamp` DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
    `value` Float64 CODEC(ZSTD(3))
)
SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp) SETTINGS index_granularity = 32768
RECENT SAMPLES INNER COLUMNS
(
    `id` Tuple(UInt64, UUID),
    `timestamp` DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
    `value` Float64 CODEC(ZSTD(3))
)
RECENT SAMPLES INNER ENGINE = MergeTree PARTITION BY toStartOfInterval(toDateTime(timestamp), toIntervalHour(5)) ORDER BY (id, timestamp) TTL toDateTime(timestamp) + toIntervalSecond(345600) SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
TAGS INNER COLUMNS
(
    `id` Tuple(UInt64, LowCardinality(UUID)) DEFAULT tuple(sipHash64(metric_name), toLowCardinality(reinterpretAsUUID(sipHash128(tags)))),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
TAGS INNER ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1, index_granularity = 8192
METRICS INNER COLUMNS
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
METRICS INNER ENGINE = ReplacingMergeTree ORDER BY metric_family_name
```

So the columns were generated automatically and also there are four inner target tables with their own column definitions
stored in the `INNER COLUMNS` clauses. The `recent_samples_ttl_seconds` setting was written into the `SETTINGS` clause
with its default value: the setting defines the TTL of the recent samples table, so its effective value is fixed at creation.

Inner target tables have names like `.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`,
`.inner_id.recentsamples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, `.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`,
`.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
and each target table has its own set of columns:

```sql
CREATE TABLE default.`.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` Tuple(UInt64, LowCardinality(UUID)),
    `timestamp` DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
    `value` Float64 CODEC(ZSTD(3))
)
ENGINE = MergeTree
ORDER BY (id, timestamp)
SETTINGS index_granularity = 32768
```

```sql
CREATE TABLE default.`.inner_id.recentsamples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` Tuple(UInt64, UUID),
    `timestamp` DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
    `value` Float64 CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toStartOfInterval(toDateTime(timestamp), toIntervalHour(5))
ORDER BY (id, timestamp)
TTL toDateTime(timestamp) + toIntervalSecond(345600)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
```

```sql
CREATE TABLE default.`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` Tuple(UInt64, LowCardinality(UUID)) DEFAULT tuple(sipHash64(metric_name), toLowCardinality(reinterpretAsUUID(sipHash128(tags)))),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
ENGINE = AggregatingMergeTree
PRIMARY KEY metric_name
ORDER BY (metric_name, id)
SETTINGS allow_dimensions_outside_sorting_key = 1, index_granularity = 8192
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
SETTINGS index_granularity = 8192
```

## Creating a table AS existing table {#create-as}

Statement `CREATE TABLE new_table AS existing_table` copies from the `existing_table`:

- `SETTINGS`
- `INNER COLUMNS` for each kind
- `INNER ENGINE` for each kind

The statement is not allowed if the `existing_table` has external targets.
The outer column list is regenerated and not copied.

## Adjusting types of columns {#adjusting-column-types}

You can adjust the types of columns in the inner target tables using the `INNER COLUMNS` clause. For example, to store timestamps in microseconds and values as `Float32` use:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(6) CODEC(DoubleDelta, ZSTD(1)), value Float32 CODEC(ZSTD(3)))
```

Specifying inner columns without codecs means using the default codec for them:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(6), value Float32)
```

## The `id` column {#id-column}

The `id` column contains identifiers, every identifier is calculated for a combination of a metric name and tags.
The type and the `DEFAULT` expression used to generate identifiers can be customized via the `TAGS INNER COLUMNS` clause:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
TAGS INNER COLUMNS (id UInt64 DEFAULT sipHash64(tags))
```

The `id` column can be of any comparable non-Nullable type. The `id` types declared in the samples and tags inner tables must match.

If no `DEFAULT` expression is given for the `id` column and the `id_generator` setting is not set, ClickHouse will choose the `DEFAULT` expression automatically based on the `id` type, but only if the `id` type is one of `UUID`, `UInt64`, `UInt128`, `FixedString(16)`, the same types wrapped in `LowCardinality`, or a tuple of two of those types. For such a tuple the automatically chosen expression calculates a hash of the metric name in the first component and a hash of all the tags in the second component.

A `LowCardinality` identifier type, e.g. `Tuple(UInt64, LowCardinality(UUID))`, keeps the identifiers dictionary-encoded: the samples table stores small per-block dictionaries with dictionary indexes instead of repeating the full identifier in every row, which reduces the amount of data read by queries.

The `id_generator` setting offers the same customization without using the `INNER COLUMNS` clause:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SETTINGS id_generator = 'sipHash64(tags)'
```

If the setting is set, it's used to generate `id` even if the column's `DEFAULT` contains a different expression.

## The `tags` column {#tags-column}

The `tags` column contains all the tags of a time series, including the `__name__` tag with the name of a metric.

The `tags_to_columns` setting allows to specify that a specific tag should also be stored in a separate column
in addition to the map inside the `tags` column:

```sql
CREATE TABLE my_table
ENGINE = TimeSeries
SETTINGS tags_to_columns = {'instance': 'instance', 'job': 'job'}
```

This statement will add columns `instance` and `job` to the inner [tags](#tags-table) target table.
The values of the tags `instance` and `job` will be stored both in those columns and in the `tags` column.

:::note
In tables created by older versions of ClickHouse the `tags` column contains only the tags without dedicated
columns and without the metric name, and the `all_tags` column is an ephemeral column which was filled on insertion
with all the tags except the metric name.
:::

## Table engines of inner target tables {#inner-table-engines}

By default inner target tables use the following table engines:
- the [samples](#samples-table) table uses [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree);
- the [recent samples](#recent-samples-table) table uses [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) partitioned by 5-hour buckets (see the [recent_samples_partition_by](#settings) setting) with a `TTL` derived from
the [recent_samples_ttl_seconds](#settings) setting and with `ttl_only_drop_parts` enabled, so expired parts are dropped as a whole;
- the [tags](#tags-table) table uses [AggregatingMergeTree](/reference/engines/table-engines/mergetree-family/aggregatingmergetree) because the same data is often inserted multiple times to this table so we need a way
to remove duplicates, and also because it's required to do aggregation for columns `min_time` and `max_time`;
- the [metrics](#metrics-table) table uses [ReplacingMergeTree](/reference/engines/table-engines/mergetree-family/replacingmergetree) because the same data is often inserted multiple times to this table so we need a way
to remove duplicates.

The engine family of the generated inner tables follows the `default_table_engine` query-level setting:
with `default_table_engine = ReplicatedMergeTree` or `SharedMergeTree` the inner tables use the corresponding
`Replicated` or `Shared` engines. With `default_table_engine = None` (or any other value) the engines of the inner tables
must be specified explicitly.

All the inner tables must have the same replication type: if one of them is replicated (or shared), the other inner
tables must be replicated (or shared) too, otherwise their contents would diverge between replicas. For example,
declaring `SAMPLES INNER ENGINE = ReplicatedMergeTree(...)` requires the other inner engines to be replicated as well -
either declared explicitly or generated with `default_table_engine = ReplicatedMergeTree`.

Other table engines also can be used for inner target tables if it's specified so:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES ENGINE=ReplicatedMergeTree
RECENT SAMPLES ENGINE=ReplicatedMergeTree
TAGS ENGINE=ReplicatedAggregatingMergeTree
METRICS ENGINE=ReplicatedReplacingMergeTree
```

The [tags](#tags-table) table keeps the tag columns (and the `tags` Map) outside its sorting key,
which `AggregatingMergeTree` rejects by default (see [`allow_dimensions_outside_sorting_key`](/reference/engines/table-engines/mergetree-family/aggregatingmergetree)).
This is safe here because those columns are functionally dependent on `id`, which is part of the sorting key, so all
rows that a background merge collapses together share the same values. When the inner tags table is generated or its
engine is specified inline as above, `TimeSeries` sets `allow_dimensions_outside_sorting_key = 1` on it automatically;
for a manually created [external](#external-target-tables) aggregating tags table you must set it yourself.

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

An external table can also be used as the [recent samples](#recent-samples-table) target (the `RECENT SAMPLES my_recent_samples_table` clause).
Such a table must have the same columns as an external samples table, and it must retain at least
[recent_samples_ttl_seconds](#settings) seconds of data, which is the user's responsibility.

The external tables' column types (`id`, `timestamp`, `value`, and the `<tag_value_column>`s listed in [`tags_to_columns`](#settings)) must match what the `TimeSeries` table would otherwise generate internally (see [Samples table](#samples-table), [Tags table](#tags-table), and [Metrics table](#metrics-table) for the type constraints). Type mismatches are reported at `CREATE` time.

The id-generator expression for an external tags target is resolved at INSERT time in the following order: the [`id_generator`](#settings) setting (if set), then the `DEFAULT` declared on the external table's `id` column (if any), then the canonical generator derived from the `id` type. The setting therefore overrides whatever `DEFAULT` is declared on the external table — see [The `id` column](#id-column) for details.

## Altering settings {#altering-settings}

Two settings can be changed after `CREATE`:

- `id_generator`
- `filter_by_min_time_and_max_time`

```sql
ALTER TABLE my_table MODIFY SETTING id_generator = 'sipHash64(tags)';
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
| `use_all_tags_column_to_generate_id` | Bool | false | Obsolete setting, does nothing |
| `store_min_time_and_max_time` | Bool | true | If set to true then the table will store `min_time` and `max_time` for each time series |
| `aggregate_min_time_and_max_time` | Bool | true | When creating an inner target `tags` table, this flag enables using `SimpleAggregateFunction(min, Nullable(DateTime64(3)))` instead of just `Nullable(DateTime64(3))` as the type of the `min_time` column, and the same for the `max_time` column |
| `filter_by_min_time_and_max_time` | Bool | true | If set to true then the table will use the `min_time` and `max_time` columns for filtering time series |
| `samples_index_granularity` | UInt64 | 32768 | Sets `index_granularity` of the inner [samples](#samples-table) table. When set explicitly, it overrides `index_granularity` from the engine declaration. Ignored for an external samples table and a non-MergeTree engine |
| `recent_samples_ttl_seconds` | UInt64 | 345600 | Retention of the additional `recent samples` target table, which every inserted sample is written to as well. An inner recent samples table always gets `TTL toDateTime(timestamp) + toIntervalSecond(recent_samples_ttl_seconds)` derived from this setting (overriding any TTL from the engine declaration); an external recent samples table must retain at least this many seconds of data. Queries whose time range fits in the TTL window prefer the recent samples table to the main samples table (see the query-level setting `time_series_prefer_recent_samples_table`). The default is 4 days; the effective value is pinned into the table definition at CREATE time. Set to 0 to disable the recent samples table |
| `recent_samples_partition_by` | Expression | `toStartOfInterval(toDateTime(timestamp), toIntervalHour(5))` | Partition key of the inner `recent samples` table, for example `toStartOfHour(timestamp)`. When set explicitly, it overrides the partition key from the engine declaration; if neither is set, one partition per 5 hours is used. Ignored for an external recent samples table. Requires `recent_samples_ttl_seconds` to be non-zero |
| `recent_samples_index_granularity` | UInt64 | 8192 | Sets `index_granularity` of the inner `recent samples` table. When set explicitly, it overrides `index_granularity` from the engine declaration. Ignored for an external recent samples table and a non-MergeTree engine. Requires `recent_samples_ttl_seconds` to be non-zero |
| `tags_index_granularity` | UInt64 | 8192 | Sets `index_granularity` of the inner [tags](#tags-table) table. When set explicitly, it overrides `index_granularity` from the engine declaration. Ignored for an external tags table and a non-MergeTree engine |

# Functions {#functions}

Here is a list of functions supporting a `TimeSeries` table as an argument:
- [timeSeriesSamples](/reference/functions/table-functions/timeSeriesSamples)
- [timeSeriesTags](/reference/functions/table-functions/timeSeriesTags)
- [timeSeriesMetrics](/reference/functions/table-functions/timeSeriesMetrics)
)DOCS_MD",
        .syntax = "ENGINE = TimeSeries()"});
}

}
