#include <DataTypes/DataTypeNullable.h>
#include "config.h"

#if USE_PARQUET && USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableChanges.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakeSink.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakePartitionedSink.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/WriteTransaction.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/transformTypesRecursively.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Common/logger_useful.h>
#include <Common/assert_cast.h>
#include <Common/FailPoint.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Interpreters/DeltaMetadataLog.h>

namespace CurrentMetrics
{
    extern const Metric DeltaLakeSnapshotCacheSizeElements;
};

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char delta_lake_metadata_iterate_pause[];
}

namespace Setting
{
    extern const SettingsBool delta_lake_log_metadata;
    extern const SettingsBool allow_experimental_delta_lake_writes;
    extern const SettingsBool delta_lake_reload_schema_for_consistency;
    extern const SettingsInt64 delta_lake_snapshot_start_version;
    extern const SettingsInt64 delta_lake_snapshot_end_version;
    extern const SettingsInt64 delta_lake_snapshot_version;
}

void tracingCallback(struct ffi::Event event)
{
    /// Do not pollute logs with very long messages
    if (event.message.len > 200)
        return;

    const auto message = fmt::format(
        "Message: {}, code line: {}:{}, target: {}",
        std::string_view(event.message.ptr, event.message.len),
        DeltaLake::KernelUtils::fromDeltaString(event.file),
        event.line,
        DeltaLake::KernelUtils::fromDeltaString(event.target));

    auto log = getLogger("DeltaKernelTracing");
    if (event.level == ffi::Level::ERROR)
    {
        LOG_ERROR(log, "{}", message);
    }
    else if (event.level == ffi::Level::WARN)
    {
        LOG_WARNING(log, "{}", message);
    }
    else if (event.level == ffi::Level::INFO)
    {
        LOG_INFO(log, "{}", message);
    }
    else if (event.level == ffi::Level::DEBUG)
    {
        LOG_DEBUG(log, "{}", message);
    }
    else if (event.level == ffi::Level::TRACE)
    {
        LOG_TRACE(log, "{}", message);
    }
}

static constexpr auto deltalake_metadata_directory = "_delta_log";
static constexpr auto metadata_file_suffix = ".json";

namespace
{

std::optional<size_t> extractDeltaLakeSnapshotVersionFromMetadata(StorageMetadataPtr storage_metadata)
{
    if (!storage_metadata || !storage_metadata->datalake_table_state.has_value())
        return std::nullopt;

    if (!std::holds_alternative<DeltaLake::TableStateSnapshot>(storage_metadata->datalake_table_state.value()))
        return std::nullopt;

    const auto & state = std::get<DeltaLake::TableStateSnapshot>(storage_metadata->datalake_table_state.value());
    return state.version;
}

}

DeltaLakeMetadataDeltaKernel::DeltaLakeMetadataDeltaKernel(
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationWeakPtr configuration_)
    : log(getLogger("DeltaLakeMetadata"))
    , kernel_helper(DB::getKernelHelper(configuration_.lock(), object_storage_))
    , object_storage(object_storage_)
    , format_name(configuration_.lock()->format)
    /// TODO: Supports size limit, not just elements limit.
    /// TODO: Support weight function (by default weight = 1 for all elements).
    /// TODO: Add a setting for cache size.
    ///       At the moment leave it as a small value (10),
    ///       because in most cases users only use latest snapshot version.
    , snapshots(
        CurrentMetrics::end(),
        CurrentMetrics::DeltaLakeSnapshotCacheSizeElements,
        /* max_size_in_bytes */10,
        /* max_count */10)
{
}


static std::optional<DeltaLakeMetadataDeltaKernel::SnapshotVersion>
getSnapshotVersion(const Settings & settings)
{
    const auto & value = settings[Setting::delta_lake_snapshot_version].value;
    if (value >= 0)
        return static_cast<UInt64>(value);

    if (value == DeltaLake::TableSnapshot::LATEST_SNAPSHOT_VERSION)
        return std::nullopt;

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Incorrect delta_lake_snapshot_version setting value: {}. "
        "Expected value >= -1 "
        "(-1 is latest snapshot version, value >= 0 is a specific snapshot version)",
        value);
}

DeltaLake::TableSnapshotPtr
DeltaLakeMetadataDeltaKernel::getTableSnapshot(std::optional<SnapshotVersion> version) const
{
    std::lock_guard lock(snapshots_mutex);

    /// Fallback to latest_snapshot_version.
    /// In case we needed a newer version - update() must
    /// have been called to reload latest_snapshot_version.
    std::optional<SnapshotVersion> result_snapshot_version = version.has_value()
        ? version
        : latest_snapshot_version;

    auto snapshot_creator = [&]()
    {
        /// Constructor itself is lightweight.
        return std::make_shared<DeltaLake::TableSnapshot>(
            result_snapshot_version,
            kernel_helper,
            object_storage,
            log);
    };

    DeltaLake::TableSnapshotPtr snapshot;
    bool created = true;
    if (result_snapshot_version.has_value())
    {
        std::tie(snapshot, created) = snapshots.getOrSet(
            result_snapshot_version.value(), std::move(snapshot_creator));
    }
    else
    {
        snapshot = snapshot_creator();
        latest_snapshot_version = snapshot->getVersion();
        snapshots.set(latest_snapshot_version.value(), snapshot);
    }

    LOG_TEST(
        log, "Using snapshot version: {}, latest loaded snapshot version: {}, reused cached snapshot: {}",
        result_snapshot_version.has_value() ? result_snapshot_version.value() : snapshot->getVersion(),
        latestSnapshotVersionToStr(), !created);

    return snapshot;
}

bool DeltaLakeMetadataDeltaKernel::operator ==(const IDataLakeMetadata & metadata) const
{
    const auto & delta_lake_metadata = dynamic_cast<const DeltaLakeMetadataDeltaKernel &>(metadata);
    return getTableSnapshot()->getVersion() == delta_lake_metadata.getTableSnapshot()->getVersion();
}

std::optional<size_t> DeltaLakeMetadataDeltaKernel::totalRows(ContextPtr context) const
{
    const auto & settings = context->getSettingsRef();
    if (auto start_version = settings[Setting::delta_lake_snapshot_start_version].value;
        start_version != DeltaLake::TableSnapshot::LATEST_SNAPSHOT_VERSION)
    {
        /// TODO: Support total rows/bytes for CDF.
        return std::nullopt;
    }

    auto snapshot_version = getSnapshotVersion(settings);
    try
    {
        return getTableSnapshot(snapshot_version)->getTotalRows();
    }
    catch (...)
    {
        DB::tryLogCurrentException(
            log, "Failed to get total rows for Delta Lake table at location " + kernel_helper->getTableLocation());
        return std::nullopt;
    }
}

std::optional<size_t> DeltaLakeMetadataDeltaKernel::totalBytes(ContextPtr context) const
{
    const auto & settings = context->getSettingsRef();
    if (auto start_version = settings[Setting::delta_lake_snapshot_start_version].value;
        start_version != DeltaLake::TableSnapshot::LATEST_SNAPSHOT_VERSION)
    {
        /// TODO: Support total rows/bytes for CDF.
        return std::nullopt;
    }

    auto snapshot_version = getSnapshotVersion(settings);
    try
    {
        return getTableSnapshot(snapshot_version)->getTotalBytes();
    }
    catch (...)
    {
        DB::tryLogCurrentException(
            log, "Failed to get total bytes for Delta Lake table at location " + kernel_helper->getTableLocation());
        return std::nullopt;
    }
}

void DeltaLakeMetadataDeltaKernel::update(const ContextPtr & context)
{
    const auto snapshot_version = getSnapshotVersion(context->getSettingsRef());
    if (!snapshot_version.has_value())
    {
        std::lock_guard lock(snapshots_mutex);
        auto latest_snapshot = std::make_shared<DeltaLake::TableSnapshot>(
                /* version */std::nullopt,
                kernel_helper,
                object_storage,
                log);

        size_t version = latest_snapshot->getVersion();
        snapshots.getOrSet(version, [&]() { return latest_snapshot; });

        LOG_TEST(
            log, "Updating latest snapshot version from {} to {}",
            latestSnapshotVersionToStr(), version);

        latest_snapshot_version = version;
    }
}

DeltaLake::TableChangesPtr DeltaLakeMetadataDeltaKernel::getTableChanges(
    const DeltaLake::TableChangesVersionRange & version_range,
    const Block & header,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context) const
{
    return std::make_shared<DeltaLake::TableChanges>(version_range, kernel_helper, header, format_settings, format_name, context);
}

std::optional<DataLakeTableStateSnapshot> DeltaLakeMetadataDeltaKernel::getTableStateSnapshot(ContextPtr context) const
{
    const auto snapshot_version = getSnapshotVersion(context->getSettingsRef());
    auto snapshot = getTableSnapshot(snapshot_version);

    DeltaLake::TableStateSnapshot state;
    state.version = snapshot->getVersion();
    return DataLakeTableStateSnapshot{state};
}

std::unique_ptr<StorageInMemoryMetadata> DeltaLakeMetadataDeltaKernel::buildStorageMetadataFromState(
    const DataLakeTableStateSnapshot & state, ContextPtr) const
{
    chassert(std::holds_alternative<DeltaLake::TableStateSnapshot>(state));
    const auto & delta_state = std::get<DeltaLake::TableStateSnapshot>(state);
    auto snapshot = getTableSnapshot(static_cast<SnapshotVersion>(delta_state.version));

    auto result = std::make_unique<StorageInMemoryMetadata>();
    result->setColumns(ColumnsDescription{snapshot->getTableSchema()});
    result->setDataLakeTableState(state);
    return result;
}

bool DeltaLakeMetadataDeltaKernel::shouldReloadSchemaForConsistency(ContextPtr context) const
{
    return context->getSettingsRef()[Setting::delta_lake_reload_schema_for_consistency];
}

ObjectIterator DeltaLakeMetadataDeltaKernel::iterate(
    const ActionsDAG * filter_dag,
    FileProgressCallback callback,
    size_t list_batch_size,
    StorageMetadataPtr storage_metadata_snapshot,
    ContextPtr context) const
{
    logMetadataFiles(context);

    FailPointInjection::pauseFailPoint(FailPoints::delta_lake_metadata_iterate_pause);

    /// Use the snapshot version from the storage metadata snapshot if available.
    /// This ensures we use the same version that was captured when the storage snapshot was created,
    /// preventing logical races where the table is updated between snapshot creation and iteration.
    std::optional<SnapshotVersion> snapshot_version;
    if (auto version_from_metadata = extractDeltaLakeSnapshotVersionFromMetadata(storage_metadata_snapshot))
    {
        snapshot_version = static_cast<SnapshotVersion>(*version_from_metadata);
        LOG_TEST(log, "Using snapshot version {} from storage metadata snapshot", snapshot_version.value());
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No version found in table state snapshot");
    }

    return getTableSnapshot(snapshot_version)->iterate(filter_dag, callback, list_batch_size, context);
}

NamesAndTypesList DeltaLakeMetadataDeltaKernel::getTableSchema(ContextPtr local_context) const
{
    const auto & settings = local_context->getSettingsRef();
    if (auto start_version = settings[Setting::delta_lake_snapshot_start_version].value;
        start_version != DeltaLake::TableSnapshot::LATEST_SNAPSHOT_VERSION)
    {
        auto version_range = DeltaLake::TableChanges::getVersionRange(
            start_version,
            settings[Setting::delta_lake_snapshot_end_version].value);

        /// TODO: Once we support passing metadata state via metadata snapshot
        /// (already done in iceberg),
        /// then need to put table changes there as well to reuse.
        return getTableChanges(
            version_range,
            /* source_header */{},
            /* format_settings */{},
            local_context)->getSchema();
    }
    const auto snapshot_version = getSnapshotVersion(local_context->getSettingsRef());
    return getTableSnapshot(snapshot_version)->getTableSchema();
}

void DeltaLakeMetadataDeltaKernel::modifyFormatSettings(FormatSettings & format_settings, const Context &) const
{
    /// There can be missing columns because of ALTER ADD/DROP COLUMN.
    /// So to support reading from such tables it is enough to turn on this setting.
    format_settings.parquet.allow_missing_columns = true;
}

/// Returns non virtual column names, and virtual columns names and types.
static std::pair<Names, NamesAndTypesList> splitVirtualColumns(
    const Names & columns,
    VirtualsDescriptionPtr virtual_columns_description)
{
    Names non_virtual_columns;
    NamesAndTypesList virtual_columns;
    for (const auto & column_name : columns)
    {
        if (auto virtual_column = virtual_columns_description->tryGet(column_name))
            virtual_columns.emplace_back(std::move(*virtual_column));
        else
            non_virtual_columns.push_back(column_name);
    }
    return {non_virtual_columns, virtual_columns};
}

static DataTypePtr replaceTypeNamesToPhysicalRecursively(
    const DataTypePtr & type,
    const std::string & parent_explicit_name,
    const NameToNameMap & physical_names_map)
{
    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get());
    if (!tuple_type || !tuple_type->hasExplicitNames())
        return type;

    const auto & element_names = tuple_type->getElementNames();
    const auto & elements = tuple_type->getElements();

    Names result_element_names;
    DataTypes result_elements;
    result_element_names.resize(element_names.size());
    result_elements.resize(element_names.size());

    for (size_t i = 0; i < element_names.size(); ++i)
    {
        const auto & element_name = element_names[i];
        const auto full_element_name = parent_explicit_name.empty() ? element_name : parent_explicit_name + "." + element_name;

        auto physical_name = DeltaLake::tryGetPhysicalName(full_element_name, physical_names_map);
        if (physical_name)
        {
            auto pos = physical_name->find_last_of('.');
            if (pos != std::string::npos)
                physical_name = physical_name->substr(pos + 1);
            result_element_names[i] = *physical_name;
        }
        else
            result_element_names[i] = element_name;

        const auto & child_type = elements[i];
        result_elements[i] = replaceTypeNamesToPhysicalRecursively(child_type, full_element_name, physical_names_map);
    }
    return std::make_shared<DataTypeTuple>(result_elements, result_element_names);
}

/// Returns physical column and whether it is readable from data file.
/// We do not change given column actual type,
/// but can only change names inside the type (in case of Tuple).
static std::pair<NameAndTypePair, bool> getPhysicalNameAndType(
    const NameAndTypePair & column,
    const NamesAndTypesList & read_schema,
    const NameToNameMap & physical_names_map,
    LoggerPtr log)
{
    auto physical_name_in_storage = DeltaLake::getPhysicalName(column.getNameInStorage(), physical_names_map);
    auto physical_type_in_storage = replaceTypeNamesToPhysicalRecursively(column.getTypeInStorage(), column.getNameInStorage(), physical_names_map);

    /// Take column from read_schema, but only use it to check if column is readable,
    /// because read_schema_column.type can be different from physical_type_in_storage,
    /// because column.type is what user specified in CREATE TABLE query,
    /// and the second is what we parsed ourselves from delta-lake metadata.
    /// E.g. we abive by manually specified schema if it exists.
    auto read_schema_column = read_schema.tryGetByName(physical_name_in_storage);

    LOG_TEST(
        log, "Name: {}, name in storage: {}, subcolumn name: {}, "
        "physical name in storage: {}, physical type in storage: {}, read schema: {}",
        column.name, column.getNameInStorage(), column.getSubcolumnName(),
        physical_name_in_storage, physical_type_in_storage, read_schema_column.has_value() ? read_schema_column->dump() : "None");

    /// If column is not a readable column, but if columnMapping.mode == 'name'
    /// we will still have it named as col-<uuid> in metadata.
    NameAndTypePair result_column;
    if (column.isSubcolumn())
    {
        result_column = NameAndTypePair(
            physical_name_in_storage,
            column.getSubcolumnName(),
            physical_type_in_storage,
            column.type);
    }
    else
    {
        result_column = NameAndTypePair(physical_name_in_storage, physical_type_in_storage);
    }

    return {result_column, /* readable */read_schema_column.has_value()};
}

ReadFromFormatInfo DeltaLakeMetadataDeltaKernel::prepareReadingFromFormat(
    const Strings & requested_columns,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context,
    bool supports_subset_of_columns,
    bool /*supports_tuple_elements*/)
{
    /// The below code is similar to what can be found in `prepareReadingFromFormat.cpp`,
    /// but is adjusted for delta-lake.
    ReadFromFormatInfo info;

    /// Use the snapshot version from the storage metadata snapshot if available.
    /// This ensures we use the same version that was captured when the storage snapshot was created,
    /// preventing logical races where the table is updated between snapshot creation and reading.
    std::optional<SnapshotVersion> snapshot_version;
    if (auto version_from_metadata = extractDeltaLakeSnapshotVersionFromMetadata(storage_snapshot->metadata))
    {
        snapshot_version = static_cast<SnapshotVersion>(*version_from_metadata);
        LOG_TEST(log, "Using snapshot version {} from storage metadata snapshot for prepareReadingFromFormat", snapshot_version.value());
    }
    else
    {
        /// Fall back to reading from settings if no version is stored in metadata.
        snapshot_version = getSnapshotVersion(context->getSettingsRef());
        LOG_TEST(
            log, "Using snapshot version {} from settings (no version in metadata)",
            snapshot_version.has_value() ? toString(snapshot_version.value()) : "Latest");
    }

    auto snapshot = getTableSnapshot(snapshot_version);

    /// Read schema is different from table schema in case:
    /// 1. we have partition columns (they are not stored in the actual data)
    /// 2. columnMapping.mode = 'name' or 'id'.
    const auto physical_names_map = snapshot->getPhysicalNamesMap();
    const auto read_columns_desc = ColumnsDescription(snapshot->getReadSchema());
    std::unordered_set<std::string> partition_columns;
    {
        auto columns = snapshot->getPartitionColumns();
        partition_columns.insert(columns.begin(), columns.end());
    }

    Names columns_to_read;
    std::tie(columns_to_read, info.requested_virtual_columns) =
        splitVirtualColumns(requested_columns, storage_snapshot->virtual_columns);

    /// Create header for Source with non virtual columns
    /// and add virtual columns at the end of the header.
    /// (because they will be added to the chunk after reading regular columns).
    info.source_header = storage_snapshot->getSampleBlockForColumns(columns_to_read);
    for (const auto & column : info.requested_virtual_columns)
        info.source_header.insert({column.type->createColumn(), column.type, column.name});

    /// Set requested columns that should be read from data.
    info.requested_columns = storage_snapshot->getColumnsByNames(
        GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(),
        columns_to_read);

    /// Difference in `requested_table_columns` and `columns_to_read` after executing `filterTupleColumnsToRead`:
    /// If we have a column `c1.c2` which is an array and have `SELECT c1.c2.size0`,
    /// then in requested_table_columns we will have `c1.c2` as name in storage and `size0` as subcolumn name,
    /// while in columns_to_read we will have `c1` as name in storage and `c2` as subcolumn name.
    columns_to_read = filterTupleColumnsToRead(info.requested_columns);

    auto readable_columns_with_subcolumns = read_columns_desc.get(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns());
    auto readable_columns = read_columns_desc.get(GetColumnsOptions(GetColumnsOptions::All));

    /// We include partition columns in requested_columns (and not in format_header),
    /// because we will insert partition columns into chunk right after it is read from data file,
    /// so we want it to be verified that chunk contains all the requested columns.
    for (auto & name_and_type : info.requested_columns)
    {
        auto [result_name_and_type, _] = getPhysicalNameAndType(
            name_and_type,
            readable_columns_with_subcolumns,
            physical_names_map,
            log);
        name_and_type = result_name_and_type;
    }

    if (supports_subset_of_columns)
    {
        /// If only virtual columns were requested, just read the smallest non-partitin column.
        /// Because format header cannot be empty.
        if (columns_to_read.empty())
        {
            auto table_columns = storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns());
            NamesAndTypesList non_partition_columns;
            for (const auto & name_and_type : table_columns)
                if (!partition_columns.contains(name_and_type.name))
                    non_partition_columns.push_back(name_and_type);
            columns_to_read.push_back(ExpressionActions::getSmallestColumn(non_partition_columns).name);
        }

        info.columns_description = storage_snapshot->getDescriptionForColumns(columns_to_read);
    }
    else
    {
        info.columns_description = storage_snapshot->getDescriptionForColumns(
            storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::AllPhysical)).getNames());
    }

    info.serialization_hints = getSerializationHintsForFileLikeStorage(storage_snapshot->metadata, context);

    /// Set format_header with columns that should be read from data.
    auto columns_to_read_desc = info.columns_description.get(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns());
    for (auto & name_and_type : columns_to_read_desc)
    {
        auto [result_name_and_type, readable] = getPhysicalNameAndType(
            name_and_type,
            readable_columns_with_subcolumns,
            physical_names_map,
            log);

        if (readable)
            info.format_header.insert(ColumnWithTypeAndName{result_name_and_type.type, result_name_and_type.name});
    }

    for (const auto & name_and_type : info.columns_description)
        info.columns_description.rename(
            name_and_type.name,
            DeltaLake::getPhysicalName(name_and_type.name, physical_names_map));

    LOG_TEST(log, "Format header: {}", info.format_header.dumpStructure());
    LOG_TEST(log, "Source header: {}", info.source_header.dumpStructure());
    LOG_TEST(log, "Requested columns: {}", info.requested_columns.toString());
    LOG_TEST(log, "Columns description: {}", info.columns_description.toString(true));
    return info;
}

SinkToStoragePtr DeltaLakeMetadataDeltaKernel::write(
    SharedHeader sample_block,
    const StorageID & /* table_id */,
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context,
    std::shared_ptr<DataLake::ICatalog> /* catalog */)
{
    if (!context->getSettingsRef()[Setting::allow_experimental_delta_lake_writes])
    {
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "To enable delta lake writes, use allow_experimental_delta_lake_writes = 1");
    }

    const auto snapshot_version = getSnapshotVersion(context->getSettingsRef());
    auto snapshot = getTableSnapshot(snapshot_version);
    Names partition_columns = snapshot->getPartitionColumns();

    auto delta_transaction = std::make_shared<DeltaLake::WriteTransaction>(kernel_helper);
    delta_transaction->create();

    if (partition_columns.empty())
    {
        return std::make_shared<DeltaLakeSink>(
            delta_transaction,
            object_storage_,
            context,
            sample_block,
            format_settings,
            configuration->format,
            configuration->compression_method);
    }

    return std::make_shared<DeltaLakePartitionedSink>(
        delta_transaction,
        partition_columns,
        object_storage_,
        context,
        sample_block,
        format_settings,
        configuration->format,
        configuration->compression_method);
}

void DeltaLakeMetadataDeltaKernel::logMetadataFiles(ContextPtr context) const
{
    if (!context->getSettingsRef()[Setting::delta_lake_log_metadata].value)
        return;

    const auto keys = listFiles(*object_storage, kernel_helper->getDataPath(), deltalake_metadata_directory, metadata_file_suffix);
    auto read_settings = context->getReadSettings();
    for (const String & key : keys)
    {
        RelativePathWithMetadata object_info(key);
        auto buf = createReadBuffer(object_info, object_storage, context, log);
        String json_str;
        readStringUntilEOF(json_str, *buf);
        insertDeltaRowToLogTable(context, json_str, kernel_helper->getDataPath(), key);
    }

}

std::string DeltaLakeMetadataDeltaKernel::latestSnapshotVersionToStr() const
{
    return latest_snapshot_version.has_value() ? toString(latest_snapshot_version.value()) : "Unknown";
}

}

#endif
