#include <DataTypes/DataTypeNullable.h>
#include "config.h"

#if USE_PARQUET && USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakeSink.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakePartitionedSink.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/WriteTransaction.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/transformTypesRecursively.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Common/logger_useful.h>
#include <Common/assert_cast.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Interpreters/DeltaMetadataLog.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

namespace Setting
{
    extern const SettingsBool delta_lake_log_metadata;
    extern const SettingsBool allow_experimental_delta_lake_writes;
}

[[maybe_unused]] static void tracingCallback(struct ffi::Event event)
{
    /// Do not pollute logs.
    if (event.message.len > 100)
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

DeltaLakeMetadataDeltaKernel::DeltaLakeMetadataDeltaKernel(
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationWeakPtr configuration_,
    ContextPtr context)
    : log(getLogger("DeltaLakeMetadata"))
    , kernel_helper(DB::getKernelHelper(configuration_.lock(), object_storage))
    , table_snapshot(std::make_shared<DeltaLake::TableSnapshot>(
            kernel_helper,
            object_storage,
            context,
            log))
{
    object_storage_common = object_storage;
#ifdef DEBUG_OR_SANITIZER_BUILD
    //ffi::enable_event_tracing(tracingCallback, ffi::Level::TRACE);
#endif
}

bool DeltaLakeMetadataDeltaKernel::operator ==(const IDataLakeMetadata & metadata) const
{
    const auto & delta_lake_metadata = dynamic_cast<const DeltaLakeMetadataDeltaKernel &>(metadata);
    std::lock_guard lk1(table_snapshot_mutex);
    std::lock_guard lk2(delta_lake_metadata.table_snapshot_mutex);
    return table_snapshot->getVersion() == delta_lake_metadata.table_snapshot->getVersion();
}

void DeltaLakeMetadataDeltaKernel::update(const ContextPtr & context)
{
    std::lock_guard lock(table_snapshot_mutex);
    table_snapshot->update(context);
}

ObjectIterator DeltaLakeMetadataDeltaKernel::iterate(
    const ActionsDAG * filter_dag,
    FileProgressCallback callback,
    size_t list_batch_size,
    StorageMetadataPtr /*storage_metadata_snapshot*/,
    ContextPtr context) const
{
    logMetadataFiles(context);
    std::lock_guard lock(table_snapshot_mutex);
    return table_snapshot->iterate(filter_dag, callback, list_batch_size);
}

NamesAndTypesList DeltaLakeMetadataDeltaKernel::getTableSchema(ContextPtr /*local_context*/) const
{
    std::lock_guard lock(table_snapshot_mutex);
    return table_snapshot->getTableSchema();
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

    /// Read schema is different from table schema in case:
    /// 1. we have partition columns (they are not stored in the actual data)
    /// 2. columnMapping.mode = 'name' or 'id'.
    DB::NameToNameMap physical_names_map;
    ColumnsDescription read_columns_desc;
    std::unordered_set<std::string> partition_columns;
    {
        std::lock_guard lock(table_snapshot_mutex);
        physical_names_map = table_snapshot->getPhysicalNamesMap();
        read_columns_desc = ColumnsDescription(table_snapshot->getReadSchema());

        auto columns = table_snapshot->getPartitionColumns();
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
    ObjectStoragePtr object_storage,
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

    Names partition_columns;
    {
        std::lock_guard lock(table_snapshot_mutex);
        partition_columns = table_snapshot->getPartitionColumns();
    }

    auto delta_transaction = std::make_shared<DeltaLake::WriteTransaction>(kernel_helper);
    delta_transaction->create();

    if (partition_columns.empty())
    {
        return std::make_shared<DeltaLakeSink>(
            delta_transaction, configuration, object_storage, context, sample_block, format_settings);
    }

    return std::make_shared<DeltaLakePartitionedSink>(
        delta_transaction, configuration, partition_columns, object_storage,
        context, sample_block, format_settings);
}

void DeltaLakeMetadataDeltaKernel::logMetadataFiles(ContextPtr context) const
{
    if (!context->getSettingsRef()[Setting::delta_lake_log_metadata].value)
        return;

    const auto keys = listFiles(*object_storage_common, kernel_helper->getDataPath(), deltalake_metadata_directory, metadata_file_suffix);
    auto read_settings = context->getReadSettings();
    for (const String & key : keys)
    {
        RelativePathWithMetadata object_info(key);
        auto buf = createReadBuffer(object_info, object_storage_common, context, log);
        String json_str;
        readStringUntilEOF(json_str, *buf);
        insertDeltaRowToLogTable(context, json_str, kernel_helper->getDataPath(), key);
    }

}

}

#endif
