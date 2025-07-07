#include "config.h"

#if USE_PARQUET && USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>
#include <fmt/ranges.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

DeltaLakeMetadataDeltaKernel::DeltaLakeMetadataDeltaKernel(
    ObjectStoragePtr object_storage,
    ConfigurationObserverPtr configuration_,
    bool read_schema_same_as_table_schema_)
    : log(getLogger("DeltaLakeMetadata"))
    , table_snapshot(
        std::make_shared<DeltaLake::TableSnapshot>(
            getKernelHelper(configuration_.lock(), object_storage),
            object_storage,
            read_schema_same_as_table_schema_,
            log))
{
}

bool DeltaLakeMetadataDeltaKernel::operator ==(const IDataLakeMetadata & metadata) const
{
    const auto & delta_lake_metadata = dynamic_cast<const DeltaLakeMetadataDeltaKernel &>(metadata);
    return table_snapshot->getVersion() == delta_lake_metadata.table_snapshot->getVersion();
}

bool DeltaLakeMetadataDeltaKernel::update(const ContextPtr &)
{
    return table_snapshot->update();
}

ObjectIterator DeltaLakeMetadataDeltaKernel::iterate(
    const ActionsDAG * filter_dag,
    FileProgressCallback callback,
    size_t list_batch_size) const
{
    return table_snapshot->iterate(filter_dag, callback, list_batch_size);
}

NamesAndTypesList DeltaLakeMetadataDeltaKernel::getTableSchema() const
{
    return table_snapshot->getTableSchema();
}

void DeltaLakeMetadataDeltaKernel::modifyFormatSettings(FormatSettings & format_settings) const
{
    /// There can be missing columns because of ALTER ADD/DROP COLUMN.
    /// So to support reading from such tables it is enough to turn on this setting.
    format_settings.parquet.allow_missing_columns = true;
}

DB::ReadFromFormatInfo DeltaLakeMetadataDeltaKernel::prepareReadingFromFormat(
    const Strings & requested_columns,
    const DB::StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context,
    bool supports_subset_of_columns)
{
    auto info = DB::prepareReadingFromFormat(requested_columns, storage_snapshot, context, supports_subset_of_columns);
    info.format_header.clear();

    /// Read schema is different from table schema in case:
    /// 1. we have partition columns (they are not stored in the actual data)
    /// 2. columnMapping.mode = 'name' or 'id'.
    /// So we add partition columns to read schema and put it together into format_header.
    /// Partition values will be added to result data right after data is read.

    for (const auto & [column_name, column_type] : table_snapshot->getReadSchema())
        info.format_header.insert({column_type->createColumn(), column_type, column_name});

    const auto & physical_names_map = table_snapshot->getPhysicalNamesMap();
    auto get_physical_name = [&](const std::string & column_name)
    {
        if (physical_names_map.empty())
            return column_name;
        auto it = physical_names_map.find(column_name);
        if (it == physical_names_map.end())
        {
            Names keys;
            keys.reserve(physical_names_map.size());
            for (const auto & [key, _] : physical_names_map)
                keys.push_back(key);

            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Not found column {} in physical names map. There are only columns: {}",
                column_name, fmt::join(keys, ", "));
        }
        return it->second;
    };

    const auto & table_schema = table_snapshot->getTableSchema();
    for (const auto & column_name : table_snapshot->getPartitionColumns())
    {
        auto name_and_type = table_schema.tryGetByName(column_name);
        if (!name_and_type)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Not found partition column {} in table schema", column_name);

        info.format_header.insert({name_and_type->type->createColumn(), name_and_type->type, get_physical_name(column_name)});
    }

    /// Update requested columns to reference actual physical column names.
    if (!physical_names_map.empty())
    {
        for (auto & [column_name, _] : info.requested_columns)
            column_name = get_physical_name(column_name);
    }

    return info;
}

}

#endif
