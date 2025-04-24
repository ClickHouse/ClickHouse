#include "StorageSystemProjectionPartsColumns.h"

#include <Common/escapeForFileName.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/VirtualColumnUtils.h>
#include <Databases/IDatabase.h>
#include <Parsers/queryToString.h>

namespace DB
{


StorageSystemProjectionPartsColumns::StorageSystemProjectionPartsColumns(const StorageID & table_id_)
    : StorageSystemPartsBase(table_id_,
    ColumnsDescription{
        {"partition",                                  std::make_shared<DataTypeString>(), "The partition name. "},
        {"name",                                       std::make_shared<DataTypeString>(), "Name of the data part."},
        {"part_type",                                  std::make_shared<DataTypeString>(), "The data part storing format."},
        {"parent_name",                                std::make_shared<DataTypeString>(), "The name of the source (parent) data part."},
        {"parent_uuid",                                std::make_shared<DataTypeUUID>(),   "The UUID of the source (parent) data part."},
        {"parent_part_type",                           std::make_shared<DataTypeString>(), "The source (parent) data part storing format."},
        {"active",                                     std::make_shared<DataTypeUInt8>(),  "Flag that indicates whether the data part is active"},
        {"marks",                                      std::make_shared<DataTypeUInt64>(), "The number of marks."},
        {"rows",                                       std::make_shared<DataTypeUInt64>(), "The number of rows."},
        {"bytes_on_disk",                              std::make_shared<DataTypeUInt64>(), "Total size of all the data part files in bytes."},
        {"data_compressed_bytes",                      std::make_shared<DataTypeUInt64>(), "Total size of compressed data in the data part. All the auxiliary files (for example, files with marks) are not included."},
        {"data_uncompressed_bytes",                    std::make_shared<DataTypeUInt64>(), "Total size of uncompressed data in the data part. All the auxiliary files (for example, files with marks) are not included."},
        {"marks_bytes",                                std::make_shared<DataTypeUInt64>(), "The size of the file with marks."},
        {"parent_marks",                               std::make_shared<DataTypeUInt64>(), "The number of marks in the source (parent) part."},
        {"parent_rows",                                std::make_shared<DataTypeUInt64>(), "The number of rows in the source (parent) part."},
        {"parent_bytes_on_disk",                       std::make_shared<DataTypeUInt64>(), "Total size of all the source (parent) data part files in bytes."},
        {"parent_data_compressed_bytes",               std::make_shared<DataTypeUInt64>(), "Total size of compressed data in the source (parent) data part."},
        {"parent_data_uncompressed_bytes",             std::make_shared<DataTypeUInt64>(), "Total size of uncompressed data in the source (parent) data part."},
        {"parent_marks_bytes",                         std::make_shared<DataTypeUInt64>(), "The size of the file with marks in the source (parent) data part."},
        {"modification_time",                          std::make_shared<DataTypeDateTime>(), "The time the directory with the data part was modified. This usually corresponds to the time of data part creation."},
        {"remove_time",                                std::make_shared<DataTypeDateTime>(), "The time when the data part became inactive."},
        {"refcount",                                   std::make_shared<DataTypeUInt32>(), "The number of places where the data part is used. A value greater than 2 indicates that the data part is used in queries or merges."},
        {"min_date",                                   std::make_shared<DataTypeDate>(), "The minimum value for the Date column if that is included in the partition key."},
        {"max_date",                                   std::make_shared<DataTypeDate>(), "The maximum value for the Date column if that is included in the partition key."},
        {"min_time",                                   std::make_shared<DataTypeDateTime>(), "The minimum value for the DateTime column if that is included in the partition key."},
        {"max_time",                                   std::make_shared<DataTypeDateTime>(), "The maximum value for the DateTime column if that is included in the partition key."},
        {"partition_id",                               std::make_shared<DataTypeString>(), "ID of the partition."},
        {"min_block_number",                           std::make_shared<DataTypeInt64>(), "The minimum number of data parts that make up the current part after merging."},
        {"max_block_number",                           std::make_shared<DataTypeInt64>(), "The maximum number of data parts that make up the current part after merging."},
        {"level",                                      std::make_shared<DataTypeUInt32>(), "Depth of the merge tree. Zero means that the current part was created by insert rather than by merging other parts."},
        {"data_version",                               std::make_shared<DataTypeUInt64>(), "Number that is used to determine which mutations should be applied to the data part (mutations with a version higher than data_version)."},
        {"primary_key_bytes_in_memory",                std::make_shared<DataTypeUInt64>(), "The amount of memory (in bytes) used by primary key values."},
        {"primary_key_bytes_in_memory_allocated",      std::make_shared<DataTypeUInt64>(), "The amount of memory (in bytes) reserved for primary key values."},

        {"database",                                   std::make_shared<DataTypeString>(), "Name of the database."},
        {"table",                                      std::make_shared<DataTypeString>(), "Name of the table."},
        {"engine",                                     std::make_shared<DataTypeString>(), "Name of the table engine without parameters."},
        {"disk_name",                                  std::make_shared<DataTypeString>(), "Name of a disk that stores the data part."},
        {"path",                                       std::make_shared<DataTypeString>(), "Absolute path to the folder with data part files."},

        {"column",                                     std::make_shared<DataTypeString>(), "Name of the column."},
        {"type",                                       std::make_shared<DataTypeString>(), "Column type."},
        {"column_position",                            std::make_shared<DataTypeUInt64>(), "Ordinal position of a column in a table starting with 1."},
        {"default_kind",                               std::make_shared<DataTypeString>(), "Expression type (DEFAULT, MATERIALIZED, ALIAS) for the default value, or an empty string if it is not defined."},
        {"default_expression",                         std::make_shared<DataTypeString>(), "Expression for the default value, or an empty string if it is not defined."},
        {"column_bytes_on_disk",                       std::make_shared<DataTypeUInt64>(), "Total size of the column in bytes."},
        {"column_data_compressed_bytes",               std::make_shared<DataTypeUInt64>(), "Total size of compressed data in the column, in bytes."},
        {"column_data_uncompressed_bytes",             std::make_shared<DataTypeUInt64>(), "Total size of the decompressed data in the column, in bytes."},
        {"column_marks_bytes",                         std::make_shared<DataTypeUInt64>(), "The size of the column with marks, in bytes."},
        {"column_modification_time",                   std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "The last time the column was modified."},
    }
    )
{
}

void StorageSystemProjectionPartsColumns::processNextStorage(
    ContextPtr, MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column)
{
    /// Prepare information about columns in storage.
    struct ColumnInfo
    {
        String default_kind;
        String default_expression;
    };

    auto storage_metadata = info.storage->getInMemoryMetadataPtr();
    std::unordered_map<String, std::unordered_map<String, ColumnInfo>> projection_columns_info;
    for (const auto & projection : storage_metadata->getProjections())
    {
        auto & columns_info = projection_columns_info[projection.name];
        for (const auto & column : projection.metadata->getColumns())
        {
            ColumnInfo column_info;
            if (column.default_desc.expression)
            {
                column_info.default_kind = toString(column.default_desc.kind);
                column_info.default_expression = queryToString(column.default_desc.expression);
            }

            columns_info[column.name] = column_info;
        }
    }

    /// Go through the list of projection parts.
    MergeTreeData::DataPartStateVector all_parts_state;
    MergeTreeData::ProjectionPartsVector all_parts = info.getProjectionParts(all_parts_state, has_state_column);
    for (size_t part_number = 0; part_number < all_parts.projection_parts.size(); ++part_number)
    {
        const auto & part = all_parts.projection_parts[part_number];
        const auto * parent_part = part->getParentPart();
        chassert(parent_part);

        auto part_state = all_parts_state[part_number];
        auto columns_size = part->getTotalColumnsSize();
        auto parent_columns_size = parent_part->getTotalColumnsSize();

        /// For convenience, in returned refcount, don't add references that was due to local variables in this method: all_parts, active_parts.
        auto use_count = part.use_count() - 1;

        auto min_max_date = parent_part->getMinMaxDate();
        auto min_max_time = parent_part->getMinMaxTime();

        auto index_size_in_bytes = part->getIndexSizeInBytes();
        auto index_size_in_allocated_bytes = part->getIndexSizeInAllocatedBytes();

        using State = MergeTreeDataPartState;

        size_t column_position = 0;
        auto & columns_info = projection_columns_info[part->name];
        for (const auto & column : part->getColumns())
        {
            ++column_position;
            size_t src_index = 0, res_index = 0;
            if (columns_mask[src_index++])
            {
                WriteBufferFromOwnString out;
                parent_part->partition.serializeText(*info.data, out, format_settings);
                columns[res_index++]->insert(out.str());
            }
            if (columns_mask[src_index++])
                columns[res_index++]->insert(part->name);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(part->getTypeName());
            if (columns_mask[src_index++])
                columns[res_index++]->insert(parent_part->name);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(parent_part->uuid);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(parent_part->getTypeName());
            if (columns_mask[src_index++])
                columns[res_index++]->insert(part_state == State::Active);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(part->getMarksCount());
            if (columns_mask[src_index++])
                columns[res_index++]->insert(part->rows_count);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(part->getBytesOnDisk());
            if (columns_mask[src_index++])
                columns[res_index++]->insert(columns_size.data_compressed);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(columns_size.data_uncompressed);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(columns_size.marks);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(parent_part->getMarksCount());
            if (columns_mask[src_index++])
                columns[res_index++]->insert(parent_part->rows_count);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(parent_part->getBytesOnDisk());
            if (columns_mask[src_index++])
                columns[res_index++]->insert(parent_columns_size.data_compressed);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(parent_columns_size.data_uncompressed);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(parent_columns_size.marks);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(UInt64(part->modification_time));
            if (columns_mask[src_index++])
                columns[res_index++]->insert(UInt64(part->remove_time.load(std::memory_order_relaxed)));

            if (columns_mask[src_index++])
                columns[res_index++]->insert(UInt64(use_count));

            if (columns_mask[src_index++])
                columns[res_index++]->insert(min_max_date.first);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(min_max_date.second);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(static_cast<UInt32>(min_max_time.first));
            if (columns_mask[src_index++])
                columns[res_index++]->insert(static_cast<UInt32>(min_max_time.second));

            if (columns_mask[src_index++])
                columns[res_index++]->insert(parent_part->info.partition_id);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(parent_part->info.min_block);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(parent_part->info.max_block);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(parent_part->info.level);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(UInt64(parent_part->info.getDataVersion()));
            if (columns_mask[src_index++])
                columns[res_index++]->insert(index_size_in_bytes);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(index_size_in_allocated_bytes);

            if (columns_mask[src_index++])
                columns[res_index++]->insert(info.database);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(info.table);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(info.engine);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(part->getDataPartStorage().getDiskName());
            if (columns_mask[src_index++])
                columns[res_index++]->insert(part->getDataPartStorage().getFullPath());

            if (columns_mask[src_index++])
                columns[res_index++]->insert(column.name);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(column.type->getName());
            if (columns_mask[src_index++])
                columns[res_index++]->insert(column_position);

            auto column_info_it = columns_info.find(column.name);
            if (column_info_it != columns_info.end())
            {
                if (columns_mask[src_index++])
                    columns[res_index++]->insert(column_info_it->second.default_kind);
                if (columns_mask[src_index++])
                    columns[res_index++]->insert(column_info_it->second.default_expression);
            }
            else
            {
                if (columns_mask[src_index++])
                    columns[res_index++]->insertDefault();
                if (columns_mask[src_index++])
                    columns[res_index++]->insertDefault();
            }

            ColumnSize column_size = part->getColumnSize(column.name);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(column_size.data_compressed + column_size.marks);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(column_size.data_compressed);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(column_size.data_uncompressed);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(column_size.marks);
            if (columns_mask[src_index++])
            {
                if (auto column_modification_time = part->getColumnModificationTime(column.name))
                    columns[res_index++]->insert(UInt64(column_modification_time.value()));
                else
                    columns[res_index++]->insertDefault();
            }

            if (has_state_column)
                columns[res_index++]->insert(part->stateString());
        }
    }
}

}
