#include "StorageSystemProjectionParts.h"

#include <Common/escapeForFileName.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeUUID.h>
#include <Storages/VirtualColumnUtils.h>
#include <Databases/IDatabase.h>
#include <Parsers/queryToString.h>
#include <base/hex.h>

namespace DB
{

StorageSystemProjectionParts::StorageSystemProjectionParts(const StorageID & table_id_)
    : StorageSystemPartsBase(table_id_,
    ColumnsDescription{
        {"partition",                                   std::make_shared<DataTypeString>(), "The partition name."},
        {"name",                                        std::make_shared<DataTypeString>(), "Name of the data part."},
        {"part_type",                                   std::make_shared<DataTypeString>(), "The data part storing format. Possible Values: Wide (a file per column) and Compact (a single file for all columns)."},
        {"parent_name",                                 std::make_shared<DataTypeString>(), "The name of the source (parent) data part."},
        {"parent_uuid",                                 std::make_shared<DataTypeUUID>(),   "The UUID of the source (parent) data part."},
        {"parent_part_type",                            std::make_shared<DataTypeString>(), "The source (parent) data part storing format."},
        {"active",                                      std::make_shared<DataTypeUInt8>(),  "Flag that indicates whether the data part is active. If a data part is active, it's used in a table. Otherwise, it's about to be deleted. Inactive data parts appear after merging and mutating operations."},
        {"marks",                                       std::make_shared<DataTypeUInt64>(), "The number of marks. To get the approximate number of rows in a data part, multiply marks by the index granularity (usually 8192) (this hint does not work for adaptive granularity)."},
        {"rows",                                        std::make_shared<DataTypeUInt64>(), "The number of rows."},
        {"bytes_on_disk",                               std::make_shared<DataTypeUInt64>(), "Total size of all the data part files in bytes."},
        {"data_compressed_bytes",                       std::make_shared<DataTypeUInt64>(), "Total size of compressed data in the data part. All the auxiliary files (for example, files with marks) are not included."},
        {"data_uncompressed_bytes",                     std::make_shared<DataTypeUInt64>(), "Total size of uncompressed data in the data part. All the auxiliary files (for example, files with marks) are not included."},
        {"marks_bytes",                                 std::make_shared<DataTypeUInt64>(), "The size of the file with marks."},
        {"parent_marks",                                std::make_shared<DataTypeUInt64>(), "The number of marks in the source (parent) part."},
        {"parent_rows",                                 std::make_shared<DataTypeUInt64>(), "The number of rows in the source (parent) part."},
        {"parent_bytes_on_disk",                        std::make_shared<DataTypeUInt64>(), "Total size of all the source (parent) data part files in bytes."},
        {"parent_data_compressed_bytes",                std::make_shared<DataTypeUInt64>(), "Total size of compressed data in the source (parent) data part."},
        {"parent_data_uncompressed_bytes",              std::make_shared<DataTypeUInt64>(), "Total size of uncompressed data in the source (parent) data part."},
        {"parent_marks_bytes",                          std::make_shared<DataTypeUInt64>(), "The size of the file with marks in the source (parent) data part."},
        {"modification_time",                           std::make_shared<DataTypeDateTime>(), "The time the directory with the data part was modified. This usually corresponds to the time of data part creation."},
        {"remove_time",                                 std::make_shared<DataTypeDateTime>(), "The time when the data part became inactive."},
        {"refcount",                                    std::make_shared<DataTypeUInt32>(), "The number of places where the data part is used. A value greater than 2 indicates that the data part is used in queries or merges."},
        {"min_date",                                    std::make_shared<DataTypeDate>(), "The minimum value of the date key in the data part."},
        {"max_date",                                    std::make_shared<DataTypeDate>(), "The maximum value of the date key in the data part."},
        {"min_time",                                    std::make_shared<DataTypeDateTime>(), "The minimum value of the date and time key in the data part."},
        {"max_time",                                    std::make_shared<DataTypeDateTime>(), "The maximum value of the date and time key in the data part."},
        {"partition_id",                                std::make_shared<DataTypeString>(), "ID of the partition."},
        {"min_block_number",                            std::make_shared<DataTypeInt64>(),  "The minimum number of data parts that make up the current part after merging."},
        {"max_block_number",                            std::make_shared<DataTypeInt64>(),  "The maximum number of data parts that make up the current part after merging."},
        {"level",                                       std::make_shared<DataTypeUInt32>(), "Depth of the merge tree. Zero means that the current part was created by insert rather than by merging other parts."},
        {"data_version",                                std::make_shared<DataTypeUInt64>(), "Number that is used to determine which mutations should be applied to the data part (mutations with a version higher than data_version)."},
        {"primary_key_bytes_in_memory",                 std::make_shared<DataTypeUInt64>(), "The amount of memory (in bytes) used by primary key values."},
        {"primary_key_bytes_in_memory_allocated",       std::make_shared<DataTypeUInt64>(), "The amount of memory (in bytes) reserved for primary key values."},
        {"is_frozen",                                   std::make_shared<DataTypeUInt8>(),  "Flag that shows that a partition data backup exists. 1, the backup exists. 0, the backup does not exist. "},

        {"database",                                    std::make_shared<DataTypeString>(), "Name of the database."},
        {"table",                                       std::make_shared<DataTypeString>(), "Name of the table."},
        {"engine",                                      std::make_shared<DataTypeString>(), "Name of the table engine without parameters."},
        {"disk_name",                                   std::make_shared<DataTypeString>(), "Name of a disk that stores the data part."},
        {"path",                                        std::make_shared<DataTypeString>(), "Absolute path to the folder with data part files."},

        {"hash_of_all_files",                           std::make_shared<DataTypeString>(), "sipHash128 of compressed files."},
        {"hash_of_uncompressed_files",                  std::make_shared<DataTypeString>(), "sipHash128 of uncompressed files (files with marks, index file etc.)."},
        {"uncompressed_hash_of_compressed_files",       std::make_shared<DataTypeString>(), "sipHash128 of data in the compressed files as if they were uncompressed."},

        {"delete_ttl_info_min",                         std::make_shared<DataTypeDateTime>(), "The minimum value of the date and time key for TTL DELETE rule."},
        {"delete_ttl_info_max",                         std::make_shared<DataTypeDateTime>(), "The maximum value of the date and time key for TTL DELETE rule."},

        {"move_ttl_info.expression",                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Array of expressions. Each expression defines a TTL MOVE rule."},
        {"move_ttl_info.min",                           std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()), "Array of date and time values. Each element describes the minimum key value for a TTL MOVE rule."},
        {"move_ttl_info.max",                           std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()), "Array of date and time values. Each element describes the maximum key value for a TTL MOVE rule."},

        {"default_compression_codec",                   std::make_shared<DataTypeString>(), "The name of the codec used to compress this data part (in case when there is no explicit codec for columns)."},

        {"recompression_ttl_info.expression",           std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),   "The TTL expression."},
        {"recompression_ttl_info.min",                  std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()), "The minimum value of the calculated TTL expression within this part. Used to understand whether we have at least one row with expired TTL."},
        {"recompression_ttl_info.max",                  std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()), "The maximum value of the calculated TTL expression within this part. Used to understand whether we have all rows with expired TTL."},

        {"group_by_ttl_info.expression",                std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),   "The TTL expression."},
        {"group_by_ttl_info.min",                       std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()), "The minimum value of the calculated TTL expression within this part. Used to understand whether we have at least one row with expired TTL."},
        {"group_by_ttl_info.max",                       std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()), "The maximum value of the calculated TTL expression within this part. Used to understand whether we have all rows with expired TTL."},

        {"rows_where_ttl_info.expression",              std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),   "The TTL expression."},
        {"rows_where_ttl_info.min",                     std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()), "The minimum value of the calculated TTL expression within this part. Used to understand whether we have at least one row with expired TTL."},
        {"rows_where_ttl_info.max",                     std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()), "The maximum value of the calculated TTL expression within this part. Used to understand whether we have all rows with expired TTL."},

        {"is_broken",                                   std::make_shared<DataTypeUInt8>(), "Whether projection part is broken"},
        {"exception_code",                              std::make_shared<DataTypeInt32>(), "Exception message explaining broken state of the projection part"},
        {"exception",                                   std::make_shared<DataTypeString>(), "Exception code explaining broken state of the projection part"},
    })
{
}

void StorageSystemProjectionParts::processNextStorage(
    ContextPtr, MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column)
{
    using State = MergeTreeDataPartState;
    MergeTreeData::DataPartStateVector all_parts_state;
    MergeTreeData::ProjectionPartsVector all_parts = info.getProjectionParts(all_parts_state, has_state_column);
    for (size_t part_number = 0; part_number < all_parts.projection_parts.size(); ++part_number)
    {
        const auto & part = all_parts.projection_parts[part_number];
        const auto * parent_part = part->getParentPart();
        chassert(parent_part);

        auto part_state = all_parts_state[part_number];

        ColumnSize columns_size = part->getTotalColumnsSize();
        ColumnSize parent_columns_size = parent_part->getTotalColumnsSize();

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
            columns[res_index++]->insert(static_cast<UInt64>(part->modification_time));

        if (columns_mask[src_index++])
        {
            time_t remove_time = part->remove_time.load(std::memory_order_relaxed);
            columns[res_index++]->insert(static_cast<UInt64>(remove_time == std::numeric_limits<time_t>::max() ? 0 : remove_time));
        }

        /// For convenience, in returned refcount, don't add references that was due to local variables in this method: all_parts, active_parts.
        if (columns_mask[src_index++])
            columns[res_index++]->insert(static_cast<UInt64>(part.use_count() - 1));

        auto min_max_date = parent_part->getMinMaxDate();
        auto min_max_time = parent_part->getMinMaxTime();

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
            columns[res_index++]->insert(static_cast<UInt64>(parent_part->info.getDataVersion()));
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->getIndexSizeInBytes());
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->getIndexSizeInAllocatedBytes());
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->is_frozen.load(std::memory_order_relaxed));

        if (columns_mask[src_index++])
            columns[res_index++]->insert(info.database);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(info.table);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(info.engine);

        if (part->isStoredOnDisk())
        {
            if (columns_mask[src_index++])
                columns[res_index++]->insert(part->getDataPartStorage().getDiskName());
            if (columns_mask[src_index++])
                columns[res_index++]->insert(part->getDataPartStorage().getFullPath());
        }
        else
        {
            if (columns_mask[src_index++])
                columns[res_index++]->insertDefault();
            if (columns_mask[src_index++])
                columns[res_index++]->insertDefault();
        }


        {
            MinimalisticDataPartChecksums helper;
            if (columns_mask[src_index] || columns_mask[src_index + 1] || columns_mask[src_index + 2])
                helper.computeTotalChecksums(part->checksums);

            if (columns_mask[src_index++])
            {
                auto checksum = helper.hash_of_all_files;
                columns[res_index++]->insert(getHexUIntLowercase(checksum));
            }
            if (columns_mask[src_index++])
            {
                auto checksum = helper.hash_of_uncompressed_files;
                columns[res_index++]->insert(getHexUIntLowercase(checksum));
            }
            if (columns_mask[src_index++])
            {
                auto checksum = helper.uncompressed_hash_of_compressed_files;
                columns[res_index++]->insert(getHexUIntLowercase(checksum));
            }
        }

        /// delete_ttl_info
        if (columns_mask[src_index++])
            columns[res_index++]->insert(static_cast<UInt32>(part->ttl_infos.table_ttl.min));
        if (columns_mask[src_index++])
            columns[res_index++]->insert(static_cast<UInt32>(part->ttl_infos.table_ttl.max));

        auto add_ttl_info_map = [&](const TTLInfoMap & ttl_info_map)
        {
            Array expression_array;
            Array min_array;
            Array max_array;
            if (columns_mask[src_index])
                expression_array.reserve(ttl_info_map.size());
            if (columns_mask[src_index + 1])
                min_array.reserve(ttl_info_map.size());
            if (columns_mask[src_index + 2])
                max_array.reserve(ttl_info_map.size());
            for (const auto & [expression, ttl_info] : ttl_info_map)
            {
                if (columns_mask[src_index])
                    expression_array.emplace_back(expression);
                if (columns_mask[src_index + 1])
                    min_array.push_back(static_cast<UInt32>(ttl_info.min));
                if (columns_mask[src_index + 2])
                    max_array.push_back(static_cast<UInt32>(ttl_info.max));
            }
            if (columns_mask[src_index++])
                columns[res_index++]->insert(expression_array);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(min_array);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(max_array);
        };

        add_ttl_info_map(part->ttl_infos.moves_ttl);

        if (columns_mask[src_index++])
        {
            if (part->default_codec)
                columns[res_index++]->insert(queryToString(part->default_codec->getCodecDesc()));
            else
                columns[res_index++]->insertDefault();
        }

        add_ttl_info_map(part->ttl_infos.recompression_ttl);
        add_ttl_info_map(part->ttl_infos.group_by_ttl);
        add_ttl_info_map(part->ttl_infos.rows_where_ttl);

        {
            if (columns_mask[src_index++])
                columns[res_index++]->insert(part->is_broken.load(std::memory_order_relaxed));

            if (part->is_broken)
            {
                std::lock_guard lock(part->broken_reason_mutex);
                if (columns_mask[src_index++])
                    columns[res_index++]->insert(part->exception_code);
                if (columns_mask[src_index++])
                    columns[res_index++]->insert(part->exception);
            }
            else
            {
                if (columns_mask[src_index++])
                    columns[res_index++]->insertDefault();
                if (columns_mask[src_index++])
                    columns[res_index++]->insertDefault();
            }
        }

        /// _state column should be the latest.
        /// Do not use part->getState*, it can be changed from different thread
        if (has_state_column)
            columns[res_index++]->insert(IMergeTreeDataPart::stateString(part_state));
    }
}

}
