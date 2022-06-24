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
#include <Common/hex.h>

namespace DB
{

StorageSystemProjectionParts::StorageSystemProjectionParts(const StorageID & table_id_)
    : StorageSystemPartsBase(table_id_,
    {
        {"partition",                                   std::make_shared<DataTypeString>()},
        {"name",                                        std::make_shared<DataTypeString>()},
        {"part_type",                                   std::make_shared<DataTypeString>()},
        {"parent_name",                                 std::make_shared<DataTypeString>()},
        {"parent_uuid",                                 std::make_shared<DataTypeUUID>()},
        {"parent_part_type",                            std::make_shared<DataTypeString>()},
        {"active",                                      std::make_shared<DataTypeUInt8>()},
        {"marks",                                       std::make_shared<DataTypeUInt64>()},
        {"rows",                                        std::make_shared<DataTypeUInt64>()},
        {"bytes_on_disk",                               std::make_shared<DataTypeUInt64>()},
        {"data_compressed_bytes",                       std::make_shared<DataTypeUInt64>()},
        {"data_uncompressed_bytes",                     std::make_shared<DataTypeUInt64>()},
        {"marks_bytes",                                 std::make_shared<DataTypeUInt64>()},
        {"parent_marks",                                std::make_shared<DataTypeUInt64>()},
        {"parent_rows",                                 std::make_shared<DataTypeUInt64>()},
        {"parent_bytes_on_disk",                        std::make_shared<DataTypeUInt64>()},
        {"parent_data_compressed_bytes",                std::make_shared<DataTypeUInt64>()},
        {"parent_data_uncompressed_bytes",              std::make_shared<DataTypeUInt64>()},
        {"parent_marks_bytes",                          std::make_shared<DataTypeUInt64>()},
        {"modification_time",                           std::make_shared<DataTypeDateTime>()},
        {"remove_time",                                 std::make_shared<DataTypeDateTime>()},
        {"refcount",                                    std::make_shared<DataTypeUInt32>()},
        {"min_date",                                    std::make_shared<DataTypeDate>()},
        {"max_date",                                    std::make_shared<DataTypeDate>()},
        {"min_time",                                    std::make_shared<DataTypeDateTime>()},
        {"max_time",                                    std::make_shared<DataTypeDateTime>()},
        {"partition_id",                                std::make_shared<DataTypeString>()},
        {"min_block_number",                            std::make_shared<DataTypeInt64>()},
        {"max_block_number",                            std::make_shared<DataTypeInt64>()},
        {"level",                                       std::make_shared<DataTypeUInt32>()},
        {"data_version",                                std::make_shared<DataTypeUInt64>()},
        {"primary_key_bytes_in_memory",                 std::make_shared<DataTypeUInt64>()},
        {"primary_key_bytes_in_memory_allocated",       std::make_shared<DataTypeUInt64>()},
        {"is_frozen",                                   std::make_shared<DataTypeUInt8>()},

        {"database",                                    std::make_shared<DataTypeString>()},
        {"table",                                       std::make_shared<DataTypeString>()},
        {"engine",                                      std::make_shared<DataTypeString>()},
        {"disk_name",                                   std::make_shared<DataTypeString>()},
        {"path",                                        std::make_shared<DataTypeString>()},

        {"hash_of_all_files",                           std::make_shared<DataTypeString>()},
        {"hash_of_uncompressed_files",                  std::make_shared<DataTypeString>()},
        {"uncompressed_hash_of_compressed_files",       std::make_shared<DataTypeString>()},

        {"delete_ttl_info_min",                         std::make_shared<DataTypeDateTime>()},
        {"delete_ttl_info_max",                         std::make_shared<DataTypeDateTime>()},

        {"move_ttl_info.expression",                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"move_ttl_info.min",                           std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>())},
        {"move_ttl_info.max",                           std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>())},

        {"default_compression_codec",                   std::make_shared<DataTypeString>()},

        {"recompression_ttl_info.expression",           std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"recompression_ttl_info.min",                  std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>())},
        {"recompression_ttl_info.max",                  std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>())},

        {"group_by_ttl_info.expression",                std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"group_by_ttl_info.min",                       std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>())},
        {"group_by_ttl_info.max",                       std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>())},

        {"rows_where_ttl_info.expression",              std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"rows_where_ttl_info.min",                     std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>())},
        {"rows_where_ttl_info.max",                     std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>())}
    }
    )
{
}

void StorageSystemProjectionParts::processNextStorage(
    ContextPtr, MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column)
{
    using State = IMergeTreeDataPart::State;
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
                columns[res_index++]->insert(part->data_part_storage->getDiskName());
            if (columns_mask[src_index++])
                columns[res_index++]->insert(part->data_part_storage->getFullPath());
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
                columns[res_index++]->insert(getHexUIntLowercase(checksum.first) + getHexUIntLowercase(checksum.second));
            }
            if (columns_mask[src_index++])
            {
                auto checksum = helper.hash_of_uncompressed_files;
                columns[res_index++]->insert(getHexUIntLowercase(checksum.first) + getHexUIntLowercase(checksum.second));
            }
            if (columns_mask[src_index++])
            {
                auto checksum = helper.uncompressed_hash_of_compressed_files;
                columns[res_index++]->insert(getHexUIntLowercase(checksum.first) + getHexUIntLowercase(checksum.second));
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
            columns[res_index++]->insert(queryToString(part->default_codec->getCodecDesc()));

        add_ttl_info_map(part->ttl_infos.recompression_ttl);
        add_ttl_info_map(part->ttl_infos.group_by_ttl);
        add_ttl_info_map(part->ttl_infos.rows_where_ttl);

        /// _state column should be the latest.
        /// Do not use part->getState*, it can be changed from different thread
        if (has_state_column)
            columns[res_index++]->insert(IMergeTreeDataPart::stateString(part_state));
    }
}

}
