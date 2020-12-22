#include "StorageSystemParts.h"

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

StorageSystemParts::StorageSystemParts(const StorageID & table_id_)
    : StorageSystemPartsBase(table_id_,
    {
        {"partition",                                   std::make_shared<DataTypeString>()},
        {"name",                                        std::make_shared<DataTypeString>()},
        {"uuid",                                        std::make_shared<DataTypeUUID>()},
        {"part_type",                                   std::make_shared<DataTypeString>()},
        {"active",                                      std::make_shared<DataTypeUInt8>()},
        {"marks",                                       std::make_shared<DataTypeUInt64>()},
        {"rows",                                        std::make_shared<DataTypeUInt64>()},
        {"bytes_on_disk",                               std::make_shared<DataTypeUInt64>()},
        {"data_compressed_bytes",                       std::make_shared<DataTypeUInt64>()},
        {"data_uncompressed_bytes",                     std::make_shared<DataTypeUInt64>()},
        {"marks_bytes",                                 std::make_shared<DataTypeUInt64>()},
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
    }
    )
{
}

void StorageSystemParts::processNextStorage(MutableColumns & columns_, const StoragesInfo & info, bool has_state_column)
{
    using State = IMergeTreeDataPart::State;
    MergeTreeData::DataPartStateVector all_parts_state;
    MergeTreeData::DataPartsVector all_parts;

    all_parts = info.getParts(all_parts_state, has_state_column);

    for (size_t part_number = 0; part_number < all_parts.size(); ++part_number)
    {
        const auto & part = all_parts[part_number];
        auto part_state = all_parts_state[part_number];

        ColumnSize columns_size = part->getTotalColumnsSize();

        size_t i = 0;
        {
            WriteBufferFromOwnString out;
            part->partition.serializeText(*info.data, out, format_settings);
            columns_[i++]->insert(out.str());
        }
        columns_[i++]->insert(part->name);
        columns_[i++]->insert(part->uuid);
        columns_[i++]->insert(part->getTypeName());
        columns_[i++]->insert(part_state == State::Committed);
        columns_[i++]->insert(part->getMarksCount());
        columns_[i++]->insert(part->rows_count);
        columns_[i++]->insert(part->getBytesOnDisk());
        columns_[i++]->insert(columns_size.data_compressed);
        columns_[i++]->insert(columns_size.data_uncompressed);
        columns_[i++]->insert(columns_size.marks);
        columns_[i++]->insert(static_cast<UInt64>(part->modification_time));

        time_t remove_time = part->remove_time.load(std::memory_order_relaxed);
        columns_[i++]->insert(static_cast<UInt64>(remove_time == std::numeric_limits<time_t>::max() ? 0 : remove_time));

        /// For convenience, in returned refcount, don't add references that was due to local variables in this method: all_parts, active_parts.
        columns_[i++]->insert(static_cast<UInt64>(part.use_count() - 1));

        columns_[i++]->insert(part->getMinDate());
        columns_[i++]->insert(part->getMaxDate());
        columns_[i++]->insert(static_cast<UInt32>(part->getMinTime()));
        columns_[i++]->insert(static_cast<UInt32>(part->getMaxTime()));
        columns_[i++]->insert(part->info.partition_id);
        columns_[i++]->insert(part->info.min_block);
        columns_[i++]->insert(part->info.max_block);
        columns_[i++]->insert(part->info.level);
        columns_[i++]->insert(static_cast<UInt64>(part->info.getDataVersion()));
        columns_[i++]->insert(part->getIndexSizeInBytes());
        columns_[i++]->insert(part->getIndexSizeInAllocatedBytes());
        columns_[i++]->insert(part->is_frozen.load(std::memory_order_relaxed));

        columns_[i++]->insert(info.database);
        columns_[i++]->insert(info.table);
        columns_[i++]->insert(info.engine);
        if (part->isStoredOnDisk())
        {
            columns_[i++]->insert(part->volume->getDisk()->getName());
            columns_[i++]->insert(part->getFullPath());
        }
        else
        {
            columns_[i++]->insertDefault();
            columns_[i++]->insertDefault();
        }

        if (has_state_column)
            columns_[i++]->insert(part->stateString());

        MinimalisticDataPartChecksums helper;
        helper.computeTotalChecksums(part->checksums);

        auto checksum = helper.hash_of_all_files;
        columns_[i++]->insert(getHexUIntLowercase(checksum.first) + getHexUIntLowercase(checksum.second));

        checksum = helper.hash_of_uncompressed_files;
        columns_[i++]->insert(getHexUIntLowercase(checksum.first) + getHexUIntLowercase(checksum.second));

        checksum = helper.uncompressed_hash_of_compressed_files;
        columns_[i++]->insert(getHexUIntLowercase(checksum.first) + getHexUIntLowercase(checksum.second));

        /// delete_ttl_info
        {
            columns_[i++]->insert(static_cast<UInt32>(part->ttl_infos.table_ttl.min));
            columns_[i++]->insert(static_cast<UInt32>(part->ttl_infos.table_ttl.max));
        }

        auto add_ttl_info_map = [&](const TTLInfoMap & ttl_info_map)
        {
            Array expression_array;
            Array min_array;
            Array max_array;
            expression_array.reserve(ttl_info_map.size());
            min_array.reserve(ttl_info_map.size());
            max_array.reserve(ttl_info_map.size());
            for (const auto & [expression, ttl_info] : ttl_info_map)
            {
                expression_array.emplace_back(expression);
                min_array.push_back(static_cast<UInt32>(ttl_info.min));
                max_array.push_back(static_cast<UInt32>(ttl_info.max));
            }
            columns_[i++]->insert(expression_array);
            columns_[i++]->insert(min_array);
            columns_[i++]->insert(max_array);
        };

        add_ttl_info_map(part->ttl_infos.moves_ttl);

        columns_[i++]->insert(queryToString(part->default_codec->getCodecDesc()));

        add_ttl_info_map(part->ttl_infos.recompression_ttl);
    }
}

}
