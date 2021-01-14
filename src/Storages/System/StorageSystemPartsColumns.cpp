#include "StorageSystemPartsColumns.h"

#include <Common/escapeForFileName.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <Storages/VirtualColumnUtils.h>
#include <Databases/IDatabase.h>
#include <Parsers/queryToString.h>

namespace DB
{


StorageSystemPartsColumns::StorageSystemPartsColumns(const StorageID & table_id_)
    : StorageSystemPartsBase(table_id_,
    {
        {"partition",                                  std::make_shared<DataTypeString>()},
        {"name",                                       std::make_shared<DataTypeString>()},
        {"part_type",                                  std::make_shared<DataTypeString>()},
        {"active",                                     std::make_shared<DataTypeUInt8>()},
        {"marks",                                      std::make_shared<DataTypeUInt64>()},
        {"rows",                                       std::make_shared<DataTypeUInt64>()},
        {"bytes_on_disk",                              std::make_shared<DataTypeUInt64>()},
        {"data_compressed_bytes",                      std::make_shared<DataTypeUInt64>()},
        {"data_uncompressed_bytes",                    std::make_shared<DataTypeUInt64>()},
        {"marks_bytes",                                std::make_shared<DataTypeUInt64>()},
        {"modification_time",                          std::make_shared<DataTypeDateTime>()},
        {"remove_time",                                std::make_shared<DataTypeDateTime>()},
        {"refcount",                                   std::make_shared<DataTypeUInt32>()},
        {"min_date",                                   std::make_shared<DataTypeDate>()},
        {"max_date",                                   std::make_shared<DataTypeDate>()},
        {"partition_id",                               std::make_shared<DataTypeString>()},
        {"min_block_number",                           std::make_shared<DataTypeInt64>()},
        {"max_block_number",                           std::make_shared<DataTypeInt64>()},
        {"level",                                      std::make_shared<DataTypeUInt32>()},
        {"data_version",                               std::make_shared<DataTypeUInt64>()},
        {"primary_key_bytes_in_memory",                std::make_shared<DataTypeUInt64>()},
        {"primary_key_bytes_in_memory_allocated",      std::make_shared<DataTypeUInt64>()},

        {"database",                                   std::make_shared<DataTypeString>()},
        {"table",                                      std::make_shared<DataTypeString>()},
        {"engine",                                     std::make_shared<DataTypeString>()},
        {"disk_name",                                  std::make_shared<DataTypeString>()},
        {"path",                                       std::make_shared<DataTypeString>()},

        {"column",                                     std::make_shared<DataTypeString>()},
        {"type",                                       std::make_shared<DataTypeString>()},
        {"column_position",                            std::make_shared<DataTypeUInt64>()},
        {"default_kind",                               std::make_shared<DataTypeString>()},
        {"default_expression",                         std::make_shared<DataTypeString>()},
        {"column_bytes_on_disk",                       std::make_shared<DataTypeUInt64>()},
        {"column_data_compressed_bytes",               std::make_shared<DataTypeUInt64>()},
        {"column_data_uncompressed_bytes",             std::make_shared<DataTypeUInt64>()},
        {"column_marks_bytes",                         std::make_shared<DataTypeUInt64>()}
    }
    )
{
}

void StorageSystemPartsColumns::processNextStorage(MutableColumns & columns_, const StoragesInfo & info, bool has_state_column)
{
    /// Prepare information about columns in storage.
    struct ColumnInfo
    {
        String default_kind;
        String default_expression;
    };

    std::unordered_map<String, ColumnInfo> columns_info;
    for (const auto & column : info.storage->getInMemoryMetadataPtr()->getColumns())
    {
        ColumnInfo column_info;
        if (column.default_desc.expression)
        {
            column_info.default_kind = toString(column.default_desc.kind);
            column_info.default_expression = queryToString(column.default_desc.expression);
        }

        columns_info[column.name] = column_info;
    }

    /// Go through the list of parts.
    MergeTreeData::DataPartStateVector all_parts_state;
    MergeTreeData::DataPartsVector all_parts;
    all_parts = info.getParts(all_parts_state, has_state_column);
    for (size_t part_number = 0; part_number < all_parts.size(); ++part_number)
    {
        const auto & part = all_parts[part_number];
        auto part_state = all_parts_state[part_number];
        auto columns_size = part->getTotalColumnsSize();

        /// For convenience, in returned refcount, don't add references that was due to local variables in this method: all_parts, active_parts.
        auto use_count = part.use_count() - 1;
        auto min_date = part->getMinDate();
        auto max_date = part->getMaxDate();
        auto index_size_in_bytes = part->getIndexSizeInBytes();
        auto index_size_in_allocated_bytes = part->getIndexSizeInAllocatedBytes();

        using State = IMergeTreeDataPart::State;

        size_t column_position = 0;
        for (const auto & column : part->getColumns())
        {
            ++column_position;
            size_t j = 0;
            {
                WriteBufferFromOwnString out;
                part->partition.serializeText(*info.data, out, format_settings);
                columns_[j++]->insert(out.str());
            }
            columns_[j++]->insert(part->name);
            columns_[j++]->insert(part->getTypeName());
            columns_[j++]->insert(part_state == State::Committed);
            columns_[j++]->insert(part->getMarksCount());

            columns_[j++]->insert(part->rows_count);
            columns_[j++]->insert(part->getBytesOnDisk());
            columns_[j++]->insert(columns_size.data_compressed);
            columns_[j++]->insert(columns_size.data_uncompressed);
            columns_[j++]->insert(columns_size.marks);
            columns_[j++]->insert(UInt64(part->modification_time));
            columns_[j++]->insert(UInt64(part->remove_time.load(std::memory_order_relaxed)));

            columns_[j++]->insert(UInt64(use_count));

            columns_[j++]->insert(min_date);
            columns_[j++]->insert(max_date);
            columns_[j++]->insert(part->info.partition_id);
            columns_[j++]->insert(part->info.min_block);
            columns_[j++]->insert(part->info.max_block);
            columns_[j++]->insert(part->info.level);
            columns_[j++]->insert(UInt64(part->info.getDataVersion()));
            columns_[j++]->insert(index_size_in_bytes);
            columns_[j++]->insert(index_size_in_allocated_bytes);

            columns_[j++]->insert(info.database);
            columns_[j++]->insert(info.table);
            columns_[j++]->insert(info.engine);
            columns_[j++]->insert(part->volume->getDisk()->getName());
            columns_[j++]->insert(part->getFullPath());

            columns_[j++]->insert(column.name);
            columns_[j++]->insert(column.type->getName());
            columns_[j++]->insert(column_position);

            auto column_info_it = columns_info.find(column.name);
            if (column_info_it != columns_info.end())
            {
                columns_[j++]->insert(column_info_it->second.default_kind);
                columns_[j++]->insert(column_info_it->second.default_expression);
            }
            else
            {
                columns_[j++]->insertDefault();
                columns_[j++]->insertDefault();
            }

            ColumnSize column_size = part->getColumnSize(column.name, *column.type);
            columns_[j++]->insert(column_size.data_compressed + column_size.marks);
            columns_[j++]->insert(column_size.data_compressed);
            columns_[j++]->insert(column_size.data_uncompressed);
            columns_[j++]->insert(column_size.marks);

            if (has_state_column)
                columns_[j++]->insert(part->stateString());
        }
    }
}

}
