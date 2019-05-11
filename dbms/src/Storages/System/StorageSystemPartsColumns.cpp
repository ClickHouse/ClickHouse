#include <Common/escapeForFileName.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemPartsColumns.h>
#include <Storages/VirtualColumnUtils.h>
#include <Databases/IDatabase.h>
#include <Parsers/queryToString.h>

namespace DB
{


StorageSystemPartsColumns::StorageSystemPartsColumns(const std::string & name)
    : StorageSystemPartsBase(name,
    {
        {"partition",                                  std::make_shared<DataTypeString>()},
        {"name",                                       std::make_shared<DataTypeString>()},
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
        {"path",                                       std::make_shared<DataTypeString>()},

        {"column",                                     std::make_shared<DataTypeString>()},
        {"type",                                       std::make_shared<DataTypeString>() },
        {"default_kind",                               std::make_shared<DataTypeString>() },
        {"default_expression",                         std::make_shared<DataTypeString>() },
        {"column_bytes_on_disk",                       std::make_shared<DataTypeUInt64>() },
        {"column_data_compressed_bytes",               std::make_shared<DataTypeUInt64>() },
        {"column_data_uncompressed_bytes",             std::make_shared<DataTypeUInt64>() },
        {"column_marks_bytes",                         std::make_shared<DataTypeUInt64>() },
    }
    )
{
}

void StorageSystemPartsColumns::processNextStorage(MutableColumns & columns, const StoragesInfo & info, bool has_state_column)
{
    /// Prepare information about columns in storage.
    struct ColumnInfo
    {
        String default_kind;
        String default_expression;
    };

    std::unordered_map<String, ColumnInfo> columns_info;
    for (const auto & column : info.storage->getColumns())
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
    for (size_t part_number = 0; part_number < info.all_parts.size(); ++part_number)
    {
        const auto & part = info.all_parts[part_number];
        auto part_state = info.all_parts_state[part_number];
        auto columns_size = part->getTotalColumnsSize();

        /// For convenience, in returned refcount, don't add references that was due to local variables in this method: all_parts, active_parts.
        auto use_count = part.use_count() - 1;
        auto min_date = part->getMinDate();
        auto max_date = part->getMaxDate();
        auto index_size_in_bytes = part->getIndexSizeInBytes();
        auto index_size_in_allocated_bytes = part->getIndexSizeInAllocatedBytes();

        using State = MergeTreeDataPart::State;

        for (const auto & column : part->columns)
        {
            size_t j = 0;
            {
                WriteBufferFromOwnString out;
                part->partition.serializeText(*info.data, out, format_settings);
                columns[j++]->insert(out.str());
            }
            columns[j++]->insert(part->name);
            columns[j++]->insert(part_state == State::Committed);
            columns[j++]->insert(part->getMarksCount());

            columns[j++]->insert(part->rows_count);
            columns[j++]->insert(part->bytes_on_disk.load(std::memory_order_relaxed));
            columns[j++]->insert(columns_size.data_compressed);
            columns[j++]->insert(columns_size.data_uncompressed);
            columns[j++]->insert(columns_size.marks);
            columns[j++]->insert(UInt64(part->modification_time));
            columns[j++]->insert(UInt64(part->remove_time.load(std::memory_order_relaxed)));

            columns[j++]->insert(UInt64(use_count));

            columns[j++]->insert(min_date);
            columns[j++]->insert(max_date);
            columns[j++]->insert(part->info.partition_id);
            columns[j++]->insert(part->info.min_block);
            columns[j++]->insert(part->info.max_block);
            columns[j++]->insert(part->info.level);
            columns[j++]->insert(UInt64(part->info.getDataVersion()));
            columns[j++]->insert(index_size_in_bytes);
            columns[j++]->insert(index_size_in_allocated_bytes);

            columns[j++]->insert(info.database);
            columns[j++]->insert(info.table);
            columns[j++]->insert(info.engine);
            columns[j++]->insert(part->getFullPath());
            columns[j++]->insert(column.name);
            columns[j++]->insert(column.type->getName());

            auto column_info_it = columns_info.find(column.name);
            if (column_info_it != columns_info.end())
            {
                columns[j++]->insert(column_info_it->second.default_kind);
                columns[j++]->insert(column_info_it->second.default_expression);
            }
            else
            {
                columns[j++]->insertDefault();
                columns[j++]->insertDefault();
            }

            MergeTreeDataPart::ColumnSize column_size = part->getColumnSize(column.name, *column.type);
            columns[j++]->insert(column_size.data_compressed + column_size.marks);
            columns[j++]->insert(column_size.data_compressed);
            columns[j++]->insert(column_size.data_uncompressed);
            columns[j++]->insert(column_size.marks);

            if (has_state_column)
                columns[j++]->insert(part->stateString());
        }
    }
}

}
