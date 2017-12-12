#include <Common/escapeForFileName.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemPartsColumns.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/VirtualColumnUtils.h>
#include <Databases/IDatabase.h>
#include <Parsers/queryToString.h>

namespace DB
{


StorageSystemPartsColumns::StorageSystemPartsColumns(const std::string & name)
        : StorageSystemPartsBase(name,
              {
                      {"partition",           std::make_shared<DataTypeString>()},
                      {"name",                std::make_shared<DataTypeString>()},
                      {"active",              std::make_shared<DataTypeUInt8>()},
                      {"marks",               std::make_shared<DataTypeUInt64>()},
                      {"marks_bytes_in_block",        std::make_shared<DataTypeUInt64>()},
                      {"rows",                std::make_shared<DataTypeUInt64>()},
                      {"bytes",               std::make_shared<DataTypeUInt64>()},
                      {"modification_time",   std::make_shared<DataTypeDateTime>()},
                      {"remove_time",         std::make_shared<DataTypeDateTime>()},
                      {"refcount",            std::make_shared<DataTypeUInt32>()},
                      {"min_date",            std::make_shared<DataTypeDate>()},
                      {"max_date",            std::make_shared<DataTypeDate>()},
                      {"min_block_number",    std::make_shared<DataTypeInt64>()},
                      {"max_block_number",    std::make_shared<DataTypeInt64>()},
                      {"level",               std::make_shared<DataTypeUInt32>()},
                      {"primary_key_bytes_in_memory", std::make_shared<DataTypeUInt64>()},
                      {"primary_key_bytes_in_memory_allocated", std::make_shared<DataTypeUInt64>()},

                      {"database",            std::make_shared<DataTypeString>()},
                      {"table",               std::make_shared<DataTypeString>()},
                      {"engine",              std::make_shared<DataTypeString>()},
                      {"column",              std::make_shared<DataTypeString>()},
                      { "type",               std::make_shared<DataTypeString>() },
                      { "default_kind",       std::make_shared<DataTypeString>() },
                      { "default_expression", std::make_shared<DataTypeString>() },
                      { "data_compressed_bytes",      std::make_shared<DataTypeUInt64>() },
                      { "data_uncompressed_bytes",    std::make_shared<DataTypeUInt64>() },
                      { "marks_bytes_in_column",      std::make_shared<DataTypeUInt64>() },
              }
        )
{
}

void StorageSystemPartsColumns::processNextStorage(Block & block, const StoragesInfo & info, bool has_state_column)
{
    /// Prepare information about columns in storage.
    struct ColumnInfo
    {
        String default_kind;
        String default_expression;
    };

    NamesAndTypesList columns = info.storage->getColumnsList();
    columns.insert(std::end(columns), std::begin(info.storage->alias_columns), std::end(info.storage->alias_columns));
    column_defaults = info.storage->column_defaults;
    std::unordered_map<String, ColumnInfo> columns_info;

    for (const auto & column : columns)
    {
        ColumnInfo column_info;

        const auto it = column_defaults.find(column.name);
        if (it != std::end(column_defaults))
        {
            column_info.default_kind = toString(it->second.type);
            column_info.default_expression = queryToString(it->second.expression);
        }

        columns_info[column.name] = column_info;
    }

    /// Go through the list of parts.
    for (size_t part_number = 0; part_number < info.all_parts.size(); ++part_number)
    {
        const auto & part = info.all_parts[part_number];
        auto part_state = info.all_parts_state[part_number];

        auto total_mrk_size_in_bytes = part->getTotalMrkSizeInBytes();
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
                part->partition.serializeTextQuoted(*info.data, out);
                block.getByPosition(j++).column->insert(out.str());
            }
            block.getByPosition(j++).column->insert(part->name);
            block.getByPosition(j++).column->insert(static_cast<UInt64>(part_state == State::Committed));
            block.getByPosition(j++).column->insert(static_cast<UInt64>(part->marks_count));
            block.getByPosition(j++).column->insert(static_cast<UInt64>(total_mrk_size_in_bytes));

            block.getByPosition(j++).column->insert(static_cast<UInt64>(part->rows_count));
            block.getByPosition(j++).column->insert(static_cast<UInt64>(part->size_in_bytes));
            block.getByPosition(j++).column->insert(static_cast<UInt64>(part->modification_time));
            block.getByPosition(j++).column->insert(static_cast<UInt64>(part->remove_time));

            block.getByPosition(j++).column->insert(static_cast<UInt64>(use_count));

            block.getByPosition(j++).column->insert(static_cast<UInt64>(min_date));
            block.getByPosition(j++).column->insert(static_cast<UInt64>(max_date));
            block.getByPosition(j++).column->insert(part->info.min_block);
            block.getByPosition(j++).column->insert(part->info.max_block);
            block.getByPosition(j++).column->insert(static_cast<UInt64>(part->info.level));
            block.getByPosition(j++).column->insert(static_cast<UInt64>(index_size_in_bytes));
            block.getByPosition(j++).column->insert(static_cast<UInt64>(index_size_in_allocated_bytes));

            block.getByPosition(j++).column->insert(info.database);
            block.getByPosition(j++).column->insert(info.table);
            block.getByPosition(j++).column->insert(info.engine);
            block.getByPosition(j++).column->insert(column.name);
            block.getByPosition(j++).column->insert(column.type->getName());

            auto column_info_it = columns_info.find(column.name);
            if (column_info_it != columns_info.end())
            {
                block.getByPosition(j++).column->insert(column_info_it->second.default_kind);
                block.getByPosition(j++).column->insert(column_info_it->second.default_expression);
            }
            else
            {
                block.getByPosition(j++).column->insertDefault();
                block.getByPosition(j++).column->insertDefault();
            }

            block.getByPosition(j++).column->insert(part->getColumnCompressedSize(column.name));
            block.getByPosition(j++).column->insert(part->getColumnUncompressedSize(column.name));
            block.getByPosition(j++).column->insert(part->getColumnMrkSize(column.name));

            if (has_state_column)
                block.getByPosition(j++).column->insert(part->stateString());
        }
    }
}

}
