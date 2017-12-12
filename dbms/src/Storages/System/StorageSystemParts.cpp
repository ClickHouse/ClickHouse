#include <Common/escapeForFileName.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemParts.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/VirtualColumnUtils.h>
#include <Databases/IDatabase.h>

namespace DB
{

StorageSystemParts::StorageSystemParts(const std::string & name)
    : StorageSystemPartsBase(name,
    {
        {"partition",           std::make_shared<DataTypeString>()},
        {"name",                std::make_shared<DataTypeString>()},
        {"active",              std::make_shared<DataTypeUInt8>()},
        {"marks",               std::make_shared<DataTypeUInt64>()},
        {"marks_size",          std::make_shared<DataTypeUInt64>()},
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
        {"engine",              std::make_shared<DataTypeString>()}
    }
    )
{
}

void StorageSystemParts::processNextStorage(Block & block, const StoragesInfo & info, bool has_state_column)
{
    using State = MergeTreeDataPart::State;

    for (size_t part_number = 0; part_number < info.all_parts.size(); ++part_number)
    {
        const auto & part = info.all_parts[part_number];
        auto part_state = info.all_parts_state[part_number];

        size_t i = 0;
        {
            WriteBufferFromOwnString out;
            part->partition.serializeTextQuoted(*info.data, out);
            block.getByPosition(i++).column->insert(out.str());
        }
        block.getByPosition(i++).column->insert(part->name);
        block.getByPosition(i++).column->insert(static_cast<UInt64>(part_state == State::Committed));
        block.getByPosition(i++).column->insert(static_cast<UInt64>(part->marks_count));
        block.getByPosition(i++).column->insert(static_cast<UInt64>(part->getTotalMrkSizeInBytes()));
        block.getByPosition(i++).column->insert(static_cast<UInt64>(part->rows_count));
        block.getByPosition(i++).column->insert(static_cast<UInt64>(part->size_in_bytes));
        block.getByPosition(i++).column->insert(static_cast<UInt64>(part->modification_time));
        block.getByPosition(i++).column->insert(static_cast<UInt64>(part->remove_time));

        /// For convenience, in returned refcount, don't add references that was due to local variables in this method: all_parts, active_parts.
        block.getByPosition(i++).column->insert(static_cast<UInt64>(part.use_count() - 1));

        block.getByPosition(i++).column->insert(static_cast<UInt64>(part->getMinDate()));
        block.getByPosition(i++).column->insert(static_cast<UInt64>(part->getMaxDate()));
        block.getByPosition(i++).column->insert(part->info.min_block);
        block.getByPosition(i++).column->insert(part->info.max_block);
        block.getByPosition(i++).column->insert(static_cast<UInt64>(part->info.level));
        block.getByPosition(i++).column->insert(static_cast<UInt64>(part->getIndexSizeInBytes()));
        block.getByPosition(i++).column->insert(static_cast<UInt64>(part->getIndexSizeInAllocatedBytes()));

        block.getByPosition(i++).column->insert(info.database);
        block.getByPosition(i++).column->insert(info.table);
        block.getByPosition(i++).column->insert(info.engine);

        if (has_state_column)
            block.getByPosition(i++).column->insert(part->stateString());
    }
}

}
