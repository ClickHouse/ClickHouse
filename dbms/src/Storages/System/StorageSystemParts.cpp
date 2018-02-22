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
        {"engine",              std::make_shared<DataTypeString>()},
        {"path",                std::make_shared<DataTypeString>()}
    }
    )
{
}

void StorageSystemParts::processNextStorage(MutableColumns & columns, const StoragesInfo & info, bool has_state_column)
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
            columns[i++]->insert(out.str());
        }
        columns[i++]->insert(part->name);
        columns[i++]->insert(static_cast<UInt64>(part_state == State::Committed));
        columns[i++]->insert(static_cast<UInt64>(part->marks_count));
        columns[i++]->insert(static_cast<UInt64>(part->getTotalMrkSizeInBytes()));
        columns[i++]->insert(static_cast<UInt64>(part->rows_count));
        columns[i++]->insert(static_cast<UInt64>(part->size_in_bytes));
        columns[i++]->insert(static_cast<UInt64>(part->modification_time));
        columns[i++]->insert(static_cast<UInt64>(part->remove_time));

        /// For convenience, in returned refcount, don't add references that was due to local variables in this method: all_parts, active_parts.
        columns[i++]->insert(static_cast<UInt64>(part.use_count() - 1));

        columns[i++]->insert(static_cast<UInt64>(part->getMinDate()));
        columns[i++]->insert(static_cast<UInt64>(part->getMaxDate()));
        columns[i++]->insert(part->info.min_block);
        columns[i++]->insert(part->info.max_block);
        columns[i++]->insert(static_cast<UInt64>(part->info.level));
        columns[i++]->insert(static_cast<UInt64>(part->getIndexSizeInBytes()));
        columns[i++]->insert(static_cast<UInt64>(part->getIndexSizeInAllocatedBytes()));

        columns[i++]->insert(info.database);
        columns[i++]->insert(info.table);
        columns[i++]->insert(info.engine);
        columns[i++]->insert(part->getFullPath());

        if (has_state_column)
            columns[i++]->insert(part->stateString());
    }
}

}
