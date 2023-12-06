#include <Storages/System/StorageSystemDroppedTablesParts.h>
#include <atomic>
#include <memory>
#include <string_view>

#include <Storages/MergeTree/MergeTreeData.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeUUID.h>
#include <Parsers/queryToString.h>
#include <Interpreters/TransactionVersionMetadata.h>
#include <Interpreters/Context.h>


namespace
{

std::string_view getRemovalStateDescription(DB::DataPartRemovalState state)
{
    switch (state)
    {
    case DB::DataPartRemovalState::NOT_ATTEMPTED:
        return "Cleanup thread hasn't seen this part yet";
    case DB::DataPartRemovalState::VISIBLE_TO_TRANSACTIONS:
        return "Part maybe visible for transactions";
    case DB::DataPartRemovalState::NON_UNIQUE_OWNERSHIP:
        return "Part ownership is not unique";
    case DB::DataPartRemovalState::NOT_REACHED_REMOVAL_TIME:
        return "Part hasn't reached removal time yet";
    case DB::DataPartRemovalState::HAS_SKIPPED_MUTATION_PARENT:
        return "Waiting mutation parent to be removed";
    case DB::DataPartRemovalState::EMPTY_PART_COVERS_OTHER_PARTS:
        return "Waiting for covered parts to be removed first";
    case DB::DataPartRemovalState::REMOVED:
        return "Part was selected to be removed";
    }
}

}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

NamesAndTypesList StorageSystemDroppedTablesParts::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
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
        {"primary_key_size",                            std::make_shared<DataTypeUInt64>()},
        {"marks_bytes",                                 std::make_shared<DataTypeUInt64>()},
        {"secondary_indices_compressed_bytes",          std::make_shared<DataTypeUInt64>()},
        {"secondary_indices_uncompressed_bytes",        std::make_shared<DataTypeUInt64>()},
        {"secondary_indices_marks_bytes",               std::make_shared<DataTypeUInt64>()},
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
        {"rows_where_ttl_info.max",                     std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>())},

        {"projections",                                 std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},

        {"visible",                                     std::make_shared<DataTypeUInt8>()},
        {"creation_tid",                                getTransactionIDDataType()},
        {"removal_tid_lock",                            std::make_shared<DataTypeUInt64>()},
        {"removal_tid",                                 getTransactionIDDataType()},
        {"creation_csn",                                std::make_shared<DataTypeUInt64>()},
        {"removal_csn",                                 std::make_shared<DataTypeUInt64>()},

        {"has_lightweight_delete",                      std::make_shared<DataTypeUInt8>()},

        {"last_removal_attempt_time",                    std::make_shared<DataTypeDateTime>()},
        {"removal_state",                               std::make_shared<DataTypeString>()},
    };
    return names_and_types;
}


void StorageSystemDroppedTablesParts::fillData(MutableColumns & columns, ContextPtr context, const SelectQueryInfo &) const
{
    auto tables_mark_dropped = DatabaseCatalog::instance().getTablesMarkedDropped();

    for (const auto & storage : tables_mark_dropped)
    {
        const auto * data = dynamic_cast<MergeTreeData *>(storage.table.get());
        if (!data)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown engine {}", storage.table->getName());

        using State = MergeTreeData::DataPartState;

        MergeTreeData::DataPartStateVector all_parts_state;
        auto all_parts = data->getDataPartsVectorForInternalUsage({State::Active, State::Outdated}, &all_parts_state);

        for (size_t part_number = 0; part_number < all_parts.size(); ++part_number)
        {
            const auto & part = all_parts[part_number];
            auto part_state = all_parts_state[part_number];

            ColumnSize columns_size = part->getTotalColumnsSize();
            ColumnSize secondary_indexes_size = part->getTotalSeconaryIndicesSize();

            size_t res_index = 0;

            {
                WriteBufferFromOwnString out;
                part->partition.serializeText(*data, out, FormatSettings{});
                columns[res_index++]->insert(out.str());
            }
            columns[res_index++]->insert(part->name);
            columns[res_index++]->insert(part->uuid);
            columns[res_index++]->insert(part->getTypeName());
            columns[res_index++]->insert(part_state == State::Active);


            columns[res_index++]->insert(part->getMarksCount());
            columns[res_index++]->insert(part->rows_count);
            columns[res_index++]->insert(part->getBytesOnDisk());
            columns[res_index++]->insert(columns_size.data_compressed);
            columns[res_index++]->insert(columns_size.data_uncompressed);
            columns[res_index++]->insert(part->getIndexSizeFromFile());
            columns[res_index++]->insert(columns_size.marks);
            columns[res_index++]->insert(secondary_indexes_size.data_compressed);
            columns[res_index++]->insert(secondary_indexes_size.data_uncompressed);
            columns[res_index++]->insert(secondary_indexes_size.marks);
            columns[res_index++]->insert(static_cast<UInt64>(part->modification_time));
            {
                time_t remove_time = part->remove_time.load(std::memory_order_relaxed);
                columns[res_index++]->insert(static_cast<UInt64>(remove_time == std::numeric_limits<time_t>::max() ? 0 : remove_time));
            }

            /// For convenience, in returned refcount, don't add references that was due to local variables in this method: all_parts, active_parts.
            columns[res_index++]->insert(static_cast<UInt64>(part.use_count() - 1));

            auto min_max_date = part->getMinMaxDate();
            auto min_max_time = part->getMinMaxTime();

            columns[res_index++]->insert(min_max_date.first);
            columns[res_index++]->insert(min_max_date.second);
            columns[res_index++]->insert(static_cast<UInt32>(min_max_time.first));
            columns[res_index++]->insert(static_cast<UInt32>(min_max_time.second));
            columns[res_index++]->insert(part->info.partition_id);
            columns[res_index++]->insert(part->info.min_block);
            columns[res_index++]->insert(part->info.max_block);
            columns[res_index++]->insert(part->info.level);
            columns[res_index++]->insert(static_cast<UInt64>(part->info.getDataVersion()));
            columns[res_index++]->insert(part->getIndexSizeInBytes());
            columns[res_index++]->insert(part->getIndexSizeInAllocatedBytes());
            columns[res_index++]->insert(part->is_frozen.load(std::memory_order_relaxed));

            columns[res_index++]->insert(storage.table->getStorageID().getDatabaseName());
            columns[res_index++]->insert(storage.table->getStorageID().getTableName());
            columns[res_index++]->insert(storage.table->getName());

            {
                if (part->isStoredOnDisk())
                    columns[res_index++]->insert(part->getDataPartStorage().getDiskName());
                else
                    columns[res_index++]->insertDefault();
            }

            /// The full path changes at clean up thread, so do not read it if parts can be deleted, avoid the race.
            if (part->isStoredOnDisk()
                && part_state != State::Deleting && part_state != State::DeleteOnDestroy && part_state != State::Temporary)
            {
                columns[res_index++]->insert(part->getDataPartStorage().getFullPath());
            }
            else
                columns[res_index++]->insertDefault();


            {
                MinimalisticDataPartChecksums helper;
                helper.computeTotalChecksums(part->checksums);

                columns[res_index++]->insert(getHexUIntLowercase(helper.hash_of_all_files));
                columns[res_index++]->insert(getHexUIntLowercase(helper.hash_of_uncompressed_files));
                columns[res_index++]->insert(getHexUIntLowercase(helper.uncompressed_hash_of_compressed_files));
            }

            /// delete_ttl_info
            columns[res_index++]->insert(static_cast<UInt32>(part->ttl_infos.table_ttl.min));
            columns[res_index++]->insert(static_cast<UInt32>(part->ttl_infos.table_ttl.max));

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
                columns[res_index++]->insert(expression_array);
                columns[res_index++]->insert(min_array);
                columns[res_index++]->insert(max_array);
            };

            add_ttl_info_map(part->ttl_infos.moves_ttl);

            columns[res_index++]->insert(queryToString(part->default_codec->getCodecDesc()));

            add_ttl_info_map(part->ttl_infos.recompression_ttl);
            add_ttl_info_map(part->ttl_infos.group_by_ttl);
            add_ttl_info_map(part->ttl_infos.rows_where_ttl);

            Array projections;
            for (const auto & [name, _] : part->getProjectionParts())
                projections.push_back(name);

            columns[res_index++]->insert(projections);

            {
                auto txn = context->getCurrentTransaction();
                if (txn)
                    columns[res_index++]->insert(part->version.isVisible(*txn));
                else
                    columns[res_index++]->insert(part_state == State::Active);
            }

            auto get_tid_as_field = [](const TransactionID & tid) -> Field
            {
                return Tuple{tid.start_csn, tid.local_tid, tid.host_id};
            };

            columns[res_index++]->insert(get_tid_as_field(part->version.creation_tid));
            columns[res_index++]->insert(part->version.removal_tid_lock.load(std::memory_order_relaxed));
            columns[res_index++]->insert(get_tid_as_field(part->version.getRemovalTID()));
            columns[res_index++]->insert(part->version.creation_csn.load(std::memory_order_relaxed));
            columns[res_index++]->insert(part->version.removal_csn.load(std::memory_order_relaxed));
            columns[res_index++]->insert(part->hasLightweightDelete());
            columns[res_index++]->insert(static_cast<UInt64>(part->last_removal_attempt_time.load(std::memory_order_relaxed)));
            columns[res_index++]->insert(getRemovalStateDescription(part->removal_state.load(std::memory_order_relaxed)));

        }
        
    }

}


}
