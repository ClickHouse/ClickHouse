#include <Storages/System/StorageSystemParts.h>
#include <atomic>
#include <memory>
#include <string_view>

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

StorageSystemParts::StorageSystemParts(const StorageID & table_id_)
    : StorageSystemPartsBase(table_id_,
    ColumnsDescription{
        {"partition",                                   std::make_shared<DataTypeString>(),    "The partition name."},
        {"name",                                        std::make_shared<DataTypeString>(),    "Name of the data part."},
        {"uuid",                                        std::make_shared<DataTypeUUID>(),      "The UUID of data part."},
        {"part_type",                                   std::make_shared<DataTypeString>(),    "The data part storing format. Possible Values: Wide (a file per column) and Compact (a single file for all columns)."},
        {"active",                                      std::make_shared<DataTypeUInt8>(),     "Flag that indicates whether the data part is active. If a data part is active, it's used in a table. Otherwise, it's about to be deleted. Inactive data parts appear after merging and mutating operations."},
        {"marks",                                       std::make_shared<DataTypeUInt64>(),    "The number of marks. To get the approximate number of rows in a data part, multiply marks by the index granularity (usually 8192) (this hint does not work for adaptive granularity)."},
        {"rows",                                        std::make_shared<DataTypeUInt64>(),    "The number of rows."},
        {"bytes_on_disk",                               std::make_shared<DataTypeUInt64>(),    "Total size of all the data part files in bytes."},
        {"data_compressed_bytes",                       std::make_shared<DataTypeUInt64>(),    "Total size of compressed data in the data part. All the auxiliary files (for example, files with marks) are not included."},
        {"data_uncompressed_bytes",                     std::make_shared<DataTypeUInt64>(),    "Total size of uncompressed data in the data part. All the auxiliary files (for example, files with marks) are not included."},
        {"primary_key_size",                            std::make_shared<DataTypeUInt64>(),    "The amount of memory (in bytes) used by primary key values in the primary.idx/cidx file on disk."},
        {"marks_bytes",                                 std::make_shared<DataTypeUInt64>(),    "The size of the file with marks."},
        {"secondary_indices_compressed_bytes",          std::make_shared<DataTypeUInt64>(),    "Total size of compressed data for secondary indices in the data part. All the auxiliary files (for example, files with marks) are not included."},
        {"secondary_indices_uncompressed_bytes",        std::make_shared<DataTypeUInt64>(),    "Total size of uncompressed data for secondary indices in the data part. All the auxiliary files (for example, files with marks) are not included."},
        {"secondary_indices_marks_bytes",               std::make_shared<DataTypeUInt64>(),    "The size of the file with marks for secondary indices."},
        {"modification_time",                           std::make_shared<DataTypeDateTime>(),  "The time the directory with the data part was modified. This usually corresponds to the time of data part creation."},
        {"remove_time",                                 std::make_shared<DataTypeDateTime>(),  "The time when the data part became inactive."},
        {"refcount",                                    std::make_shared<DataTypeUInt32>(),    "The number of places where the data part is used. A value greater than 2 indicates that the data part is used in queries or merges."},
        {"min_date",                                    std::make_shared<DataTypeDate>(),      "The minimum value of the date key in the data part."},
        {"max_date",                                    std::make_shared<DataTypeDate>(),      "The maximum value of the date key in the data part."},
        {"min_time",                                    std::make_shared<DataTypeDateTime>(),  "The minimum value of the date and time key in the data part."},
        {"max_time",                                    std::make_shared<DataTypeDateTime>(),  "The maximum value of the date and time key in the data part."},
        {"partition_id",                                std::make_shared<DataTypeString>(),    "ID of the partition."},
        {"min_block_number",                            std::make_shared<DataTypeInt64>(),     "The minimum number of data parts that make up the current part after merging."},
        {"max_block_number",                            std::make_shared<DataTypeInt64>(),     "The maximum number of data parts that make up the current part after merging."},
        {"level",                                       std::make_shared<DataTypeUInt32>(),    "Depth of the merge tree. Zero means that the current part was created by insert rather than by merging other parts."},
        {"data_version",                                std::make_shared<DataTypeUInt64>(),    "Number that is used to determine which mutations should be applied to the data part (mutations with a version higher than data_version)."},
        {"primary_key_bytes_in_memory",                 std::make_shared<DataTypeUInt64>(),    "The amount of memory (in bytes) used by primary key values."},
        {"primary_key_bytes_in_memory_allocated",       std::make_shared<DataTypeUInt64>(),    "The amount of memory (in bytes) reserved for primary key values."},
        {"is_frozen",                                   std::make_shared<DataTypeUInt8>(),     "Flag that shows that a partition data backup exists. 1, the backup exists. 0, the backup does not exist. "},

        {"database",                                    std::make_shared<DataTypeString>(),    "Name of the database."},
        {"table",                                       std::make_shared<DataTypeString>(),    "Name of the table."},
        {"engine",                                      std::make_shared<DataTypeString>(),    "Name of the table engine without parameters."},
        {"disk_name",                                   std::make_shared<DataTypeString>(),    "Name of a disk that stores the data part."},
        {"path",                                        std::make_shared<DataTypeString>(),    "Absolute path to the folder with data part files."},

        {"hash_of_all_files",                           std::make_shared<DataTypeString>(),    "sipHash128 of compressed files."},
        {"hash_of_uncompressed_files",                  std::make_shared<DataTypeString>(),    "sipHash128 of uncompressed files (files with marks, index file etc.)."},
        {"uncompressed_hash_of_compressed_files",       std::make_shared<DataTypeString>(),    "sipHash128 of data in the compressed files as if they were uncompressed."},

        {"delete_ttl_info_min",                         std::make_shared<DataTypeDateTime>(),  "The minimum value of the date and time key for TTL DELETE rule."},
        {"delete_ttl_info_max",                         std::make_shared<DataTypeDateTime>(),  "The maximum value of the date and time key for TTL DELETE rule."},

        {"move_ttl_info.expression",                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),   "Array of expressions. Each expression defines a TTL MOVE rule."},
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

        {"projections",                                 std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The list of projection names calculated for this part."},

        {"visible",                                     std::make_shared<DataTypeUInt8>(), "Flag which indicated whether this part is visible for SELECT queries."},
        {"creation_tid",                                getTransactionIDDataType(), "ID of transaction that has created/is trying to create this object."},
        {"removal_tid_lock",                            std::make_shared<DataTypeUInt64>(), "Hash of removal_tid, used to lock an object for removal."},
        {"removal_tid",                                 getTransactionIDDataType(), "ID of transaction that has removed/is trying to remove this object"},
        {"creation_csn",                                std::make_shared<DataTypeUInt64>(), "CSN of transaction that has created this object"},
        {"removal_csn",                                 std::make_shared<DataTypeUInt64>(), "CSN of transaction that has removed this object"},

        {"has_lightweight_delete",                      std::make_shared<DataTypeUInt8>(), "The flag which indicated whether the part has lightweight delete mask."},

        {"last_removal_attempt_time",                   std::make_shared<DataTypeDateTime>(), "The last time the server tried to delete this part."},
        {"removal_state",                               std::make_shared<DataTypeString>(), "The current state of part removal process."},
    }
    )
{
}

void StorageSystemParts::processNextStorage(
    ContextPtr context, MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column)
{
    using State = MergeTreeDataPartState;
    MergeTreeData::DataPartStateVector all_parts_state;
    MergeTreeData::DataPartsVector all_parts;

    all_parts = info.getParts(all_parts_state, has_state_column);

    for (size_t part_number = 0; part_number < all_parts.size(); ++part_number)
    {
        const auto & part = all_parts[part_number];
        auto part_state = all_parts_state[part_number];

        ColumnSize columns_size = part->getTotalColumnsSize();
        ColumnSize secondary_indexes_size = part->getTotalSecondaryIndicesSize();

        size_t src_index = 0, res_index = 0;
        if (columns_mask[src_index++])
        {
            WriteBufferFromOwnString out;
            part->partition.serializeText(*info.data, out, format_settings);
            columns[res_index++]->insert(out.str());
        }
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->name);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->uuid);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->getTypeName());
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
            columns[res_index++]->insert(part->getIndexSizeFromFile());
        if (columns_mask[src_index++])
            columns[res_index++]->insert(columns_size.marks);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(secondary_indexes_size.data_compressed);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(secondary_indexes_size.data_uncompressed);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(secondary_indexes_size.marks);
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

        auto min_max_date = part->getMinMaxDate();
        auto min_max_time = part->getMinMaxTime();

        if (columns_mask[src_index++])
            columns[res_index++]->insert(min_max_date.first);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(min_max_date.second);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(static_cast<UInt32>(min_max_time.first));
        if (columns_mask[src_index++])
            columns[res_index++]->insert(static_cast<UInt32>(min_max_time.second));
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->info.partition_id);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->info.min_block);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->info.max_block);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->info.level);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(static_cast<UInt64>(part->info.getDataVersion()));
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

        if (columns_mask[src_index++])
        {
            if (part->isStoredOnDisk())
                columns[res_index++]->insert(part->getDataPartStorage().getDiskName());
            else
                columns[res_index++]->insertDefault();
        }

        if (columns_mask[src_index++])
        {
            /// The full path changes at clean up thread, so do not read it if parts can be deleted, avoid the race.
            if (part->isStoredOnDisk()
                && part_state != State::Deleting && part_state != State::DeleteOnDestroy && part_state != State::Temporary)
            {
                columns[res_index++]->insert(part->getDataPartStorage().getFullPath());
            }
            else
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
            columns[res_index++]->insert(queryToString(part->default_codec->getCodecDesc()));

        add_ttl_info_map(part->ttl_infos.recompression_ttl);
        add_ttl_info_map(part->ttl_infos.group_by_ttl);
        add_ttl_info_map(part->ttl_infos.rows_where_ttl);

        Array projections;
        for (const auto & [name, _] : part->getProjectionParts())
            projections.push_back(name);

        if (columns_mask[src_index++])
            columns[res_index++]->insert(projections);

        if (columns_mask[src_index++])
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

        if (columns_mask[src_index++])
            columns[res_index++]->insert(get_tid_as_field(part->version.creation_tid));
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->version.removal_tid_lock.load(std::memory_order_relaxed));
        if (columns_mask[src_index++])
            columns[res_index++]->insert(get_tid_as_field(part->version.getRemovalTID()));
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->version.creation_csn.load(std::memory_order_relaxed));
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->version.removal_csn.load(std::memory_order_relaxed));
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->hasLightweightDelete());
        if (columns_mask[src_index++])
            columns[res_index++]->insert(static_cast<UInt64>(part->last_removal_attempt_time.load(std::memory_order_relaxed)));
        if (columns_mask[src_index++])
            columns[res_index++]->insert(getRemovalStateDescription(part->removal_state.load(std::memory_order_relaxed)));

        /// _state column should be the latest.
        /// Do not use part->getState*, it can be changed from different thread
        if (has_state_column)
            columns[res_index++]->insert(IMergeTreeDataPart::stateString(part_state));
    }
}

}
