#include <Storages/System/StorageSystemReplicatedPartitionExports.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/MergeTree/ExportList.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include "Columns/ColumnString.h"
#include "Storages/VirtualColumnUtils.h"


namespace DB
{

ColumnsDescription StorageSystemReplicatedPartitionExports::getColumnsDescription()
{
    auto last_exception_tuple = std::make_shared<DataTypeTuple>(
        DataTypes{
            std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeDateTime>(),
            std::make_shared<DataTypeUInt64>(),
        },
        Names{"replica", "message", "part", "time", "count"});

    auto backoff_tuple = std::make_shared<DataTypeTuple>(
        DataTypes{
            std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeUInt64>(),
            std::make_shared<DataTypeDateTime>(),
        },
        Names{"part", "attempts", "next_retry_time"});

    return ColumnsDescription
    {
        {"source_database", std::make_shared<DataTypeString>(), "Name of the source database."},
        {"source_table", std::make_shared<DataTypeString>(), "Name of the source table."},
        {"destination_database", std::make_shared<DataTypeString>(), "Name of the destination database."},
        {"destination_table", std::make_shared<DataTypeString>(), "Name of the destination table."},
        {"create_time", std::make_shared<DataTypeDateTime>(), "Date and time when the export command was submitted"},
        {"partition_id", std::make_shared<DataTypeString>(), "ID of the partition"},
        {"transaction_id", std::make_shared<DataTypeString>(), "ID of the transaction."},
        {"query_id", std::make_shared<DataTypeString>(), "Query ID of the export operation."},
        {"source_replica", std::make_shared<DataTypeString>(), "Name of the source replica."},
        {"parts", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "List of part names to be exported."},
        {"parts_count", std::make_shared<DataTypeUInt64>(), "Number of parts in the export."},
        {"parts_to_do", std::make_shared<DataTypeUInt64>(), "Number of parts pending to be exported."},
        {"status", std::make_shared<DataTypeString>(), "Status of the export."},
        {"last_exception_per_replica", std::make_shared<DataTypeArray>(last_exception_tuple),
            "Per-replica last exception entries. Each tuple records the most recent exception observed by that replica plus a best-effort within-replica count. Empty array if no replica has reported an exception for this task."},
        {"exception_count", std::make_shared<DataTypeUInt64>(),
            "Sum of per-replica exception counts. Each replica owns its own count, so the sum is exact w.r.t. the in-memory snapshot; within-replica updates remain best-effort and may under-count by one under concurrent failures."},
        {"destination_file_paths", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())),
            "Per-part destination file paths written to the destination object storage. Keyed by part name; values are the file paths produced by exporting that part. Mirrored from ZooKeeper on every poll while PENDING; partial during in-flight tasks. When the in-memory mirror could not fully refresh from Keeper (or a processed leaf is unreadable), the map may contain the key and/or path value '<failed to read from zk>' for the affected part (or for the whole field if listing processed leaves failed); replaced on the next successful poll."},
        {"committed_metadata_file", std::make_shared<DataTypeString>(),
            "For Iceberg destinations: path of the new metadata JSON file written at commit time. Empty for non-Iceberg destinations and for tasks that have not committed yet. May also be empty if the committing replica crashed between writing the object-storage files and persisting commit_info. If the export was already committed by a previous run (detected via the transaction id stored in the snapshot summary), this column holds a human-readable note instead of a path since the original committer's paths are not trivially recoverable."},
        {"committed_manifest_list", std::make_shared<DataTypeString>(),
            "For Iceberg destinations: path of the manifest list file (snap-*.avro) referenced by the new snapshot. Empty under the same conditions as committed_metadata_file."},
        {"committed_manifest_file", std::make_shared<DataTypeString>(),
            "For Iceberg destinations: path of the manifest file referenced by committed_manifest_list. Empty under the same conditions as committed_metadata_file."},
        {"committed_marker_file", std::make_shared<DataTypeString>(),
            "For plain object storage destinations: path of the per-transaction commit marker file written by the destination. Empty for Iceberg destinations and for tasks that have not committed yet."},
        {"local_backoff_per_part", std::make_shared<DataTypeArray>(backoff_tuple),
            "Per-part retry back-off local to this replica: parts currently waiting before their next attempt, with attempt count and the next eligible time. Not shared across replicas; empty if no part is backing off."},
    };
}

void StorageSystemReplicatedPartitionExports::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    std::map<String, std::map<String, StoragePtr>> replicated_merge_tree_tables;
    for (const auto & db : DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false}))
    {
        /// skip data lakes
        if (db.second->isExternal())
            continue;

        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, db.first);

        for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            const auto & table = iterator->table();
            if (!table)
                continue;

            StorageReplicatedMergeTree * table_replicated = dynamic_cast<StorageReplicatedMergeTree *>(table.get());
            if (!table_replicated)
                continue;

            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, db.first, iterator->name()))
                continue;

            replicated_merge_tree_tables[db.first][iterator->name()] = table;
        }
    }

    MutableColumnPtr col_database_mut = ColumnString::create();
    MutableColumnPtr col_table_mut = ColumnString::create();

    for (auto & db : replicated_merge_tree_tables)
    {
        for (auto & table : db.second)
        {
            col_database_mut->insert(db.first);
            col_table_mut->insert(table.first);
        }
    }

    ColumnPtr col_database = std::move(col_database_mut);
    ColumnPtr col_table = std::move(col_table_mut);

    /// Determine what tables are needed by the conditions in the query.
    {
        Block filtered_block
        {
            { col_database, std::make_shared<DataTypeString>(), "database" },
            { col_table, std::make_shared<DataTypeString>(), "table" },
        };

        VirtualColumnUtils::filterBlockWithPredicate(predicate, filtered_block, context);

        if (!filtered_block.rows())
            return;

        col_database = filtered_block.getByName("database").column;
        col_table = filtered_block.getByName("table").column;
    }

    for (size_t i_storage = 0; i_storage < col_database->size(); ++i_storage)
    {
        const auto database = (*col_database)[i_storage].safeGet<String>();
        const auto table = (*col_table)[i_storage].safeGet<String>();

        std::vector<ReplicatedPartitionExportInfo> partition_exports_info;
        {
            const IStorage * storage = replicated_merge_tree_tables[database][table].get();
            if (const auto * replicated_merge_tree = dynamic_cast<const StorageReplicatedMergeTree *>(storage))
                partition_exports_info = replicated_merge_tree->getPartitionExportsInfo();
        }

        for (const ReplicatedPartitionExportInfo & info : partition_exports_info)
        {
            std::size_t i = 0;
            res_columns[i++]->insert(database);
            res_columns[i++]->insert(table);
            res_columns[i++]->insert(info.destination_database);
            res_columns[i++]->insert(info.destination_table);
            res_columns[i++]->insert(info.create_time);
            res_columns[i++]->insert(info.partition_id);
            res_columns[i++]->insert(info.transaction_id);
            res_columns[i++]->insert(info.query_id);
            res_columns[i++]->insert(info.source_replica);
            Array parts_array;
            parts_array.reserve(info.parts.size());
            for (const auto & part : info.parts)
                parts_array.push_back(part);
            res_columns[i++]->insert(parts_array);
            res_columns[i++]->insert(info.parts_count);
            res_columns[i++]->insert(info.parts_to_do);
            res_columns[i++]->insert(info.status);

            Array per_replica;
            per_replica.reserve(info.last_exception_per_replica.size());
            for (const auto & ex : info.last_exception_per_replica)
                per_replica.push_back(Tuple{ex.replica, ex.message, ex.part, ex.time, ex.count});
            res_columns[i++]->insert(per_replica);
            res_columns[i++]->insert(info.exception_count);

            Map destination_paths_map;
            destination_paths_map.reserve(info.destination_file_paths_per_part.size());
            for (const auto & [part_name, paths] : info.destination_file_paths_per_part)
            {
                Array paths_array;
                paths_array.reserve(paths.size());
                for (const auto & path : paths)
                    paths_array.push_back(path);
                destination_paths_map.emplace_back(Tuple{part_name, std::move(paths_array)});
            }
            res_columns[i++]->insert(std::move(destination_paths_map));

            res_columns[i++]->insert(info.committed_metadata_file);
            res_columns[i++]->insert(info.committed_manifest_list);

            res_columns[i++]->insert(info.committed_manifest_file);

            res_columns[i++]->insert(info.committed_marker_file);

            Array backoff_array;
            backoff_array.reserve(info.backoff_per_part.size());
            for (const auto & b : info.backoff_per_part)
                backoff_array.push_back(Tuple{b.part, b.attempts, b.next_retry_time});
            res_columns[i++]->insert(backoff_array);
        }
    }
}

}
