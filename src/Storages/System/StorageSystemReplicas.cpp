#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Storages/System/StorageSystemReplicas.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/VirtualColumnUtils.h>
#include <Access/ContextAccess.h>
#include <Common/typeid_cast.h>
#include <Databases/IDatabase.h>
#include <Processors/Sources/SourceFromSingleChunk.h>


namespace DB
{


StorageSystemReplicas::StorageSystemReplicas(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({
        { "database",                             std::make_shared<DataTypeString>()   },
        { "table",                                std::make_shared<DataTypeString>()   },
        { "engine",                               std::make_shared<DataTypeString>()   },
        { "is_leader",                            std::make_shared<DataTypeUInt8>()    },
        { "can_become_leader",                    std::make_shared<DataTypeUInt8>()    },
        { "is_readonly",                          std::make_shared<DataTypeUInt8>()    },
        { "is_session_expired",                   std::make_shared<DataTypeUInt8>()    },
        { "future_parts",                         std::make_shared<DataTypeUInt32>()   },
        { "parts_to_check",                       std::make_shared<DataTypeUInt32>()   },
        { "zookeeper_path",                       std::make_shared<DataTypeString>()   },
        { "replica_name",                         std::make_shared<DataTypeString>()   },
        { "replica_path",                         std::make_shared<DataTypeString>()   },
        { "columns_version",                      std::make_shared<DataTypeInt32>()    },
        { "queue_size",                           std::make_shared<DataTypeUInt32>()   },
        { "inserts_in_queue",                     std::make_shared<DataTypeUInt32>()   },
        { "merges_in_queue",                      std::make_shared<DataTypeUInt32>()   },
        { "part_mutations_in_queue",              std::make_shared<DataTypeUInt32>()   },
        { "queue_oldest_time",                    std::make_shared<DataTypeDateTime>() },
        { "inserts_oldest_time",                  std::make_shared<DataTypeDateTime>() },
        { "merges_oldest_time",                   std::make_shared<DataTypeDateTime>() },
        { "part_mutations_oldest_time",           std::make_shared<DataTypeDateTime>() },
        { "oldest_part_to_get",                   std::make_shared<DataTypeString>()   },
        { "oldest_part_to_merge_to",              std::make_shared<DataTypeString>()   },
        { "oldest_part_to_mutate_to",             std::make_shared<DataTypeString>()   },
        { "log_max_index",                        std::make_shared<DataTypeUInt64>()   },
        { "log_pointer",                          std::make_shared<DataTypeUInt64>()   },
        { "last_queue_update",                    std::make_shared<DataTypeDateTime>() },
        { "absolute_delay",                       std::make_shared<DataTypeUInt64>()   },
        { "total_replicas",                       std::make_shared<DataTypeUInt8>()    },
        { "active_replicas",                      std::make_shared<DataTypeUInt8>()    },
        { "zookeeper_exception",                  std::make_shared<DataTypeString>()   },
    }));
    setInMemoryMetadata(storage_metadata);
}


Pipe StorageSystemReplicas::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    const auto access = context.getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    /// We collect a set of replicated tables.
    std::map<String, std::map<String, StoragePtr>> replicated_tables;
    for (const auto & db : DatabaseCatalog::instance().getDatabases())
    {
        /// Check if database can contain replicated tables
        if (!db.second->canContainMergeTreeTables())
            continue;
        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, db.first);
        for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            const auto & table = iterator->table();
            if (!table)
                continue;

            if (!dynamic_cast<const StorageReplicatedMergeTree *>(table.get()))
                continue;
            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, db.first, iterator->name()))
                continue;
            replicated_tables[db.first][iterator->name()] = table;
        }
    }


    /// Do you need columns that require a ZooKeeper request to compute.
    bool with_zk_fields = false;
    for (const auto & column_name : column_names)
    {
        if (   column_name == "log_max_index"
            || column_name == "log_pointer"
            || column_name == "total_replicas"
            || column_name == "active_replicas"
            || column_name == "zookeeper_exception")
        {
            with_zk_fields = true;
            break;
        }
    }

    MutableColumnPtr col_database_mut = ColumnString::create();
    MutableColumnPtr col_table_mut = ColumnString::create();
    MutableColumnPtr col_engine_mut = ColumnString::create();

    for (auto & db : replicated_tables)
    {
        for (auto & table : db.second)
        {
            col_database_mut->insert(db.first);
            col_table_mut->insert(table.first);
            col_engine_mut->insert(table.second->getName());
        }
    }

    ColumnPtr col_database = std::move(col_database_mut);
    ColumnPtr col_table = std::move(col_table_mut);
    ColumnPtr col_engine = std::move(col_engine_mut);

    /// Determine what tables are needed by the conditions in the query.
    {
        Block filtered_block
        {
            { col_database, std::make_shared<DataTypeString>(), "database" },
            { col_table, std::make_shared<DataTypeString>(), "table" },
            { col_engine, std::make_shared<DataTypeString>(), "engine" },
        };

        VirtualColumnUtils::filterBlockWithQuery(query_info.query, filtered_block, context);

        if (!filtered_block.rows())
            return {};

        col_database = filtered_block.getByName("database").column;
        col_table = filtered_block.getByName("table").column;
        col_engine = filtered_block.getByName("engine").column;
    }

    MutableColumns res_columns = metadata_snapshot->getSampleBlock().cloneEmptyColumns();

    for (size_t i = 0, size = col_database->size(); i < size; ++i)
    {
        StorageReplicatedMergeTree::Status status;
        dynamic_cast<StorageReplicatedMergeTree &>(
            *replicated_tables
                [(*col_database)[i].safeGet<const String &>()]
                [(*col_table)[i].safeGet<const String &>()]).getStatus(status, with_zk_fields);

        size_t col_num = 3;
        res_columns[col_num++]->insert(status.is_leader);
        res_columns[col_num++]->insert(status.can_become_leader);
        res_columns[col_num++]->insert(status.is_readonly);
        res_columns[col_num++]->insert(status.is_session_expired);
        res_columns[col_num++]->insert(status.queue.future_parts);
        res_columns[col_num++]->insert(status.parts_to_check);
        res_columns[col_num++]->insert(status.zookeeper_path);
        res_columns[col_num++]->insert(status.replica_name);
        res_columns[col_num++]->insert(status.replica_path);
        res_columns[col_num++]->insert(status.columns_version);
        res_columns[col_num++]->insert(status.queue.queue_size);
        res_columns[col_num++]->insert(status.queue.inserts_in_queue);
        res_columns[col_num++]->insert(status.queue.merges_in_queue);
        res_columns[col_num++]->insert(status.queue.part_mutations_in_queue);
        res_columns[col_num++]->insert(status.queue.queue_oldest_time);
        res_columns[col_num++]->insert(status.queue.inserts_oldest_time);
        res_columns[col_num++]->insert(status.queue.merges_oldest_time);
        res_columns[col_num++]->insert(status.queue.part_mutations_oldest_time);
        res_columns[col_num++]->insert(status.queue.oldest_part_to_get);
        res_columns[col_num++]->insert(status.queue.oldest_part_to_merge_to);
        res_columns[col_num++]->insert(status.queue.oldest_part_to_mutate_to);
        res_columns[col_num++]->insert(status.log_max_index);
        res_columns[col_num++]->insert(status.log_pointer);
        res_columns[col_num++]->insert(status.queue.last_queue_update);
        res_columns[col_num++]->insert(status.absolute_delay);
        res_columns[col_num++]->insert(status.total_replicas);
        res_columns[col_num++]->insert(status.active_replicas);
        res_columns[col_num++]->insert(status.zookeeper_exception);
    }

    Block header = metadata_snapshot->getSampleBlock();

    Columns fin_columns;
    fin_columns.reserve(res_columns.size());

    for (auto & col : res_columns)
        fin_columns.emplace_back(std::move(col));

    fin_columns[0] = std::move(col_database);
    fin_columns[1] = std::move(col_table);
    fin_columns[2] = std::move(col_engine);

    UInt64 num_rows = fin_columns.at(0)->size();
    Chunk chunk(std::move(fin_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(metadata_snapshot->getSampleBlock(), std::move(chunk)));
}


}
