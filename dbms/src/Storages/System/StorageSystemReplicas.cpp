#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemReplicas.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/VirtualColumnUtils.h>
#include <Common/typeid_cast.h>
#include <Databases/IDatabase.h>


namespace DB
{


StorageSystemReplicas::StorageSystemReplicas(const std::string & name_)
    : name(name_)
    , columns{
        { "database",                             std::make_shared<DataTypeString>()   },
        { "table",                                std::make_shared<DataTypeString>()   },
        { "engine",                               std::make_shared<DataTypeString>()   },
        { "is_leader",                            std::make_shared<DataTypeUInt8>()    },
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
        { "queue_oldest_time",                    std::make_shared<DataTypeDateTime>() },
        { "inserts_oldest_time",                  std::make_shared<DataTypeDateTime>() },
        { "merges_oldest_time",                   std::make_shared<DataTypeDateTime>() },
        { "oldest_part_to_get",                   std::make_shared<DataTypeString>()   },
        { "oldest_part_to_merge_to",              std::make_shared<DataTypeString>()   },
        { "log_max_index",                        std::make_shared<DataTypeUInt64>()   },
        { "log_pointer",                          std::make_shared<DataTypeUInt64>()   },
        { "last_queue_update",                    std::make_shared<DataTypeDateTime>() },
        { "absolute_delay",                       std::make_shared<DataTypeUInt64>()   },
        { "total_replicas",                       std::make_shared<DataTypeUInt8>()    },
        { "active_replicas",                      std::make_shared<DataTypeUInt8>()    },
    }
{
}


BlockInputStreams StorageSystemReplicas::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    /// We collect a set of replicated tables.
    std::map<String, std::map<String, StoragePtr>> replicated_tables;
    for (const auto & db : context.getDatabases())
        for (auto iterator = db.second->getIterator(); iterator->isValid(); iterator->next())
            if (typeid_cast<const StorageReplicatedMergeTree *>(iterator->table().get()))
                replicated_tables[db.first][iterator->name()] = iterator->table();

    /// Do you need columns that require a walkthrough in ZooKeeper to compute.
    bool with_zk_fields = false;
    for (const auto & name : column_names)
    {
        if (   name == "log_max_index"
            || name == "log_pointer"
            || name == "total_replicas"
            || name == "active_replicas")
        {
            with_zk_fields = true;
            break;
        }
    }

    ColumnWithTypeAndName col_database            { std::make_shared<ColumnString>(),    std::make_shared<DataTypeString>(),    "database"};
    ColumnWithTypeAndName col_table                { std::make_shared<ColumnString>(),    std::make_shared<DataTypeString>(),    "table"};
    ColumnWithTypeAndName col_engine            { std::make_shared<ColumnString>(),    std::make_shared<DataTypeString>(),    "engine"};

    for (auto & db : replicated_tables)
    {
        for (auto & table : db.second)
        {
            col_database.column->insert(db.first);
            col_table.column->insert(table.first);
            col_engine.column->insert(table.second->getName());
        }
    }

    /// Determine what tables are needed by the conditions in the query.
    {
        Block filtered_block { col_database, col_table, col_engine };

        VirtualColumnUtils::filterBlockWithQuery(query_info.query, filtered_block, context);

        if (!filtered_block.rows())
            return BlockInputStreams();

        col_database = filtered_block.getByName("database");
        col_table = filtered_block.getByName("table");
        col_engine = filtered_block.getByName("engine");
    }

    ColumnWithTypeAndName col_is_leader{std::make_shared<ColumnUInt8>(), std::make_shared<DataTypeUInt8>(), "is_leader"};
    ColumnWithTypeAndName col_is_readonly{std::make_shared<ColumnUInt8>(), std::make_shared<DataTypeUInt8>(), "is_readonly"};
    ColumnWithTypeAndName col_is_session_expired{std::make_shared<ColumnUInt8>(), std::make_shared<DataTypeUInt8>(), "is_session_expired"};
    ColumnWithTypeAndName col_future_parts{std::make_shared<ColumnUInt32>(), std::make_shared<DataTypeUInt32>(), "future_parts"};
    ColumnWithTypeAndName col_parts_to_check{std::make_shared<ColumnUInt32>(), std::make_shared<DataTypeUInt32>(), "parts_to_check"};
    ColumnWithTypeAndName col_zookeeper_path{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "zookeeper_path"};
    ColumnWithTypeAndName col_replica_name{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "replica_name"};
    ColumnWithTypeAndName col_replica_path{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "replica_path"};
    ColumnWithTypeAndName col_columns_version{std::make_shared<ColumnInt32>(), std::make_shared<DataTypeInt32>(), "columns_version"};
    ColumnWithTypeAndName col_queue_size{std::make_shared<ColumnUInt32>(), std::make_shared<DataTypeUInt32>(), "queue_size"};
    ColumnWithTypeAndName col_inserts_in_queue{std::make_shared<ColumnUInt32>(), std::make_shared<DataTypeUInt32>(), "inserts_in_queue"};
    ColumnWithTypeAndName col_merges_in_queue{std::make_shared<ColumnUInt32>(), std::make_shared<DataTypeUInt32>(), "merges_in_queue"};
    ColumnWithTypeAndName col_queue_oldest_time{std::make_shared<ColumnUInt32>(), std::make_shared<DataTypeDateTime>(), "queue_oldest_time"};
    ColumnWithTypeAndName col_inserts_oldest_time{std::make_shared<ColumnUInt32>(), std::make_shared<DataTypeDateTime>(), "inserts_oldest_time"};
    ColumnWithTypeAndName col_merges_oldest_time{std::make_shared<ColumnUInt32>(), std::make_shared<DataTypeDateTime>(), "merges_oldest_time"};
    ColumnWithTypeAndName col_oldest_part_to_get{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "oldest_part_to_get"};
    ColumnWithTypeAndName col_oldest_part_to_merge_to{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "oldest_part_to_merge_to"};
    ColumnWithTypeAndName col_log_max_index{std::make_shared<ColumnUInt64>(), std::make_shared<DataTypeUInt64>(), "log_max_index"};
    ColumnWithTypeAndName col_log_pointer{std::make_shared<ColumnUInt64>(), std::make_shared<DataTypeUInt64>(), "log_pointer"};
    ColumnWithTypeAndName col_last_queue_update{std::make_shared<ColumnUInt32>(), std::make_shared<DataTypeDateTime>(), "last_queue_update"};
    ColumnWithTypeAndName col_absolute_delay{std::make_shared<ColumnUInt64>(), std::make_shared<DataTypeUInt64>(), "absolute_delay"};
    ColumnWithTypeAndName col_total_replicas{std::make_shared<ColumnUInt8>(), std::make_shared<DataTypeUInt8>(), "total_replicas"};
    ColumnWithTypeAndName col_active_replicas{std::make_shared<ColumnUInt8>(), std::make_shared<DataTypeUInt8>(), "active_replicas"};

    for (size_t i = 0, size = col_database.column->size(); i < size; ++i)
    {
        StorageReplicatedMergeTree::Status status;
        typeid_cast<StorageReplicatedMergeTree &>(
            *replicated_tables
                [(*col_database.column)[i].safeGet<const String &>()]
                [(*col_table.column)[i].safeGet<const String &>()]).getStatus(status, with_zk_fields);

        col_is_leader.column->insert(UInt64(status.is_leader));
        col_is_readonly.column->insert(UInt64(status.is_readonly));
        col_is_session_expired.column->insert(UInt64(status.is_session_expired));
        col_future_parts.column->insert(UInt64(status.queue.future_parts));
        col_parts_to_check.column->insert(UInt64(status.parts_to_check));
        col_zookeeper_path.column->insert(status.zookeeper_path);
        col_replica_name.column->insert(status.replica_name);
        col_replica_path.column->insert(status.replica_path);
        col_columns_version.column->insert(Int64(status.columns_version));
        col_queue_size.column->insert(UInt64(status.queue.queue_size));
        col_inserts_in_queue.column->insert(UInt64(status.queue.inserts_in_queue));
        col_merges_in_queue.column->insert(UInt64(status.queue.merges_in_queue));
        col_queue_oldest_time.column->insert(UInt64(status.queue.queue_oldest_time));
        col_inserts_oldest_time.column->insert(UInt64(status.queue.inserts_oldest_time));
        col_merges_oldest_time.column->insert(UInt64(status.queue.merges_oldest_time));
        col_oldest_part_to_get.column->insert(status.queue.oldest_part_to_get);
        col_oldest_part_to_merge_to.column->insert(status.queue.oldest_part_to_merge_to);
        col_log_max_index.column->insert(status.log_max_index);
        col_log_pointer.column->insert(status.log_pointer);
        col_last_queue_update.column->insert(UInt64(status.queue.last_queue_update));
        col_absolute_delay.column->insert(UInt64(status.absolute_delay));
        col_total_replicas.column->insert(UInt64(status.total_replicas));
        col_active_replicas.column->insert(UInt64(status.active_replicas));
    }

    Block block{
        col_database,
        col_table,
        col_engine,
        col_is_leader,
        col_is_readonly,
        col_is_session_expired,
        col_future_parts,
        col_parts_to_check,
        col_zookeeper_path,
        col_replica_name,
        col_replica_path,
        col_columns_version,
        col_queue_size,
        col_inserts_in_queue,
        col_merges_in_queue,
        col_queue_oldest_time,
        col_inserts_oldest_time,
        col_merges_oldest_time,
        col_oldest_part_to_get,
        col_oldest_part_to_merge_to,
        col_log_max_index,
        col_log_pointer,
        col_last_queue_update,
        col_absolute_delay,
        col_total_replicas,
        col_active_replicas,
    };

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
