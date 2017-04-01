#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemReplicationQueue.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/VirtualColumnUtils.h>
#include <Databases/IDatabase.h>


namespace DB
{


StorageSystemReplicationQueue::StorageSystemReplicationQueue(const std::string & name_)
    : name(name_)
    , columns{
        /// Table properties.
        { "database",                 std::make_shared<DataTypeString>()    },
        { "table",                     std::make_shared<DataTypeString>()    },
        { "replica_name",            std::make_shared<DataTypeString>()    },
        /// Constant element properties.
        { "position",                 std::make_shared<DataTypeUInt32>()    },
        { "node_name",                 std::make_shared<DataTypeString>()    },
        { "type",                     std::make_shared<DataTypeString>()    },
        { "create_time",            std::make_shared<DataTypeDateTime>()},
        { "required_quorum",         std::make_shared<DataTypeUInt32>()    },
        { "source_replica",         std::make_shared<DataTypeString>()    },
        { "new_part_name",             std::make_shared<DataTypeString>()    },
        { "parts_to_merge",         std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "is_detach",                std::make_shared<DataTypeUInt8>()    },
        { "is_attach_unreplicated",    std::make_shared<DataTypeUInt8>()    },
        { "attach_source_part_name",std::make_shared<DataTypeString>()    },
        /// Processing status of item.
        { "is_currently_executing",    std::make_shared<DataTypeUInt8>()    },
        { "num_tries",                std::make_shared<DataTypeUInt32>()    },
        { "last_exception",            std::make_shared<DataTypeString>()    },
        { "last_attempt_time",        std::make_shared<DataTypeDateTime>()},
        { "num_postponed",            std::make_shared<DataTypeUInt32>()    },
        { "postpone_reason",        std::make_shared<DataTypeString>()    },
        { "last_postpone_time",        std::make_shared<DataTypeDateTime>()},
    }
{
}

StoragePtr StorageSystemReplicationQueue::create(const std::string & name_)
{
    return make_shared(name_);
}


BlockInputStreams StorageSystemReplicationQueue::read(
    const Names & column_names,
    ASTPtr query,
    const Context & context,
    const Settings & settings,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned threads)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    std::map<String, std::map<String, StoragePtr>> replicated_tables;
    for (const auto & db : context.getDatabases())
        for (auto iterator = db.second->getIterator(); iterator->isValid(); iterator->next())
            if (typeid_cast<const StorageReplicatedMergeTree *>(iterator->table().get()))
                replicated_tables[db.first][iterator->name()] = iterator->table();

    ColumnWithTypeAndName col_database_to_filter        { std::make_shared<ColumnString>(),    std::make_shared<DataTypeString>(),    "database" };
    ColumnWithTypeAndName col_table_to_filter            { std::make_shared<ColumnString>(),    std::make_shared<DataTypeString>(),    "table" };

    for (auto & db : replicated_tables)
    {
        for (auto & table : db.second)
        {
            col_database_to_filter.column->insert(db.first);
            col_table_to_filter.column->insert(table.first);
        }
    }

    /// Determine what tables are needed by the conditions in the query.
    {
        Block filtered_block { col_database_to_filter, col_table_to_filter };

        VirtualColumnUtils::filterBlockWithQuery(query, filtered_block, context);

        if (!filtered_block.rows())
            return BlockInputStreams();

        col_database_to_filter     = filtered_block.getByName("database");
        col_table_to_filter     = filtered_block.getByName("table");
    }

    ColumnWithTypeAndName col_database                    { std::make_shared<ColumnString>(),    std::make_shared<DataTypeString>(),    "database" };
    ColumnWithTypeAndName col_table                        { std::make_shared<ColumnString>(),    std::make_shared<DataTypeString>(),    "table" };
    ColumnWithTypeAndName col_replica_name                 { std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(), "replica_name" };
    ColumnWithTypeAndName col_position                     { std::make_shared<ColumnUInt32>(),     std::make_shared<DataTypeUInt32>(), "position" };
    ColumnWithTypeAndName col_node_name                 { std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(), "node_name" };
    ColumnWithTypeAndName col_type                         { std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(), "type" };
    ColumnWithTypeAndName col_create_time                 { std::make_shared<ColumnUInt32>(),     std::make_shared<DataTypeDateTime>(), "create_time" };
    ColumnWithTypeAndName col_required_quorum             { std::make_shared<ColumnUInt32>(),     std::make_shared<DataTypeUInt32>(), "required_quorum" };
    ColumnWithTypeAndName col_source_replica             { std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(), "source_replica" };
    ColumnWithTypeAndName col_new_part_name             { std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(), "new_part_name" };
    ColumnWithTypeAndName col_parts_to_merge             { std::make_shared<ColumnArray>(std::make_shared<ColumnString>()),
        std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "parts_to_merge" };
    ColumnWithTypeAndName col_is_detach                 { std::make_shared<ColumnUInt8>(),         std::make_shared<DataTypeUInt8>(), "is_detach" };
    ColumnWithTypeAndName col_is_attach_unreplicated     { std::make_shared<ColumnUInt8>(),         std::make_shared<DataTypeUInt8>(), "is_attach_unreplicated" };
    ColumnWithTypeAndName col_attach_source_part_name     { std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(), "attach_source_part_name" };
    ColumnWithTypeAndName col_is_currently_executing     { std::make_shared<ColumnUInt8>(),         std::make_shared<DataTypeUInt8>(), "is_currently_executing" };
    ColumnWithTypeAndName col_num_tries                 { std::make_shared<ColumnUInt32>(),     std::make_shared<DataTypeUInt32>(), "num_tries" };
    ColumnWithTypeAndName col_last_exception             { std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(), "last_exception" };
    ColumnWithTypeAndName col_last_attempt_time         { std::make_shared<ColumnUInt32>(),     std::make_shared<DataTypeDateTime>(), "last_attempt_time" };
    ColumnWithTypeAndName col_num_postponed             { std::make_shared<ColumnUInt32>(),     std::make_shared<DataTypeUInt32>(), "num_postponed" };
    ColumnWithTypeAndName col_postpone_reason             { std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(), "postpone_reason" };
    ColumnWithTypeAndName col_last_postpone_time         { std::make_shared<ColumnUInt32>(),     std::make_shared<DataTypeDateTime>(), "last_postpone_time" };

    StorageReplicatedMergeTree::LogEntriesData queue;
    String replica_name;

    for (size_t i = 0, tables_size = col_database_to_filter.column->size(); i < tables_size; ++i)
    {
        String database = (*col_database_to_filter.column)[i].safeGet<const String &>();
        String table = (*col_table_to_filter.column)[i].safeGet<const String &>();

        typeid_cast<StorageReplicatedMergeTree &>(*replicated_tables[database][table]).getQueue(queue, replica_name);

        for (size_t j = 0, queue_size = queue.size(); j < queue_size; ++j)
        {
            const auto & entry = queue[j];

            Array parts_to_merge;
            parts_to_merge.reserve(entry.parts_to_merge.size());
            for (const auto & name : entry.parts_to_merge)
                parts_to_merge.push_back(name);

            col_database                .column->insert(database);
            col_table                    .column->insert(table);
            col_replica_name            .column->insert(replica_name);
            col_position                .column->insert(UInt64(j));
            col_node_name                .column->insert(entry.znode_name);
            col_type                    .column->insert(entry.typeToString());
            col_create_time                .column->insert(UInt64(entry.create_time));
            col_required_quorum            .column->insert(UInt64(entry.quorum));
            col_source_replica            .column->insert(entry.source_replica);
            col_new_part_name            .column->insert(entry.new_part_name);
            col_parts_to_merge            .column->insert(parts_to_merge);
            col_is_detach                .column->insert(UInt64(entry.detach));
            col_is_attach_unreplicated    .column->insert(UInt64(entry.attach_unreplicated));
            col_attach_source_part_name    .column->insert(entry.source_part_name);
            col_is_currently_executing    .column->insert(UInt64(entry.currently_executing));
            col_num_tries                .column->insert(UInt64(entry.num_tries));
            col_last_exception            .column->insert(entry.exception ? getExceptionMessage(entry.exception, false) : "");
            col_last_attempt_time        .column->insert(UInt64(entry.last_attempt_time));
            col_num_postponed            .column->insert(UInt64(entry.num_postponed));
            col_postpone_reason            .column->insert(entry.postpone_reason);
            col_last_postpone_time        .column->insert(UInt64(entry.last_postpone_time));
        }
    }

    Block block{
        col_database,
        col_table,
        col_replica_name,
        col_position,
        col_node_name,
        col_type,
        col_create_time,
        col_required_quorum,
        col_source_replica,
        col_new_part_name,
        col_parts_to_merge,
        col_is_detach,
        col_is_attach_unreplicated,
        col_attach_source_part_name,
        col_is_currently_executing,
        col_num_tries,
        col_last_exception,
        col_last_attempt_time,
        col_num_postponed,
        col_postpone_reason,
        col_last_postpone_time,
    };

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
