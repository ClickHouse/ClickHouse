#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Storages/System/StorageSystemReplicas.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Common/VirtualColumnUtils.h>
#include <DB/Databases/IDatabase.h>


namespace DB
{


StorageSystemReplicas::StorageSystemReplicas(const std::string & name_)
	: name(name_)
	, columns{
		{ "database", 				new DataTypeString	},
		{ "table", 					new DataTypeString	},
		{ "engine", 				new DataTypeString	},
		{ "is_leader", 				new DataTypeUInt8	},
		{ "is_readonly", 			new DataTypeUInt8	},
		{ "is_session_expired",		new DataTypeUInt8	},
		{ "future_parts",			new DataTypeUInt32	},
		{ "parts_to_check",			new DataTypeUInt32	},
		{ "zookeeper_path",			new DataTypeString	},
		{ "replica_name",			new DataTypeString	},
		{ "replica_path",			new DataTypeString	},
		{ "columns_version",		new DataTypeInt32	},
		{ "queue_size",				new DataTypeUInt32	},
		{ "inserts_in_queue",		new DataTypeUInt32	},
		{ "merges_in_queue",		new DataTypeUInt32	},
		{ "queue_oldest_time",		new DataTypeDateTime},
		{ "inserts_oldest_time",	new DataTypeDateTime},
		{ "merges_oldest_time",		new DataTypeDateTime},
		{ "oldest_part_to_get",		new DataTypeString	},
		{ "oldest_part_to_merge_to",new DataTypeString	},
		{ "log_max_index",			new DataTypeUInt64	},
		{ "log_pointer", 			new DataTypeUInt64	},
		{ "last_queue_update",		new DataTypeDateTime},
		{ "total_replicas",			new DataTypeUInt8	},
		{ "active_replicas", 		new DataTypeUInt8	},
	}
{
}

StoragePtr StorageSystemReplicas::create(const std::string & name_)
{
	return (new StorageSystemReplicas(name_))->thisPtr();
}


BlockInputStreams StorageSystemReplicas::read(
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

	/// Собираем набор реплицируемых таблиц.
	std::map<String, std::map<String, StoragePtr>> replicated_tables;
	for (const auto & db : context.getDatabases())
		for (auto iterator = db.second->getIterator(); iterator->isValid(); iterator->next())
			if (typeid_cast<const StorageReplicatedMergeTree *>(iterator->table().get()))
				replicated_tables[db.first][iterator->name()] = iterator->table();

	/// Нужны ли столбцы, требующие для вычисления хождение в ZooKeeper.
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

	ColumnWithTypeAndName col_database			{ new ColumnString,	new DataTypeString,	"database"};
	ColumnWithTypeAndName col_table				{ new ColumnString,	new DataTypeString,	"table"};
	ColumnWithTypeAndName col_engine			{ new ColumnString,	new DataTypeString,	"engine"};

	for (auto & db : replicated_tables)
	{
		for (auto & table : db.second)
		{
			col_database.column->insert(db.first);
			col_table.column->insert(table.first);
			col_engine.column->insert(table.second->getName());
		}
	}

	/// Определяем, какие нужны таблицы, по условиям в запросе.
	{
		Block filtered_block { col_database, col_table, col_engine };

		VirtualColumnUtils::filterBlockWithQuery(query, filtered_block, context);

		if (!filtered_block.rows())
			return BlockInputStreams();

		col_database 	= filtered_block.getByName("database");
		col_table 		= filtered_block.getByName("table");
		col_engine 		= filtered_block.getByName("engine");
	}

	ColumnWithTypeAndName col_is_leader			{ new ColumnUInt8,	new DataTypeUInt8,	"is_leader"};
	ColumnWithTypeAndName col_is_readonly		{ new ColumnUInt8,	new DataTypeUInt8,	"is_readonly"};
	ColumnWithTypeAndName col_is_session_expired{ new ColumnUInt8,	new DataTypeUInt8,	"is_session_expired"};
	ColumnWithTypeAndName col_future_parts		{ new ColumnUInt32,	new DataTypeUInt32,	"future_parts"};
	ColumnWithTypeAndName col_parts_to_check	{ new ColumnUInt32,	new DataTypeUInt32,	"parts_to_check"};
	ColumnWithTypeAndName col_zookeeper_path	{ new ColumnString,	new DataTypeString,	"zookeeper_path"};
	ColumnWithTypeAndName col_replica_name		{ new ColumnString,	new DataTypeString,	"replica_name"};
	ColumnWithTypeAndName col_replica_path		{ new ColumnString,	new DataTypeString,	"replica_path"};
	ColumnWithTypeAndName col_columns_version	{ new ColumnInt32,	new DataTypeInt32,	"columns_version"};
	ColumnWithTypeAndName col_queue_size		{ new ColumnUInt32,	new DataTypeUInt32,	"queue_size"};
	ColumnWithTypeAndName col_inserts_in_queue	{ new ColumnUInt32,	new DataTypeUInt32,	"inserts_in_queue"};
	ColumnWithTypeAndName col_merges_in_queue	{ new ColumnUInt32,	new DataTypeUInt32,	"merges_in_queue"};
	ColumnWithTypeAndName col_queue_oldest_time	{ new ColumnUInt32,	new DataTypeDateTime, "queue_oldest_time"};
	ColumnWithTypeAndName col_inserts_oldest_time{ new ColumnUInt32,new DataTypeDateTime, "inserts_oldest_time"};
	ColumnWithTypeAndName col_merges_oldest_time{ new ColumnUInt32,	new DataTypeDateTime, "merges_oldest_time"};
	ColumnWithTypeAndName col_oldest_part_to_get{ new ColumnString,	new DataTypeString, "oldest_part_to_get"};
	ColumnWithTypeAndName col_oldest_part_to_merge_to{ new ColumnString, new DataTypeString, "oldest_part_to_merge_to"};
	ColumnWithTypeAndName col_log_max_index		{ new ColumnUInt64,	new DataTypeUInt64,	"log_max_index"};
	ColumnWithTypeAndName col_log_pointer		{ new ColumnUInt64,	new DataTypeUInt64,	"log_pointer"};
	ColumnWithTypeAndName col_last_queue_update	{ new ColumnUInt32,	new DataTypeDateTime, "last_queue_update"};
	ColumnWithTypeAndName col_total_replicas	{ new ColumnUInt8,	new DataTypeUInt8,	"total_replicas"};
	ColumnWithTypeAndName col_active_replicas	{ new ColumnUInt8,	new DataTypeUInt8,	"active_replicas"};

	for (size_t i = 0, size = col_database.column->size(); i < size; ++i)
	{
		StorageReplicatedMergeTree::Status status;
		typeid_cast<StorageReplicatedMergeTree &>(
			*replicated_tables
				[(*col_database.column)[i].safeGet<const String &>()]
				[(*col_table.column)[i].safeGet<const String &>()]).getStatus(status, with_zk_fields);

		col_is_leader			.column->insert(UInt64(status.is_leader));
		col_is_readonly			.column->insert(UInt64(status.is_readonly));
		col_is_session_expired	.column->insert(UInt64(status.is_session_expired));
		col_future_parts		.column->insert(UInt64(status.queue.future_parts));
		col_parts_to_check		.column->insert(UInt64(status.parts_to_check));
		col_zookeeper_path		.column->insert(status.zookeeper_path);
		col_replica_name		.column->insert(status.replica_name);
		col_replica_path		.column->insert(status.replica_path);
		col_columns_version		.column->insert(Int64(status.columns_version));
		col_queue_size			.column->insert(UInt64(status.queue.queue_size));
		col_inserts_in_queue	.column->insert(UInt64(status.queue.inserts_in_queue));
		col_merges_in_queue		.column->insert(UInt64(status.queue.merges_in_queue));
		col_queue_oldest_time	.column->insert(UInt64(status.queue.queue_oldest_time));
		col_inserts_oldest_time	.column->insert(UInt64(status.queue.inserts_oldest_time));
		col_merges_oldest_time	.column->insert(UInt64(status.queue.merges_oldest_time));
		col_oldest_part_to_get	.column->insert(status.queue.oldest_part_to_get);
		col_oldest_part_to_merge_to.column->insert(status.queue.oldest_part_to_merge_to);
		col_log_max_index		.column->insert(status.log_max_index);
		col_log_pointer			.column->insert(status.log_pointer);
		col_last_queue_update	.column->insert(UInt64(status.queue.last_queue_update));
		col_total_replicas		.column->insert(UInt64(status.total_replicas));
		col_active_replicas		.column->insert(UInt64(status.active_replicas));
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
		col_total_replicas,
		col_active_replicas,
	};

	return BlockInputStreams(1, new OneBlockInputStream(block));
}


}
