#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Storages/StorageSystemReplicas.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Common/VirtualColumnUtils.h>


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
		{ "log_max_index",			new DataTypeUInt64	},
		{ "log_pointer", 			new DataTypeUInt64	},
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
	Databases replicated_tables;
	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

		for (const auto & db : context.getDatabases())
			for (const auto & table : db.second)
				if (typeid_cast<const StorageReplicatedMergeTree *>(table.second.get()))
					replicated_tables[db.first][table.first] = table.second;
	}

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

	ColumnWithNameAndType col_database			{ new ColumnString,	new DataTypeString,	"database"};
	ColumnWithNameAndType col_table				{ new ColumnString,	new DataTypeString,	"table"};
	ColumnWithNameAndType col_engine			{ new ColumnString,	new DataTypeString,	"engine"};

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

	ColumnWithNameAndType col_is_leader			{ new ColumnUInt8,	new DataTypeUInt8,	"is_leader"};
	ColumnWithNameAndType col_is_readonly		{ new ColumnUInt8,	new DataTypeUInt8,	"is_readonly"};
	ColumnWithNameAndType col_is_session_expired{ new ColumnUInt8,	new DataTypeUInt8,	"is_session_expired"};
	ColumnWithNameAndType col_future_parts		{ new ColumnUInt32,	new DataTypeUInt32,	"future_parts"};
	ColumnWithNameAndType col_parts_to_check	{ new ColumnUInt32,	new DataTypeUInt32,	"parts_to_check"};
	ColumnWithNameAndType col_zookeeper_path	{ new ColumnString,	new DataTypeString,	"zookeeper_path"};
	ColumnWithNameAndType col_replica_name		{ new ColumnString,	new DataTypeString,	"replica_name"};
	ColumnWithNameAndType col_replica_path		{ new ColumnString,	new DataTypeString,	"replica_path"};
	ColumnWithNameAndType col_columns_version	{ new ColumnInt32,	new DataTypeInt32,	"columns_version"};
	ColumnWithNameAndType col_queue_size		{ new ColumnUInt32,	new DataTypeUInt32,	"queue_size"};
	ColumnWithNameAndType col_inserts_in_queue	{ new ColumnUInt32,	new DataTypeUInt32,	"inserts_in_queue"};
	ColumnWithNameAndType col_merges_in_queue	{ new ColumnUInt32,	new DataTypeUInt32,	"merges_in_queue"};
	ColumnWithNameAndType col_queue_oldest_time	{ new ColumnUInt32,	new DataTypeDateTime, "queue_oldest_time"};
	ColumnWithNameAndType col_log_max_index		{ new ColumnUInt64,	new DataTypeUInt64,	"log_max_index"};
	ColumnWithNameAndType col_log_pointer		{ new ColumnUInt64,	new DataTypeUInt64,	"log_pointer"};
	ColumnWithNameAndType col_total_replicas	{ new ColumnUInt8,	new DataTypeUInt8,	"total_replicas"};
	ColumnWithNameAndType col_active_replicas	{ new ColumnUInt8,	new DataTypeUInt8,	"active_replicas"};

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
		col_future_parts		.column->insert(UInt64(status.future_parts));
		col_parts_to_check		.column->insert(UInt64(status.parts_to_check));
		col_zookeeper_path		.column->insert(status.zookeeper_path);
		col_replica_name		.column->insert(status.replica_name);
		col_replica_path		.column->insert(status.replica_path);
		col_columns_version		.column->insert(Int64(status.columns_version));
		col_queue_size			.column->insert(UInt64(status.queue_size));
		col_inserts_in_queue	.column->insert(UInt64(status.inserts_in_queue));
		col_merges_in_queue		.column->insert(UInt64(status.merges_in_queue));
		col_queue_oldest_time	.column->insert(UInt64(status.queue_oldest_time));
		col_log_max_index		.column->insert(status.log_max_index);
		col_log_pointer			.column->insert(status.log_pointer);
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
		col_log_max_index,
		col_log_pointer,
		col_total_replicas,
		col_active_replicas,
	};

	return BlockInputStreams(1, new OneBlockInputStream(block));
}


}
