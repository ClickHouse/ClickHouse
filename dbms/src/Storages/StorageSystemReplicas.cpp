#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Storages/StorageSystemReplicas.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>


namespace DB
{


StorageSystemReplicas::StorageSystemReplicas(const std::string & name_, const Context & context_)
	: name(name_), context(context_)
	, columns{
		{ "database", 				new DataTypeString	},
		{ "name", 					new DataTypeString	},
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
		{ "log_max_index",			new DataTypeUInt64	},
		{ "log_pointer", 			new DataTypeUInt64	},
		{ "total_replicas",			new DataTypeUInt8	},
		{ "active_replicas", 		new DataTypeUInt8	},
	}
{
}

StoragePtr StorageSystemReplicas::create(const std::string & name_, const Context & context_)
{
	return (new StorageSystemReplicas(name_, context_))->thisPtr();
}


BlockInputStreams StorageSystemReplicas::read(
	const Names & column_names, ASTPtr query, const Settings & settings,
	QueryProcessingStage::Enum & processed_stage, size_t max_block_size, unsigned threads)
{
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;

	/// Собираем набор реплицируемых таблиц.
	Databases replicated_tables;
	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

		for (const auto & db : context.getDatabases())
			for (const auto & table : db.second)
				if (typeid_cast<const StorageReplicatedMergeTree *>(&*table.second))
					replicated_tables[db.first][table.first] = table.second;
	}

	ColumnWithNameAndType col_database			{ new ColumnString,	new DataTypeString,	"database"};
	ColumnWithNameAndType col_name				{ new ColumnString,	new DataTypeString,	"name"};
	ColumnWithNameAndType col_engine			{ new ColumnString,	new DataTypeString,	"engine"};
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
	ColumnWithNameAndType col_log_max_index		{ new ColumnUInt64,	new DataTypeUInt64,	"log_max_index"};
	ColumnWithNameAndType col_log_pointer		{ new ColumnUInt64,	new DataTypeUInt64,	"log_pointer"};
	ColumnWithNameAndType col_total_replicas	{ new ColumnUInt8,	new DataTypeUInt8,	"total_replicas"};
	ColumnWithNameAndType col_active_replicas	{ new ColumnUInt8,	new DataTypeUInt8,	"active_replicas"};

	for (auto & db : replicated_tables)
	{
		for (auto & table : db.second)
		{
			col_database.column->insert(db.first);
			col_name.column->insert(table.first);
			col_engine.column->insert(table.second->getName());

			StorageReplicatedMergeTree::Status status;
			typeid_cast<StorageReplicatedMergeTree &>(*table.second).getStatus(status);

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
			col_log_max_index		.column->insert(status.log_max_index);
			col_log_pointer			.column->insert(status.log_pointer);
			col_total_replicas		.column->insert(UInt64(status.total_replicas));
			col_active_replicas		.column->insert(UInt64(status.active_replicas));
		}
	}

	Block block{
		col_database,
		col_name,
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
		col_log_max_index,
		col_log_pointer,
		col_total_replicas,
		col_active_replicas,
	};

	return BlockInputStreams(1, new OneBlockInputStream(block));
}


}
