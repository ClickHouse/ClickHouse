#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Storages/System/StorageSystemParts.h>
#include <DB/Storages/StorageMergeTree.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Common/VirtualColumnUtils.h>


namespace DB
{


StorageSystemParts::StorageSystemParts(const std::string & name_)
	: name(name_),
	columns
	{
		{"partition", 			new DataTypeString},
		{"name", 				new DataTypeString},
		{"replicated",			new DataTypeUInt8},
		{"active",				new DataTypeUInt8},
		{"marks",				new DataTypeUInt64},
		{"bytes",				new DataTypeUInt64},
		{"modification_time",	new DataTypeDateTime},
		{"remove_time",			new DataTypeDateTime},
		{"refcount",			new DataTypeUInt32},
		{"min_date",			new DataTypeDate},
		{"max_date",			new DataTypeDate},
		{"min_block_number",	new DataTypeInt64},
		{"max_block_number",	new DataTypeInt64},
		{"level",				new DataTypeUInt32},

		{"database", 			new DataTypeString},
		{"table", 				new DataTypeString},
		{"engine", 				new DataTypeString},
	}
{
}

StoragePtr StorageSystemParts::create(const std::string & name_)
{
	return (new StorageSystemParts(name_))->thisPtr();
}


BlockInputStreams StorageSystemParts::read(
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

	/// Будем поочередно применять WHERE к подмножеству столбцов и добавлять столбцы.
	/// Получилось довольно запутанно, но условия в WHERE учитываются почти везде, где можно.

	Block block;

	std::map<std::pair<String, String>, StoragePtr> storages;

	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

		const Databases & databases = context.getDatabases();

		/// Добавим столбец database.
		ColumnPtr database_column = new ColumnString;
		for (const auto & database : databases)
			database_column->insert(database.first);
		block.insert(ColumnWithTypeAndName(database_column, new DataTypeString, "database"));

		/// Отфильтруем блок со столбцом database.
		VirtualColumnUtils::filterBlockWithQuery(query, block, context);

		if (!block.rows())
			return BlockInputStreams();

		/// Добавим столбцы table и engine, active и replicated.
		database_column = block.getByName("database").column;
		size_t rows = database_column->size();

		IColumn::Offsets_t offsets(rows);
		ColumnPtr table_column = new ColumnString;
		ColumnPtr engine_column = new ColumnString;
		ColumnPtr replicated_column = new ColumnUInt8;
		ColumnPtr active_column = new ColumnUInt8;

		for (size_t i = 0; i < rows; ++i)
		{
			String database = (*database_column)[i].get<String>();
			const Tables & tables = databases.at(database);
			offsets[i] = i ? offsets[i - 1] : 0;
			for (const auto & table : tables)
			{
				StoragePtr storage = table.second;
				if (!dynamic_cast<StorageMergeTree *>(&*storage) &&
					!dynamic_cast<StorageReplicatedMergeTree *>(&*storage))
					continue;

				storages[std::make_pair(database, table.first)] = storage;

				/// Добавим все 4 комбинации флагов replicated и active.
				table_column->insert(table.first);
				engine_column->insert(storage->getName());
				replicated_column->insert(static_cast<UInt64>(0));
				active_column->insert(static_cast<UInt64>(0));

				table_column->insert(table.first);
				engine_column->insert(storage->getName());
				replicated_column->insert(static_cast<UInt64>(0));
				active_column->insert(static_cast<UInt64>(1));

				table_column->insert(table.first);
				engine_column->insert(storage->getName());
				replicated_column->insert(static_cast<UInt64>(1));
				active_column->insert(static_cast<UInt64>(0));

				table_column->insert(table.first);
				engine_column->insert(storage->getName());
				replicated_column->insert(static_cast<UInt64>(1));
				active_column->insert(static_cast<UInt64>(1));

				offsets[i] += 4;
			}
		}

		for (size_t i = 0; i < block.columns(); ++i)
		{
			ColumnPtr & column = block.getByPosition(i).column;
			column = column->replicate(offsets);
		}

		block.insert(ColumnWithTypeAndName(table_column, new DataTypeString, "table"));
		block.insert(ColumnWithTypeAndName(engine_column, new DataTypeString, "engine"));
		block.insert(ColumnWithTypeAndName(replicated_column, new DataTypeUInt8, "replicated"));
		block.insert(ColumnWithTypeAndName(active_column, new DataTypeUInt8, "active"));
	}

	/// Отфильтруем блок со столбцами database, table, engine, replicated и active.
	VirtualColumnUtils::filterBlockWithQuery(query, block, context);

	if (!block.rows())
		return BlockInputStreams();

	ColumnPtr filtered_database_column = block.getByName("database").column;
	ColumnPtr filtered_table_column = block.getByName("table").column;
	ColumnPtr filtered_replicated_column = block.getByName("replicated").column;
	ColumnPtr filtered_active_column = block.getByName("active").column;

	/// Наконец составим результат.
	ColumnPtr database_column = new ColumnString;
	ColumnPtr table_column = new ColumnString;
	ColumnPtr engine_column = new ColumnString;
	ColumnPtr partition_column = new ColumnString;
	ColumnPtr name_column = new ColumnString;
	ColumnPtr replicated_column = new ColumnUInt8;
	ColumnPtr active_column = new ColumnUInt8;
	ColumnPtr marks_column = new ColumnUInt64;
	ColumnPtr bytes_column = new ColumnUInt64;
	ColumnPtr modification_time_column = new ColumnUInt32;
	ColumnPtr remove_time_column = new ColumnUInt32;
	ColumnPtr refcount_column = new ColumnUInt32;
	ColumnPtr min_date_column = new ColumnUInt16;
	ColumnPtr max_date_column = new ColumnUInt16;
	ColumnPtr min_block_number_column = new ColumnInt64;
	ColumnPtr max_block_number_column = new ColumnInt64;
	ColumnPtr level_column = new ColumnUInt32;

	for (size_t i = 0; i < filtered_database_column->size();)
	{
		String database = (*filtered_database_column)[i].get<String>();
		String table = (*filtered_table_column)[i].get<String>();

		/// Посмотрим, какие комбинации значений replicated, active нам нужны.
		bool need[2][2]{}; /// [replicated][active]
		for (; i < filtered_database_column->size() &&
			(*filtered_database_column)[i].get<String>() == database &&
			(*filtered_table_column)[i].get<String>() == table; ++i)
		{
			bool replicated = !!(*filtered_replicated_column)[i].get<UInt64>();
			bool active = !!(*filtered_active_column)[i].get<UInt64>();
			need[replicated][active] = true;
		}

		StoragePtr storage = storages.at(std::make_pair(database, table));
		auto table_lock = storage->lockStructure(false); /// Чтобы таблицу не удалили.

		String engine = storage->getName();

		MergeTreeData * data[2]{}; /// [0] - unreplicated, [1] - replicated.

		if (StorageMergeTree * merge_tree = dynamic_cast<StorageMergeTree *>(&*storage))
		{
			data[0] = &merge_tree->getData();
		}
		else if (StorageReplicatedMergeTree * replicated_merge_tree = dynamic_cast<StorageReplicatedMergeTree *>(&*storage))
		{
			data[0] = replicated_merge_tree->getUnreplicatedData();
			data[1] = &replicated_merge_tree->getData();
		}

		for (UInt64 replicated = 0; replicated <= 1; ++replicated)
		{
			if (!need[replicated][0] && !need[replicated][1])
				continue;
			if (!data[replicated])
				continue;

			MergeTreeData::DataParts active_parts = data[replicated]->getDataParts();
			MergeTreeData::DataParts all_parts;
			if (need[replicated][0])
				all_parts = data[replicated]->getAllDataParts();
			else
				all_parts = active_parts;

			/// Наконец пройдем по списку кусочков.
			for (const MergeTreeData::DataPartPtr & part : all_parts)
			{
				database_column->insert(database);
				table_column->insert(table);
				engine_column->insert(engine);

				mysqlxx::Date partition_date {part->month};
				String partition = toString(partition_date.year()) + (partition_date.month() < 10 ? "0" : "") + toString(partition_date.month());
				partition_column->insert(partition);

				name_column->insert(part->name);
				replicated_column->insert(replicated);
				active_column->insert(static_cast<UInt64>(!need[replicated][0] || active_parts.count(part)));
				marks_column->insert(part->size);
				bytes_column->insert(static_cast<size_t>(part->size_in_bytes));
				modification_time_column->insert(part->modification_time);
				remove_time_column->insert(part->remove_time);
				min_date_column->insert(static_cast<UInt64>(part->left_date));
				max_date_column->insert(static_cast<UInt64>(part->right_date));
				min_block_number_column->insert(part->left);
				max_block_number_column->insert(part->right);
				level_column->insert(static_cast<UInt64>(part->level));

				/// В выводимом refcount, для удобства, не учиытываем тот, что привнесён локальными переменными all_parts, active_parts.
				refcount_column->insert(part.use_count() - (active_parts.count(part) ? 2 : 1));
			}
		}
	}

	block.clear();

	block.insert(ColumnWithTypeAndName(partition_column, 			new DataTypeString, 	"partition"));
	block.insert(ColumnWithTypeAndName(name_column, 				new DataTypeString, 	"name"));
	block.insert(ColumnWithTypeAndName(replicated_column, 			new DataTypeUInt8,		"replicated"));
	block.insert(ColumnWithTypeAndName(active_column, 				new DataTypeUInt8, 		"active"));
	block.insert(ColumnWithTypeAndName(marks_column, 				new DataTypeUInt64, 	"marks"));
	block.insert(ColumnWithTypeAndName(bytes_column, 				new DataTypeUInt64, 	"bytes"));
	block.insert(ColumnWithTypeAndName(modification_time_column, 	new DataTypeDateTime, 	"modification_time"));
	block.insert(ColumnWithTypeAndName(remove_time_column, 			new DataTypeDateTime, 	"remove_time"));
	block.insert(ColumnWithTypeAndName(refcount_column, 			new DataTypeUInt32, 	"refcount"));
	block.insert(ColumnWithTypeAndName(min_date_column,				new DataTypeDate,		"min_date"));
	block.insert(ColumnWithTypeAndName(max_date_column,				new DataTypeDate,		"max_date"));
	block.insert(ColumnWithTypeAndName(min_block_number_column,		new DataTypeInt64,		"min_block_number"));
	block.insert(ColumnWithTypeAndName(max_block_number_column,		new DataTypeInt64,		"max_block_number"));
	block.insert(ColumnWithTypeAndName(level_column,				new DataTypeUInt32,		"level"));
	block.insert(ColumnWithTypeAndName(database_column, 			new DataTypeString, 	"database"));
	block.insert(ColumnWithTypeAndName(table_column, 				new DataTypeString, 	"table"));
	block.insert(ColumnWithTypeAndName(engine_column, 				new DataTypeString, 	"engine"));

	return BlockInputStreams(1, new OneBlockInputStream(block));
}


}
