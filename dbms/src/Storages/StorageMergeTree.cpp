#include <DB/Core/FieldVisitors.h>
#include <DB/Storages/StorageMergeTree.h>
#include <DB/Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <DB/Storages/MergeTree/DiskSpaceMonitor.h>
#include <DB/Storages/MergeTree/MergeList.h>
#include <DB/Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <Poco/DirectoryIterator.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int ABORTED;
	extern const int BAD_ARGUMENTS;
}


StorageMergeTree::StorageMergeTree(
	const String & path_,
	const String & database_name_,
	const String & table_name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_,
	const ASTPtr & sampling_expression_, /// nullptr, если семплирование не поддерживается.
	size_t index_granularity_,
	const MergeTreeData::MergingParams & merging_params_,
	const MergeTreeSettings & settings_)
    : IStorage{materialized_columns_, alias_columns_, column_defaults_},
	path(path_), database_name(database_name_), table_name(table_name_), full_path(path + escapeForFileName(table_name) + '/'),
	context(context_), background_pool(context_.getBackgroundPool()),
	data(full_path, columns_,
		 materialized_columns_, alias_columns_, column_defaults_,
		 context_, primary_expr_ast_, date_column_name_,
		 sampling_expression_, index_granularity_, merging_params_,
		 settings_, database_name_ + "." + table_name, false),
	reader(data), writer(data), merger(data),
	increment(0),
	log(&Logger::get(database_name_ + "." + table_name + " (StorageMergeTree)")),
	shutdown_called(false)
{
	data.loadDataParts(false);
	data.clearOldParts();
	data.clearOldTemporaryDirectories();
	increment.set(data.getMaxDataPartIndex());

	/** Если остался старый (не использующийся сейчас) файл increment.txt, то удалим его.
	  * Это нужно сделать, чтобы избежать ситуации, когда из-за копирования данных
	  *  от сервера с новой версией (но с оставшимся некорректным и неиспользуемым increment.txt)
	  *  на сервер со старой версией (где increment.txt используется),
	  * будет скопирован и использован некорректный increment.txt.
	  *
	  * Это - защита от очень редкого гипотетического случая.
	  * Он может достигаться в БК, где довольно медленно обновляют ПО,
	  *  но зато часто делают копирование данных rsync-ом.
	  */
	{
		Poco::File obsolete_increment_txt(full_path + "increment.txt");
		if (obsolete_increment_txt.exists())
		{
			LOG_INFO(log, "Removing obsolete file " << full_path << "increment.txt");
			obsolete_increment_txt.remove();
		}
	}
}

StoragePtr StorageMergeTree::create(
	const String & path_, const String & database_name_, const String & table_name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_,
	const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	const MergeTreeData::MergingParams & merging_params_,
	const MergeTreeSettings & settings_)
{
	auto res = new StorageMergeTree{
		path_, database_name_, table_name_,
		columns_, materialized_columns_, alias_columns_, column_defaults_,
		context_, primary_expr_ast_, date_column_name_,
		sampling_expression_, index_granularity_, merging_params_, settings_
	};
	StoragePtr res_ptr = res->thisPtr();

	res->merge_task_handle = res->background_pool.addTask(std::bind(&StorageMergeTree::mergeTask, res, std::placeholders::_1));

	return res_ptr;
}


void StorageMergeTree::shutdown()
{
	if (shutdown_called)
		return;
	shutdown_called = true;
	merger.cancel();
	background_pool.removeTask(merge_task_handle);
}


StorageMergeTree::~StorageMergeTree()
{
	shutdown();
}

BlockInputStreams StorageMergeTree::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned threads)
{
	auto & select = typeid_cast<const ASTSelectQuery &>(*query);

	/// Try transferring some condition from WHERE to PREWHERE if enabled and viable
	if (settings.optimize_move_to_prewhere)
		if (select.where_expression && !select.prewhere_expression)
			MergeTreeWhereOptimizer{query, context, data, column_names, log};

	return reader.read(column_names, query, context, settings, processed_stage, max_block_size, threads, nullptr, 0);
}

BlockOutputStreamPtr StorageMergeTree::write(ASTPtr query, const Settings & settings)
{
	return new MergeTreeBlockOutputStream(*this);
}

void StorageMergeTree::drop()
{
	shutdown();
	data.dropAllData();
}

void StorageMergeTree::rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name)
{
	std::string new_full_path = new_path_to_db + escapeForFileName(new_table_name) + '/';

	data.setPath(new_full_path, true);

	path = new_path_to_db;
	table_name = new_table_name;
	full_path = new_full_path;

	/// TODO: Можно обновить названия логгеров у this, data, reader, writer, merger.
}

void StorageMergeTree::alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context)
{
	/// NOTE: Здесь так же как в ReplicatedMergeTree можно сделать ALTER, не блокирующий запись данных надолго.
	const MergeTreeMergeBlocker merge_blocker{merger};

	auto table_soft_lock = lockDataForAlter();

	data.checkAlter(params);

	auto new_columns = data.getColumnsListNonMaterialized();
	auto new_materialized_columns = data.materialized_columns;
	auto new_alias_columns = data.alias_columns;
	auto new_column_defaults = data.column_defaults;

	params.apply(new_columns, new_materialized_columns, new_alias_columns, new_column_defaults);

	auto columns_for_parts = new_columns;
	columns_for_parts.insert(std::end(columns_for_parts),
		std::begin(new_materialized_columns), std::end(new_materialized_columns));

	MergeTreeData::DataParts parts = data.getDataParts();
	std::vector<MergeTreeData::AlterDataPartTransactionPtr> transactions;
	for (const MergeTreeData::DataPartPtr & part : parts)
	{
		if (auto transaction = data.alterDataPart(part, columns_for_parts))
			transactions.push_back(std::move(transaction));
	}

	auto table_hard_lock = lockStructureForAlter();

	InterpreterAlterQuery::updateMetadata(database_name, table_name, new_columns,
		new_materialized_columns, new_alias_columns, new_column_defaults, context);

	materialized_columns = new_materialized_columns;
	alias_columns = new_alias_columns;
	column_defaults = new_column_defaults;

	data.setColumnsList(new_columns);
	data.materialized_columns = std::move(new_materialized_columns);
	data.alias_columns = std::move(new_alias_columns);
	data.column_defaults = std::move(new_column_defaults);

	for (auto & transaction : transactions)
	{
		transaction->commit();
	}
}

bool StorageMergeTree::merge(size_t aio_threshold, bool aggressive, BackgroundProcessingPool::Context * pool_context)
{
	/// Удаляем старые куски.
	data.clearOldParts();
	data.clearOldTemporaryDirectories();	/// TODO Делать это реже.

	auto structure_lock = lockStructure(true);

	size_t disk_space = DiskSpaceMonitor::getUnreservedFreeSpace(full_path);

	/// Нужно вызывать деструктор под незалоченным currently_merging_mutex.
	CurrentlyMergingPartsTaggerPtr merging_tagger;
	String merged_name;

	{
		Poco::ScopedLock<Poco::FastMutex> lock(currently_merging_mutex);

		MergeTreeData::DataPartsVector parts;
		auto can_merge = std::bind(&StorageMergeTree::canMergeParts, this, std::placeholders::_1, std::placeholders::_2);
		/// Если слияние запущено из пула потоков, и хотя бы половина потоков сливает большие куски,
		///  не будем сливать большие куски.
		size_t big_merges = background_pool.getCounter("big merges");
		bool only_small = pool_context && big_merges * 2 >= background_pool.getNumberOfThreads();

		if (!merger.selectPartsToMerge(parts, merged_name, disk_space, false, aggressive, only_small, can_merge) &&
			!merger.selectPartsToMerge(parts, merged_name, disk_space,  true, aggressive, only_small, can_merge))
		{
			return false;
		}

		merging_tagger = new CurrentlyMergingPartsTagger(parts, MergeTreeDataMerger::estimateDiskSpaceForMerge(parts), *this);

		/// Если собираемся сливать большие куски, увеличим счетчик потоков, сливающих большие куски.
		if (pool_context)
		{
			for (const auto & part : parts)
			{
				if (part->size_in_bytes > data.settings.max_bytes_to_merge_parts_small)
				{
					pool_context->incrementCounter("big merges");
					break;
				}
			}
		}
	}

	const auto & merge_entry = context.getMergeList().insert(database_name, table_name, merged_name);

	auto new_part = merger.mergePartsToTemporaryPart(
		merging_tagger->parts, merged_name, *merge_entry, aio_threshold, time(0), &*merging_tagger->reserved_space);

	merger.renameMergedTemporaryPart(merging_tagger->parts, new_part, merged_name, nullptr);

	return true;
}

bool StorageMergeTree::mergeTask(BackgroundProcessingPool::Context & background_processing_pool_context)
{
	if (shutdown_called)
		return false;

	try
	{
		size_t aio_threshold = context.getSettings().min_bytes_to_use_direct_io;
		return merge(aio_threshold, false, &background_processing_pool_context);
	}
	catch (Exception & e)
	{
		if (e.code() == ErrorCodes::ABORTED)
		{
			LOG_INFO(log, e.message());
			return false;
		}

		throw;
	}
}


bool StorageMergeTree::canMergeParts(const MergeTreeData::DataPartPtr & left, const MergeTreeData::DataPartPtr & right)
{
	return !currently_merging.count(left) && !currently_merging.count(right);
}


void StorageMergeTree::dropPartition(ASTPtr query, const Field & partition, bool detach, bool unreplicated, const Settings & settings)
{
	if (unreplicated)
		throw Exception("UNREPLICATED option for DROP has meaning only for ReplicatedMergeTree", ErrorCodes::BAD_ARGUMENTS);

	/// Просит завершить мерджи и не позволяет им начаться.
	/// Это защищает от "оживания" данных за удалённую партицию после завершения мерджа.
	const MergeTreeMergeBlocker merge_blocker{merger};
	/// Дожидается завершения мерджей и не даёт начаться новым.
	auto lock = lockForAlter();

	DayNum_t month = MergeTreeData::getMonthDayNum(partition);

	size_t removed_parts = 0;
	MergeTreeData::DataParts parts = data.getDataParts();

	for (const auto & part : parts)
	{
		if (part->month != month)
			continue;

		LOG_DEBUG(log, "Removing part " << part->name);
		++removed_parts;

		if (detach)
			data.renameAndDetachPart(part, "");
		else
			data.replaceParts({part}, {}, false);
	}

	LOG_INFO(log, (detach ? "Detached " : "Removed ") << removed_parts << " parts inside " << apply_visitor(FieldVisitorToString(), partition) << ".");
}


void StorageMergeTree::attachPartition(ASTPtr query, const Field & field, bool unreplicated, bool part, const Settings & settings)
{
	if (unreplicated)
		throw Exception("UNREPLICATED option for ATTACH has meaning only for ReplicatedMergeTree", ErrorCodes::BAD_ARGUMENTS);

	String partition;

	if (part)
		partition = field.getType() == Field::Types::UInt64 ? toString(field.get<UInt64>()) : field.safeGet<String>();
	else
		partition = MergeTreeData::getMonthName(field);

	String source_dir = "detached/";

	/// Составим список кусков, которые нужно добавить.
	Strings parts;
	if (part)
	{
		parts.push_back(partition);
	}
	else
	{
		LOG_DEBUG(log, "Looking for parts for partition " << partition << " in " << source_dir);
		ActiveDataPartSet active_parts;
		for (Poco::DirectoryIterator it = Poco::DirectoryIterator(full_path + source_dir); it != Poco::DirectoryIterator(); ++it)
		{
			String name = it.name();
			if (!ActiveDataPartSet::isPartDirectory(name))
				continue;
			if (name.substr(0, partition.size()) != partition)
				continue;
			LOG_DEBUG(log, "Found part " << name);
			active_parts.add(name);
		}
		LOG_DEBUG(log, active_parts.size() << " of them are active");
		parts = active_parts.getParts();
	}

	for (const auto & source_part_name : parts)
	{
		String source_path = source_dir + source_part_name;

		LOG_DEBUG(log, "Checking data");
		MergeTreeData::MutableDataPartPtr part = data.loadPartAndFixMetadata(source_path);

		LOG_INFO(log, "Attaching part " << source_part_name << " from " << source_path);
		data.renameTempPartAndAdd(part, &increment);

		LOG_INFO(log, "Finished attaching part");
	}

	/// На месте удаленных кусков могут появиться новые, с другими данными.
	context.resetCaches();
}


void StorageMergeTree::freezePartition(const Field & partition, const Settings & settings)
{
	/// Префикс может быть произвольным. Не обязательно месяц - можно указать лишь год.
	data.freezePartition(partition.getType() == Field::Types::UInt64
		? toString(partition.get<UInt64>())
		: partition.safeGet<String>());
}

}
