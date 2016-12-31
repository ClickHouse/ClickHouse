#include <experimental/optional>
#include <DB/Core/FieldVisitors.h>
#include <DB/Storages/StorageMergeTree.h>
#include <DB/Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <DB/Storages/MergeTree/DiskSpaceMonitor.h>
#include <DB/Storages/MergeTree/MergeList.h>
#include <DB/Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <DB/Databases/IDatabase.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/Parsers/ASTFunction.h>
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
	bool has_force_restore_data_flag,
	const MergeTreeSettings & settings_)
    : IStorage{materialized_columns_, alias_columns_, column_defaults_},
	path(path_), database_name(database_name_), table_name(table_name_), full_path(path + escapeForFileName(table_name) + '/'),
	context(context_), background_pool(context_.getBackgroundPool()),
	data(full_path, columns_,
		 materialized_columns_, alias_columns_, column_defaults_,
		 context_, primary_expr_ast_, date_column_name_,
		 sampling_expression_, index_granularity_, merging_params_,
		 settings_, database_name_ + "." + table_name, false),
	reader(data), writer(data), merger(data, context.getBackgroundPool()),
	increment(0),
	log(&Logger::get(database_name_ + "." + table_name + " (StorageMergeTree)"))
{
	data.loadDataParts(has_force_restore_data_flag);
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
	bool has_force_restore_data_flag_,
	const MergeTreeSettings & settings_)
{
	auto res = make_shared(
		path_, database_name_, table_name_,
		columns_, materialized_columns_, alias_columns_, column_defaults_,
		context_, primary_expr_ast_, date_column_name_,
		sampling_expression_, index_granularity_, merging_params_, has_force_restore_data_flag_, settings_
	);
	res->merge_task_handle = res->background_pool.addTask(std::bind(&StorageMergeTree::mergeTask, res.get()));

	return res;
}


void StorageMergeTree::shutdown()
{
	if (shutdown_called)
		return;
	shutdown_called = true;
	merger.cancelForever();
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
	if (settings.optimize_move_to_prewhere && select.where_expression && !select.prewhere_expression && !select.final())
		MergeTreeWhereOptimizer{query, context, data, column_names, log};

	return reader.read(column_names, query, context, settings, processed_stage, max_block_size, threads, nullptr, 0);
}

BlockOutputStreamPtr StorageMergeTree::write(ASTPtr query, const Settings & settings)
{
	return std::make_shared<MergeTreeBlockOutputStream>(*this);
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

	/// NOTE: Logger names are not updated.
}

void StorageMergeTree::alter(
	const AlterCommands & params,
	const String & database_name,
	const String & table_name,
	const Context & context)
{
	/// NOTE: Здесь так же как в ReplicatedMergeTree можно сделать ALTER, не блокирующий запись данных надолго.
	auto merge_blocker = merger.cancel();

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

	std::vector<MergeTreeData::AlterDataPartTransactionPtr> transactions;

	bool primary_key_is_modified = false;
	ASTPtr new_primary_key_ast = data.primary_expr_ast;

	for (const AlterCommand & param : params)
	{
		if (param.type == AlterCommand::MODIFY_PRIMARY_KEY)
		{
			primary_key_is_modified = true;
			new_primary_key_ast = param.primary_key;
		}
	}

	if (primary_key_is_modified && data.merging_params.mode == MergeTreeData::MergingParams::Unsorted)
		throw Exception("UnsortedMergeTree cannot have primary key", ErrorCodes::BAD_ARGUMENTS);

	if (primary_key_is_modified && supportsSampling())
		throw Exception("MODIFY PRIMARY KEY only supported for tables without sampling key", ErrorCodes::BAD_ARGUMENTS);

	MergeTreeData::DataParts parts = data.getAllDataParts();
	for (const MergeTreeData::DataPartPtr & part : parts)
		if (auto transaction = data.alterDataPart(part, columns_for_parts, new_primary_key_ast, false))
			transactions.push_back(std::move(transaction));

	auto table_hard_lock = lockStructureForAlter();

	IDatabase::ASTModifier engine_modifier;
	if (primary_key_is_modified)
		engine_modifier = [&new_primary_key_ast] (ASTPtr & engine_ast)
		{
			auto tuple = std::make_shared<ASTFunction>(new_primary_key_ast->range);
			tuple->name = "tuple";
			tuple->arguments = new_primary_key_ast;
			tuple->children.push_back(tuple->arguments);

			/// Первичный ключ находится на втором месте в описании движка таблицы и может быть представлен в виде кортежа.
			/// TODO: Не всегда на втором месте. Если есть ключ сэмплирования, то на третьем. Исправить.
			typeid_cast<ASTExpressionList &>(*typeid_cast<ASTFunction &>(*engine_ast).arguments).children.at(1) = tuple;
		};

	context.getDatabase(database_name)->alterTable(
		context, table_name,
		new_columns, new_materialized_columns, new_alias_columns, new_column_defaults,
		engine_modifier);

	materialized_columns = new_materialized_columns;
	alias_columns = new_alias_columns;
	column_defaults = new_column_defaults;

	data.setColumnsList(new_columns);
	data.materialized_columns = std::move(new_materialized_columns);
	data.alias_columns = std::move(new_alias_columns);
	data.column_defaults = std::move(new_column_defaults);

	if (primary_key_is_modified)
	{
		data.primary_expr_ast = new_primary_key_ast;
		data.initPrimaryKey();
	}

	for (auto & transaction : transactions)
		transaction->commit();

	if (primary_key_is_modified)
		data.loadDataParts(false);
}


/// While exists, marks parts as 'currently_merging' and reserves free space on filesystem.
/// It's possible to mark parts before.
struct CurrentlyMergingPartsTagger
{
	MergeTreeData::DataPartsVector parts;
	DiskSpaceMonitor::ReservationPtr reserved_space;
	StorageMergeTree * storage = nullptr;

	CurrentlyMergingPartsTagger() = default;

	CurrentlyMergingPartsTagger(const MergeTreeData::DataPartsVector & parts_, size_t total_size, StorageMergeTree & storage_)
		: parts(parts_), storage(&storage_)
	{
		/// Assume mutex is already locked, because this method is called from mergeTask.
		reserved_space = DiskSpaceMonitor::reserve(storage->full_path, total_size); /// May throw.
		for (const auto & part : parts)
		{
			if (storage->currently_merging.count(part))
				throw Exception("Tagging alreagy tagged part " + part->name + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);
		}
		storage->currently_merging.insert(parts.begin(), parts.end());
	}

	~CurrentlyMergingPartsTagger()
	{
		std::lock_guard<std::mutex> lock(storage->currently_merging_mutex);

		for (const auto & part : parts)
		{
			if (!storage->currently_merging.count(part))
				std::terminate();
			storage->currently_merging.erase(part);
		}
	}
};


bool StorageMergeTree::merge(
	size_t aio_threshold,
	bool aggressive,
	const String & partition,
	bool final)
{
	/// Clear old parts. It does not matter to do it more frequently than each second.
	if (auto lock = time_after_previous_cleanup.lockTestAndRestartAfter(1))
	{
		data.clearOldParts();
		data.clearOldTemporaryDirectories();
	}

	auto structure_lock = lockStructure(true);

	size_t disk_space = DiskSpaceMonitor::getUnreservedFreeSpace(full_path);

	/// Нужно вызывать деструктор под незалоченным currently_merging_mutex.
	std::experimental::optional<CurrentlyMergingPartsTagger> merging_tagger;
	String merged_name;

	{
		std::lock_guard<std::mutex> lock(currently_merging_mutex);

		MergeTreeData::DataPartsVector parts;

		auto can_merge = [this] (const MergeTreeData::DataPartPtr & left, const MergeTreeData::DataPartPtr & right)
		{
			return !currently_merging.count(left) && !currently_merging.count(right);
		};

		bool selected = false;

		if (partition.empty())
		{
			selected = merger.selectPartsToMerge(parts, merged_name, aggressive, merger.getMaxPartsSizeForMerge(), can_merge);
		}
		else
		{
			DayNum_t month = MergeTreeData::getMonthFromName(partition);
			selected = merger.selectAllPartsToMergeWithinPartition(parts, merged_name, disk_space, can_merge, month, final);
		}

		if (!selected)
			return false;

		merging_tagger.emplace(parts, MergeTreeDataMerger::estimateDiskSpaceForMerge(parts), *this);
	}

	MergeList::EntryPtr merge_entry_ptr = context.getMergeList().insert(database_name, table_name, merged_name);

	auto new_part = merger.mergePartsToTemporaryPart(
		merging_tagger->parts, merged_name, *merge_entry_ptr, aio_threshold, time(0), merging_tagger->reserved_space.get());

	merger.renameMergedTemporaryPart(merging_tagger->parts, new_part, merged_name, nullptr);

	return true;
}

bool StorageMergeTree::mergeTask()
{
	if (shutdown_called)
		return false;

	try
	{
		size_t aio_threshold = context.getSettings().min_bytes_to_use_direct_io;
		return merge(aio_threshold, false, {}, {});
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


void StorageMergeTree::dropPartition(ASTPtr query, const Field & partition, bool detach, bool unreplicated, const Settings & settings)
{
	if (unreplicated)
		throw Exception("UNREPLICATED option for DROP has meaning only for ReplicatedMergeTree", ErrorCodes::BAD_ARGUMENTS);

	/// Просит завершить мерджи и не позволяет им начаться.
	/// Это защищает от "оживания" данных за удалённую партицию после завершения мерджа.
	auto merge_blocker = merger.cancel();
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


void StorageMergeTree::freezePartition(const Field & partition, const String & with_name, const Settings & settings)
{
	/// Префикс может быть произвольным. Не обязательно месяц - можно указать лишь год.
	data.freezePartition(partition.getType() == Field::Types::UInt64
		? toString(partition.get<UInt64>())
		: partition.safeGet<String>(), with_name);
}

}
