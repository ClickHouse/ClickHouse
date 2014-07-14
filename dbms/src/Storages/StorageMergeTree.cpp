#include <DB/Storages/StorageMergeTree.h>
#include <DB/Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <DB/Storages/MergeTree/DiskSpaceMonitor.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/Interpreters/InterpreterAlterQuery.h>

namespace DB
{

StorageMergeTree::StorageMergeTree(const String & path_, const String & database_name_, const String & name_,
				NamesAndTypesListPtr columns_,
				Context & context_,
				ASTPtr & primary_expr_ast_,
				const String & date_column_name_,
				const ASTPtr & sampling_expression_, /// nullptr, если семплирование не поддерживается.
				size_t index_granularity_,
				MergeTreeData::Mode mode_,
				const String & sign_column_,
				const MergeTreeSettings & settings_)
	: path(path_), name(name_), full_path(path + escapeForFileName(name) + '/'), increment(full_path + "increment.txt"),
	background_pool(context_.getBackgroundPool()),
	data(full_path, columns_, context_, primary_expr_ast_, date_column_name_, sampling_expression_,
	index_granularity_,mode_, sign_column_, settings_, database_name_ + "." + name),
	reader(data), writer(data), merger(data),
	log(&Logger::get(database_name_ + "." + name + " (StorageMergeTree)")),
	shutdown_called(false)
{
	increment.fixIfBroken(data.getMaxDataPartIndex());

	data.clearOldParts();
}

StoragePtr StorageMergeTree::create(
	const String & path_, const String & database_name_, const String & name_,
	NamesAndTypesListPtr columns_,
	Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_,
	const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	MergeTreeData::Mode mode_,
	const String & sign_column_,
	const MergeTreeSettings & settings_)
{
	StorageMergeTree * res = new StorageMergeTree(
		path_, database_name_, name_, columns_, context_, primary_expr_ast_, date_column_name_,
		sampling_expression_, index_granularity_, mode_, sign_column_, settings_);
	StoragePtr res_ptr = res->thisPtr();

	res->merge_task_handle = res->background_pool.addTask(std::bind(&StorageMergeTree::mergeTask, res, std::placeholders::_1));

	return res_ptr;
}

void StorageMergeTree::shutdown()
{
	if (shutdown_called)
		return;
	shutdown_called = true;
	merger.cancelAll();
	background_pool.removeTask(merge_task_handle);
}


StorageMergeTree::~StorageMergeTree()
{
	shutdown();
}

BlockInputStreams StorageMergeTree::read(
	const Names & column_names,
	ASTPtr query,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	return reader.read(column_names, query, settings, processed_stage, max_block_size, threads);
}

BlockOutputStreamPtr StorageMergeTree::write(ASTPtr query)
{
	return new MergeTreeBlockOutputStream(*this);
}

void StorageMergeTree::drop()
{
	shutdown();
	data.dropAllData();
}

void StorageMergeTree::rename(const String & new_path_to_db, const String & new_name)
{
	std::string new_full_path = new_path_to_db + escapeForFileName(new_name) + '/';

	data.setPath(new_full_path);

	path = new_path_to_db;
	name = new_name;
	full_path = new_full_path;

	increment.setPath(full_path + "increment.txt");

	/// TODO: Можно обновить названия логгеров у this, data, reader, writer, merger.
}

void StorageMergeTree::alter(const AlterCommands & params, const String & database_name, const String & table_name, Context & context)
{
	/// NOTE: Здесь так же как в ReplicatedMergeTree можно сделать ALTER, не блокирующий запись данных надолго.

	auto table_soft_lock = lockDataForAlter();

	data.checkAlter(params);

	NamesAndTypesList new_columns = data.getColumnsList();
	params.apply(new_columns);

	MergeTreeData::DataParts parts = data.getDataParts();
	std::vector<MergeTreeData::AlterDataPartTransactionPtr> transactions;
	for (MergeTreeData::DataPartPtr part : parts)
	{
		transactions.push_back(data.alterDataPart(part, new_columns));
	}

	auto table_hard_lock = lockStructureForAlter();

	InterpreterAlterQuery::updateMetadata(database_name, table_name, new_columns, context);
	data.setColumnsList(new_columns);

	for (auto & transaction : transactions)
	{
		transaction->commit();
	}
}

bool StorageMergeTree::merge(bool aggressive, BackgroundProcessingPool::Context * pool_context)
{
	auto structure_lock = lockStructure(false);

	/// Удаляем старые куски.
	data.clearOldParts();

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
		int big_merges = background_pool.getCounter("big merges");
		bool only_small = pool_context && big_merges * 2 >= background_pool.getNumberOfThreads();

		if (!merger.selectPartsToMerge(parts, merged_name, disk_space, false, aggressive, only_small, can_merge) &&
			!merger.selectPartsToMerge(parts, merged_name, disk_space,  true, aggressive, only_small, can_merge))
		{
			return false;
		}

		merging_tagger = new CurrentlyMergingPartsTagger(parts, merger.estimateDiskSpaceForMerge(parts), *this);

		/// Если собираемся сливать большие куски, увеличим счетчик потоков, сливающих большие куски.
		if (pool_context)
		{
			for (const auto & part : parts)
			{
				if (part->size * data.index_granularity > 25 * 1024 * 1024)
				{
					pool_context->incrementCounter("big merges");
					break;
				}
			}
		}
	}

	merger.mergeParts(merging_tagger->parts, merged_name);

	return true;
}

bool StorageMergeTree::mergeTask(BackgroundProcessingPool::Context & context)
{
	if (shutdown_called)
		return false;
	try
	{
		return merge(false, &context);
	}
	catch (Exception & e)
	{
		if (e.code() == ErrorCodes::ABORTED)
		{
			LOG_INFO(log, "Merge cancelled");
			return false;
		}

		throw;
	}
}


bool StorageMergeTree::canMergeParts(const MergeTreeData::DataPartPtr & left, const MergeTreeData::DataPartPtr & right)
{
	return !currently_merging.count(left) && !currently_merging.count(right);
}

}
