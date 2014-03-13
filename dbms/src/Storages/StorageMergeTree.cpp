#include <DB/Storages/StorageMergeTree.h>
#include <DB/Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <DB/Storages/MergeTree/DiskSpaceMonitor.h>
#include <DB/Common/escapeForFileName.h>

namespace DB
{


StorageMergeTree::StorageMergeTree(const String & path_, const String & name_, NamesAndTypesListPtr columns_,
				const Context & context_,
				ASTPtr & primary_expr_ast_,
				const String & date_column_name_,
				const ASTPtr & sampling_expression_, /// NULL, если семплирование не поддерживается.
				size_t index_granularity_,
				MergeTreeData::Mode mode_,
				const String & sign_column_,
				const MergeTreeSettings & settings_)
	: path(path_), name(name_), full_path(path + escapeForFileName(name) + '/'), increment(full_path + "increment.txt"),
	data(	full_path, columns_, context_, primary_expr_ast_, date_column_name_, sampling_expression_,
			index_granularity_,mode_, sign_column_, settings_),
	reader(data), writer(data), merger(data),
	log(&Logger::get("StorageMergeTree")),
	shutdown_called(false)
{
	merge_threads = new boost::threadpool::pool(data.settings.merging_threads);

	increment.fixIfBroken(data.getMaxDataPartIndex());
}

StoragePtr StorageMergeTree::create(
	const String & path_, const String & name_, NamesAndTypesListPtr columns_,
	const Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_,
	const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	MergeTreeData::Mode mode_,
	const String & sign_column_,
	const MergeTreeSettings & settings_)
{
	StorageMergeTree * storage = new StorageMergeTree(
		path_, name_, columns_, context_, primary_expr_ast_, date_column_name_,
		sampling_expression_, index_granularity_, mode_, sign_column_, settings_);
	StoragePtr ptr = storage->thisPtr();
	storage->data.setOwningStorage(ptr);
	return ptr;
}

void StorageMergeTree::shutdown()
{
	if (shutdown_called)
		return;
	shutdown_called = true;
	merger.cancelAll();

	joinMergeThreads();
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
	return new MergeTreeBlockOutputStream(thisPtr());
}

void StorageMergeTree::dropImpl()
{
	merger.cancelAll();
	joinMergeThreads();
	data.dropAllData();
}

void StorageMergeTree::rename(const String & new_path_to_db, const String & new_name)
{
	BigLockPtr lock = lockAllOperations();

	std::string new_full_path = new_path_to_db + escapeForFileName(new_name) + '/';

	data.setPath(new_full_path);

	path = new_path_to_db;
	name = new_name;
	full_path = new_full_path;

	increment.setPath(full_path + "increment.txt");
}

void StorageMergeTree::alter(const ASTAlterQuery::Parameters & params)
{
	/// InterpreterAlterQuery уже взял BigLock.

	data.alter(params);
}

void StorageMergeTree::merge(size_t iterations, bool async, bool aggressive)
{
	bool while_can = false;
	if (iterations == 0)
	{
		while_can = true;
		iterations = data.settings.merging_threads;
	}

	for (size_t i = 0; i < iterations; ++i)
		merge_threads->schedule(boost::bind(&StorageMergeTree::mergeThread, this, while_can, aggressive));

	if (!async)
		joinMergeThreads();
}


void StorageMergeTree::mergeThread(bool while_can, bool aggressive)
{
	try
	{
		while (!shutdown_called)
		{
			/// Удаляем старые куски. На случай, если в слиянии что-то сломано, и из следующего блока вылетит исключение.
			data.clearOldParts();

			size_t disk_space = DiskSpaceMonitor::getUnreservedFreeSpace(full_path);

			{
				/// К концу этого логического блока должен быть вызван деструктор, чтобы затем корректно определить удаленные куски
				/// Нужно вызывать деструктор под незалоченным currently_merging_mutex.
				CurrentlyMergingPartsTaggerPtr merging_tagger;

				{
					Poco::ScopedLock<Poco::FastMutex> lock(currently_merging_mutex);

					MergeTreeData::DataPartsVector parts;
					auto can_merge = boost::bind(&StorageMergeTree::canMergeParts, this, _1, _2);
					bool only_small = false;

					/// Если есть активный мердж крупных кусков, то ограничиваемся мерджем только маленьких частей.
					for (const auto & part : currently_merging)
					{
						if (part->size * data.index_granularity > 25 * 1024 * 1024)
						{
							only_small = true;
							break;
						}
					}

					if (!merger.selectPartsToMerge(parts, disk_space, false, aggressive, only_small, can_merge) &&
						!merger.selectPartsToMerge(parts, disk_space,  true, aggressive, only_small, can_merge))
						break;

					merging_tagger = new CurrentlyMergingPartsTagger(parts, merger.estimateDiskSpaceForMerge(parts), *this);
				}

				merger.mergeParts(merging_tagger->parts);
			}

			if (shutdown_called)
				break;

			/// Удаляем куски, которые мы только что сливали.
			data.clearOldParts();

			if (!while_can)
				break;
		}
	}
	catch (const Exception & e)
	{
		LOG_ERROR(log, "Code: " << e.code() << ". " << e.displayText() << std::endl
			<< std::endl
			<< "Stack trace:" << std::endl
			<< e.getStackTrace().toString());
	}
	catch (const Poco::Exception & e)
	{
		LOG_ERROR(log, "Poco::Exception: " << e.code() << ". " << e.displayText());
	}
	catch (const std::exception & e)
	{
		LOG_ERROR(log, "std::exception: " << e.what());
	}
	catch (...)
	{
		LOG_ERROR(log, "Unknown exception");
	}
}


void StorageMergeTree::joinMergeThreads()
{
	LOG_DEBUG(log, "Waiting for merge threads to finish.");
	merge_threads->wait();
}

bool StorageMergeTree::canMergeParts(const MergeTreeData::DataPartPtr & left, const MergeTreeData::DataPartPtr & right)
{
	return !currently_merging.count(left) && !currently_merging.count(right);
}

}
