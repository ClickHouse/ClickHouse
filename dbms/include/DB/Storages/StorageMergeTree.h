#pragma once

#include <ext/shared_ptr_helper.hpp>

#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <DB/Storages/MergeTree/MergeTreeDataWriter.h>
#include <DB/Storages/MergeTree/MergeTreeDataMerger.h>
#include <DB/Storages/MergeTree/DiskSpaceMonitor.h>
#include <DB/Storages/MergeTree/BackgroundProcessingPool.h>
#include <DB/Common/Increment.h>


namespace DB
{

/** См. описание структуры данных в MergeTreeData.
  */
class StorageMergeTree : private ext::shared_ptr_helper<StorageMergeTree>, public IStorage
{
friend class ext::shared_ptr_helper<StorageMergeTree>;
friend class MergeTreeBlockOutputStream;

public:
	/** Подцепить таблицу с соответствующим именем, по соответствующему пути (с / на конце),
	  *  (корректность имён и путей не проверяется)
	  *  состоящую из указанных столбцов.
	  *
	  * primary_expr_ast	- выражение для сортировки;
	  * date_column_name 	- имя столбца с датой;
	  * index_granularity 	- на сколько строчек пишется одно значение индекса.
	  */
	static StoragePtr create(
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
		const MergeTreeSettings & settings_);

	void shutdown() override;
	~StorageMergeTree() override;

	std::string getName() const override
	{
		return data.merging_params.getModeName() + "MergeTree";
	}

	std::string getTableName() const override { return table_name; }
	bool supportsSampling() const override { return data.supportsSampling(); }
	bool supportsFinal() const override { return data.supportsFinal(); }
	bool supportsPrewhere() const override { return data.supportsPrewhere(); }
	bool supportsParallelReplicas() const override { return true; }

	const NamesAndTypesList & getColumnsListImpl() const override { return data.getColumnsListNonMaterialized(); }

	NameAndTypePair getColumn(const String & column_name) const override
	{
		return data.getColumn(column_name);
	}

	bool hasColumn(const String & column_name) const override
	{
		return data.hasColumn(column_name);
	}

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Context & context,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1) override;

	BlockOutputStreamPtr write(ASTPtr query, const Settings & settings) override;

	/** Выполнить очередной шаг объединения кусков.
	  */
	bool optimize(const String & partition, bool final, const Settings & settings) override
	{
		return merge(settings.min_bytes_to_use_direct_io, true, nullptr, partition, final);
	}

	void dropPartition(ASTPtr query, const Field & partition, bool detach, bool unreplicated, const Settings & settings) override;
	void attachPartition(ASTPtr query, const Field & partition, bool unreplicated, bool part, const Settings & settings) override;
	void freezePartition(const Field & partition, const String & with_name, const Settings & settings) override;

	void drop() override;

	void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

	void alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context) override;

	bool supportsIndexForIn() const override { return true; }

	MergeTreeData & getData() { return data; }

private:
	String path;
	String database_name;
	String table_name;
	String full_path;

	Context & context;
	BackgroundProcessingPool & background_pool;

	MergeTreeData data;
	MergeTreeDataSelectExecutor reader;
	MergeTreeDataWriter writer;
	MergeTreeDataMerger merger;

	/// Для нумерации блоков.
	SimpleIncrement increment;

	MergeTreeData::DataParts currently_merging;
	std::mutex currently_merging_mutex;

	Logger * log;

	std::atomic<bool> shutdown_called {false};

	BackgroundProcessingPool::TaskHandle merge_task_handle;

	/// While exists, marks parts as 'currently_merging' and reserves free space on filesystem.
	/// It's possible to mark parts before.
	struct CurrentlyMergingPartsTagger
	{
		MergeTreeData::DataPartsVector parts;
		DiskSpaceMonitor::ReservationPtr reserved_space;
		StorageMergeTree & storage;

		CurrentlyMergingPartsTagger(const MergeTreeData::DataPartsVector & parts_, size_t total_size, StorageMergeTree & storage_)
			: parts(parts_), storage(storage_)
		{
			/// Assume mutex is already locked, because this method is called from mergeTask.
			reserved_space = DiskSpaceMonitor::reserve(storage.full_path, total_size); /// May throw.
			for (const auto & part : parts)
			{
				if (storage.currently_merging.count(part))
					throw Exception("Tagging alreagy tagged part " + part->name + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);
			}
			storage.currently_merging.insert(parts.begin(), parts.end());
		}

		~CurrentlyMergingPartsTagger()
		{
			try
			{
				std::lock_guard<std::mutex> lock(storage.currently_merging_mutex);
				for (const auto & part : parts)
				{
					if (!storage.currently_merging.count(part))
						throw Exception("Untagging already untagged part " + part->name + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);
					storage.currently_merging.erase(part);
				}
			}
			catch (...)
			{
				tryLogCurrentException("~CurrentlyMergingPartsTagger");
			}
		}
	};

	using CurrentlyMergingPartsTaggerPtr = std::shared_ptr<CurrentlyMergingPartsTagger>;

	StorageMergeTree(
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
		const ASTPtr & sampling_expression_, /// nullptr, if sampling is not supported.
		size_t index_granularity_,
		const MergeTreeData::MergingParams & merging_params_,
		bool has_force_restore_data_flag,
		const MergeTreeSettings & settings_);

	/** Определяет, какие куски нужно объединять, и объединяет их.
	  * Если aggressive - выбрать куски, не обращая внимание на соотношение размеров и их новизну (для запроса OPTIMIZE).
	  * Возвращает, получилось ли что-нибудь объединить.
	  */
	bool merge(size_t aio_threshold, bool aggressive, BackgroundProcessingPool::Context * context, const String & partition, bool final);

	bool mergeTask(BackgroundProcessingPool::Context & context);
};

}
