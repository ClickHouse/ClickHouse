#pragma once

#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <DB/Storages/MergeTree/MergeTreeDataWriter.h>
#include <DB/Storages/MergeTree/MergeTreeDataMerger.h>
#include <DB/Storages/MergeTree/DiskSpaceMonitor.h>
#include <DB/Storages/MergeTree/BackgroundProcessingPool.h>
#include <statdaemons/Increment.h>


namespace DB
{

/** См. описание структуры данных в MergeTreeData.
  */
class StorageMergeTree : public IStorage
{
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
		MergeTreeData::Mode mode_,
		const String & sign_column_,			/// Для Collapsing режима.
		const Names & columns_to_sum_,			/// Для Summing режима.
		const MergeTreeSettings & settings_);

	void shutdown() override;
	~StorageMergeTree() override;

	std::string getName() const override
	{
		return data.getModePrefix() + "MergeTree";
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

	BlockOutputStreamPtr write(ASTPtr query) override;

	/** Выполнить очередной шаг объединения кусков.
	  */
	bool optimize(const Settings & settings) override
	{
		return merge(settings.min_bytes_to_use_direct_io, true);
	}

	void dropPartition(const Field & partition, bool detach, bool unreplicated, const Settings & settings) override;
	void attachPartition(const Field & partition, bool unreplicated, bool part, const Settings & settings) override;
	void freezePartition(const Field & partition, const Settings & settings) override;

	void drop() override;

	void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

	void alter(const AlterCommands & params, const String & database_name, const String & table_name, Context & context) override;

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
	Poco::FastMutex currently_merging_mutex;

	Logger * log;

	volatile bool shutdown_called;

	BackgroundProcessingPool::TaskHandle merge_task_handle;

	/// Пока существует, помечает части как currently_merging и держит резерв места.
	/// Вероятно, что части будут помечены заранее.
	struct CurrentlyMergingPartsTagger
	{
		MergeTreeData::DataPartsVector parts;
		DiskSpaceMonitor::ReservationPtr reserved_space;
		StorageMergeTree & storage;

		CurrentlyMergingPartsTagger(const MergeTreeData::DataPartsVector & parts_, size_t total_size, StorageMergeTree & storage_)
			: parts(parts_), storage(storage_)
		{
			/// Здесь не лочится мьютекс, так как конструктор вызывается внутри mergeTask, где он уже залочен.
			reserved_space = DiskSpaceMonitor::reserve(storage.full_path, total_size); /// Может бросить исключение.
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
				Poco::ScopedLock<Poco::FastMutex> lock(storage.currently_merging_mutex);
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

	typedef Poco::SharedPtr<CurrentlyMergingPartsTagger> CurrentlyMergingPartsTaggerPtr;

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
		const ASTPtr & sampling_expression_, /// nullptr, если семплирование не поддерживается.
		size_t index_granularity_,
		MergeTreeData::Mode mode_,
		const String & sign_column_,
		const Names & columns_to_sum_,
		const MergeTreeSettings & settings_);

	/** Определяет, какие куски нужно объединять, и объединяет их.
	  * Если aggressive - выбрать куски, не обращая внимание на соотношение размеров и их новизну (для запроса OPTIMIZE).
	  * Возвращает, получилось ли что-нибудь объединить.
	  */
	bool merge(size_t aio_threshold, bool aggressive = false, BackgroundProcessingPool::Context * context = nullptr);

	bool mergeTask(BackgroundProcessingPool::Context & context);

	/// Вызывается во время выбора кусков для слияния.
	bool canMergeParts(const MergeTreeData::DataPartPtr & left, const MergeTreeData::DataPartPtr & right);
};

}
