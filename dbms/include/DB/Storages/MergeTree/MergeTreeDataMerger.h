#pragma once

#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Storages/MergeTree/DiskSpaceMonitor.h>
#include <atomic>
#include <functional>

namespace DB
{

class MergeListEntry;
class MergeProgressCallback;
struct ReshardingJob;


/** Умеет выбирать куски для слияния и сливать их.
  */
class MergeTreeDataMerger
{
public:
	using CancellationHook = std::function<void()>;
	using AllowedMergingPredicate = std::function<bool (const MergeTreeData::DataPartPtr &, const MergeTreeData::DataPartPtr &)>;

public:
	MergeTreeDataMerger(MergeTreeData & data_, const BackgroundProcessingPool & pool_);

	void setCancellationHook(CancellationHook cancellation_hook_);

	/** Get maximum total size of parts to do merge, at current moment of time.
	  * It depends on number of free threads in background_pool and amount of free space in disk.
	  */
	size_t getMaxPartsSizeForMerge();

	/** For explicitly passed size of pool and number of used tasks.
	  * This method could be used to calculate threshold depending on number of tasks in replication queue.
	  */
	size_t getMaxPartsSizeForMerge(size_t pool_size, size_t pool_used);

	/** Выбирает, какие куски слить. Использует кучу эвристик.
	  *
	  * can_merge - функция, определяющая, можно ли объединить пару соседних кусков.
	  *  Эта функция должна координировать слияния со вставками и другими слияниями, обеспечивая, что:
	  *  - Куски, между которыми еще может появиться новый кусок, нельзя сливать. См. METR-7001.
	  *  - Кусок, который уже сливается с кем-то в одном месте, нельзя начать сливать в кем-то другим в другом месте.
	  */
	bool selectPartsToMerge(
		MergeTreeData::DataPartsVector & what,
		String & merged_name,
		bool aggressive,
		size_t max_total_size_to_merge,
		const AllowedMergingPredicate & can_merge);

	/** Выбрать для слияния все куски в заданной партиции, если возможно.
	  * final - выбирать для слияния даже единственный кусок - то есть, позволять мерджить один кусок "сам с собой".
	  */
	bool selectAllPartsToMergeWithinPartition(
		MergeTreeData::DataPartsVector & what,
		String & merged_name,
		size_t available_disk_space,
		const AllowedMergingPredicate & can_merge,
		DayNum_t partition,
		bool final);

	/** Сливает куски.
	  * Если reservation != nullptr, то и дело уменьшает размер зарезервированного места
	  *  приблизительно пропорционально количеству уже выписанных данных.
	  *
	  * Создаёт и возвращает временный кусок.
	  * Чтобы закончить мердж, вызовите функцию renameTemporaryMergedPart.
	  *
	  * time_of_merge - время, когда мердж был назначен.
	  * Важно при использовании ReplicatedGraphiteMergeTree для обеспечения одинакового мерджа на репликах.
	  */
	MergeTreeData::MutableDataPartPtr mergePartsToTemporaryPart(
		MergeTreeData::DataPartsVector & parts, const String & merged_name, MergeListEntry & merge_entry,
		size_t aio_threshold, time_t time_of_merge, DiskSpaceMonitor::Reservation * disk_reservation = nullptr);

	MergeTreeData::DataPartPtr renameMergedTemporaryPart(
		MergeTreeData::DataPartsVector & parts,
		MergeTreeData::MutableDataPartPtr & new_data_part,
		const String & merged_name,
		MergeTreeData::Transaction * out_transaction = nullptr);

	/** Перешардирует заданную партицию.
	  */
	MergeTreeData::PerShardDataParts reshardPartition(
		const ReshardingJob & job,
		DiskSpaceMonitor::Reservation * disk_reservation = nullptr);

	/// Примерное количество места на диске, нужное для мерджа. С запасом.
	static size_t estimateDiskSpaceForMerge(const MergeTreeData::DataPartsVector & parts);

private:
	/** Выбрать все куски принадлежащие одной партиции.
	  */
	MergeTreeData::DataPartsVector selectAllPartsFromPartition(DayNum_t partition);

	/** Temporarily cancel merges.
	  */
	class BlockerImpl
	{
	public:
		BlockerImpl(MergeTreeDataMerger * merger_) : merger(merger_)
		{
			++merger->cancelled;
		}

		~BlockerImpl()
		{
			--merger->cancelled;
		}
	private:
		MergeTreeDataMerger * merger;
	};

public:
	/** Cancel all merges. All currently running 'mergeParts' methods will throw exception soon.
	  * All new calls to 'mergeParts' will throw exception till all 'Blocker' objects will be destroyed.
	  */
	using Blocker = std::unique_ptr<BlockerImpl>;
	Blocker cancel() { return std::make_unique<BlockerImpl>(this); }

	/** Cancel all merges forever.
	  */
	void cancelForever() { ++cancelled; }

	bool isCancelled() const { return cancelled > 0; }

public:

	enum class MergeAlgorithm
	{
		Horizontal,	/// per-row merge of all columns
		Vertical	/// per-row merge of PK columns, per-column gather for non-PK columns
	};

private:

	MergeAlgorithm chooseMergeAlgorithm(const MergeTreeData & data, const MergeTreeData::DataPartsVector & parts,
		size_t rows_upper_bound, const NamesAndTypesList & gathering_columns, MergedRowSources & rows_sources_to_alloc) const;

private:
	MergeTreeData & data;
	const BackgroundProcessingPool & pool;

	Logger * log;

	/// Когда в последний раз писали в лог, что место на диске кончилось (чтобы не писать об этом слишком часто).
	time_t disk_space_warning_time = 0;

	CancellationHook cancellation_hook;

	std::atomic<int> cancelled {0};

	void abortReshardPartitionIfRequested();
};


}
