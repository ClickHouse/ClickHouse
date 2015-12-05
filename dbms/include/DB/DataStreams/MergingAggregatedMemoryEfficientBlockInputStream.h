#pragma once

#include <common/threadpool.hpp>
#include <DB/Interpreters/Aggregator.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Common/ConcurrentBoundedQueue.h>


namespace DB
{


/** Доагрегирует потоки блоков, держа в оперативной памяти только по одному или несколько (до merging_threads) блоков из каждого источника.
  * Это экономит оперативку в случае использования двухуровневой агрегации, где в каждом потоке будет до 256 блоков с частями результата.
  *
  * Агрегатные функции в блоках не должны быть финализированы, чтобы их состояния можно было объединить.
  */
class MergingAggregatedMemoryEfficientBlockInputStream : public IProfilingBlockInputStream
{
public:
	MergingAggregatedMemoryEfficientBlockInputStream(
		BlockInputStreams inputs_, const Aggregator::Params & params, bool final_,
		size_t reading_threads_, size_t merging_threads_);

	~MergingAggregatedMemoryEfficientBlockInputStream();

	String getName() const override { return "MergingAggregatedMemoryEfficient"; }

	String getID() const override;

	/// Отправляет запрос (инициирует вычисления) раньше, чем read.
	void readPrefix() override;

protected:
	Block readImpl() override;

private:
	Aggregator aggregator;
	bool final;
	size_t reading_threads;
	size_t merging_threads;

	bool started = false;
	volatile bool has_two_level = false;
	volatile bool has_overflows = false;
	int current_bucket_num = -1;

	struct Input
	{
		BlockInputStreamPtr stream;
		Block block;
		Block overflow_block;
		std::vector<Block> splitted_blocks;
		bool is_exhausted = false;

		Input(BlockInputStreamPtr & stream_) : stream(stream_) {}
	};

	std::vector<Input> inputs;

	using BlocksToMerge = Poco::SharedPtr<BlocksList>;

	void start();

	/// Получить блоки, которые можно мерджить. Это позволяет мерджить их параллельно в отдельных потоках.
	BlocksToMerge getNextBlocksToMerge();

	std::unique_ptr<boost::threadpool::pool> reading_pool;

	/// Для параллельного мерджа.
	struct OutputData
	{
		Block block;
		std::exception_ptr exception;

		OutputData() {}
		OutputData(Block && block_) : block(std::move(block_)) {}
		OutputData(std::exception_ptr && exception_) : exception(std::move(exception_)) {}
	};

	struct ParallelMergeData
	{
		boost::threadpool::pool pool;
		std::mutex get_next_blocks_mutex;
		ConcurrentBoundedQueue<OutputData> result_queue;
		bool exhausted = false;
		std::atomic<size_t> active_threads;

		ParallelMergeData(size_t max_threads) : pool(max_threads), result_queue(max_threads), active_threads(max_threads) {}
	};

	std::unique_ptr<ParallelMergeData> parallel_merge_data;

	void mergeThread(MemoryTracker * memory_tracker);
};

}
