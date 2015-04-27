#pragma once

#include <DB/Interpreters/Aggregator.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/ParallelInputsProcessor.h>


namespace DB
{

using Poco::SharedPtr;


/** Агрегирует несколько источников параллельно.
  * Производит агрегацию блоков из разных источников независимо в разных потоках, затем объединяет результаты.
  * Если final == false, агрегатные функции не финализируются, то есть, не заменяются на своё значение, а содержат промежуточное состояние вычислений.
  * Это необходимо, чтобы можно было продолжить агрегацию (например, объединяя потоки частично агрегированных данных).
  */
class ParallelAggregatingBlockInputStream : public IProfilingBlockInputStream
{
public:
	/** Столбцы из key_names и аргументы агрегатных функций, уже должны быть вычислены.
	  */
	ParallelAggregatingBlockInputStream(BlockInputStreams inputs, const Names & key_names,
		const AggregateDescriptions & aggregates,	bool overflow_row_, bool final_, size_t max_threads_,
		size_t max_rows_to_group_by_, OverflowMode group_by_overflow_mode_,
		Compiler * compiler_, UInt32 min_count_to_compile_, size_t group_by_two_level_threshold_)
		: aggregator(key_names, aggregates, overflow_row_, max_rows_to_group_by_, group_by_overflow_mode_,
			compiler_, min_count_to_compile_, group_by_two_level_threshold_),
		final(final_), max_threads(std::min(inputs.size(), max_threads_)),
		keys_size(aggregator.getNumberOfKeys()), aggregates_size(aggregator.getNumberOfAggregates()),
		handler(*this), processor(inputs, max_threads, handler)
	{
		children.insert(children.end(), inputs.begin(), inputs.end());
	}

	String getName() const override { return "ParallelAggregatingBlockInputStream"; }

	String getID() const override
	{
		std::stringstream res;
		res << "ParallelAggregating(";

		Strings children_ids(children.size());
		for (size_t i = 0; i < children.size(); ++i)
			children_ids[i] = children[i]->getID();

		/// Порядок не имеет значения.
		std::sort(children_ids.begin(), children_ids.end());

		for (size_t i = 0; i < children_ids.size(); ++i)
			res << (i == 0 ? "" : ", ") << children_ids[i];

		res << ", " << aggregator.getID() << ")";
		return res.str();
	}

	void cancel() override
	{
		bool old_val = false;
		if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
			return;

		processor.cancel();
	}

protected:
	Block readImpl() override
	{
		if (!executed)
		{
			executed = true;

			Aggregator::CancellationHook hook = [&]() { return this->isCancelled(); };
			aggregator.setCancellationHook(hook);

			AggregatedDataVariantsPtr data_variants = executeAndMerge();

			if (data_variants)
				blocks = aggregator.convertToBlocks(*data_variants, final, max_threads);

			it = blocks.begin();
		}

		Block res;
		if (isCancelled() || it == blocks.end())
			return res;

		res = *it;
		++it;

		return res;
	}

private:
	Aggregator aggregator;
	bool final;
	size_t max_threads;

	size_t keys_size;
	size_t aggregates_size;

	/** Используется, если есть ограничение на максимальное количество строк при агрегации,
	  *  и если group_by_overflow_mode == ANY.
	  * В этом случае, новые ключи не добавляются в набор, а производится агрегация только по
	  *  ключам, которые уже успели попасть в набор.
	  */
	bool no_more_keys = false;

	bool executed = false;
	BlocksList blocks;
	BlocksList::iterator it;

	Logger * log = &Logger::get("ParallelAggregatingBlockInputStream");


	ManyAggregatedDataVariants many_data;
	Exceptions exceptions;

	struct ThreadData
	{
		size_t src_rows = 0;
		size_t src_bytes = 0;

		StringRefs key;
		ConstColumnPlainPtrs key_columns;
		Aggregator::AggregateColumns aggregate_columns;
		Sizes key_sizes;

		ThreadData(size_t keys_size, size_t aggregates_size)
		{
			key.resize(keys_size);
			key_columns.resize(keys_size);
			aggregate_columns.resize(aggregates_size);
			key_sizes.resize(keys_size);
		}
	};

	std::vector<ThreadData> threads_data;


	struct Handler
	{
		Handler(ParallelAggregatingBlockInputStream & parent_)
			: parent(parent_) {}

		void onBlock(Block & block, size_t thread_num)
		{
			parent.aggregator.executeOnBlock(block, *parent.many_data[thread_num],
				parent.threads_data[thread_num].key_columns, parent.threads_data[thread_num].aggregate_columns,
				parent.threads_data[thread_num].key_sizes, parent.threads_data[thread_num].key,
				parent.no_more_keys);

			parent.threads_data[thread_num].src_rows += block.rowsInFirstColumn();
			parent.threads_data[thread_num].src_bytes += block.bytes();
		}

		void onFinish()
		{
		}

		void onException(ExceptionPtr & exception, size_t thread_num)
		{
			parent.exceptions[thread_num] = exception;
			parent.cancel();
		}

		ParallelAggregatingBlockInputStream & parent;
	};

	Handler handler;
	ParallelInputsProcessor<Handler> processor;


	AggregatedDataVariantsPtr executeAndMerge()
	{
		many_data.resize(max_threads);
		exceptions.resize(max_threads);

		for (size_t i = 0; i < max_threads; ++i)
			threads_data.emplace_back(keys_size, aggregates_size);

		LOG_TRACE(log, "Aggregating");

		Stopwatch watch;

		for (auto & elem : many_data)
			elem = new AggregatedDataVariants;

		processor.process();
		processor.wait();

		rethrowFirstException(exceptions);

		if (isCancelled())
			return nullptr;

		double elapsed_seconds = watch.elapsedSeconds();

		size_t total_src_rows = 0;
		size_t total_src_bytes = 0;
		for (size_t i = 0; i < max_threads; ++i)
		{
			size_t rows = many_data[i]->size();
			LOG_TRACE(log, std::fixed << std::setprecision(3)
				<< "Aggregated. " << threads_data[i].src_rows << " to " << rows << " rows"
					<< " (from " << threads_data[i].src_bytes / 1048576.0 << " MiB)"
				<< " in " << elapsed_seconds << " sec."
				<< " (" << threads_data[i].src_rows / elapsed_seconds << " rows/sec., "
					<< threads_data[i].src_bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");

			total_src_rows += threads_data[i].src_rows;
			total_src_bytes += threads_data[i].src_bytes;
		}
		LOG_TRACE(log, std::fixed << std::setprecision(3)
			<< "Total aggregated. " << total_src_rows << " rows (from " << total_src_bytes / 1048576.0 << " MiB)"
			<< " in " << elapsed_seconds << " sec."
			<< " (" << total_src_rows / elapsed_seconds << " rows/sec., " << total_src_bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");

		if (isCancelled())
			return nullptr;

		return aggregator.merge(many_data, max_threads);
	}
};

}
