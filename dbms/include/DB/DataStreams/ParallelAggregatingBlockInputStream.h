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
	ParallelAggregatingBlockInputStream(BlockInputStreams inputs, const ColumnNumbers & keys_,
		AggregateDescriptions & aggregates_, bool overflow_row_, bool final_, size_t max_threads_,
		size_t max_rows_to_group_by_ = 0, OverflowMode group_by_overflow_mode_ = OverflowMode::THROW)
		: aggregator(new Aggregator(keys_, aggregates_, overflow_row_, max_rows_to_group_by_, group_by_overflow_mode_)),
		has_been_read(false), final(final_), max_threads(std::min(inputs.size(), max_threads_)),
		keys_size(keys_.size()), aggregates_size(aggregates_.size()),
		handler(*this), processor(inputs, max_threads, handler),
		key(max_threads), key_columns(max_threads), aggregate_columns(max_threads), key_sizes(max_threads)
	{
		children.insert(children.end(), inputs.begin(), inputs.end());
	}

	/** Столбцы из key_names и аргументы агрегатных функций, уже должны быть вычислены.
	  */
	ParallelAggregatingBlockInputStream(BlockInputStreams inputs, const Names & key_names,
		const AggregateDescriptions & aggregates,	bool overflow_row_, bool final_, size_t max_threads_,
		size_t max_rows_to_group_by_ = 0, OverflowMode group_by_overflow_mode_ = OverflowMode::THROW)
		: has_been_read(false), final(final_), max_threads(std::min(inputs.size(), max_threads_)),
		keys_size(key_names.size()), aggregates_size(aggregates.size()),
		handler(*this), processor(inputs, max_threads, handler),
		key(max_threads), key_columns(max_threads), aggregate_columns(max_threads), key_sizes(max_threads)
	{
		children.insert(children.end(), inputs.begin(), inputs.end());

		aggregator = new Aggregator(key_names, aggregates, overflow_row_, max_rows_to_group_by_, group_by_overflow_mode_);
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

		res << ", " << aggregator->getID() << ")";
		return res.str();
	}

	void cancel() override
	{
		if (!__sync_bool_compare_and_swap(&is_cancelled, false, true))
			return;

		processor.cancel();
	}

protected:
	Block readImpl() override
	{
		if (has_been_read)
			return Block();

		has_been_read = true;

		many_data.resize(max_threads);
		exceptions.resize(max_threads);
		src_rows.resize(max_threads);
		src_bytes.resize(max_threads);

		LOG_TRACE(log, "Aggregating");

		Stopwatch watch;

		for (auto & elem : many_data)
			elem = new AggregatedDataVariants;

		for (size_t i = 0; i < max_threads; ++i)
		{
			key[i].resize(keys_size);
			key_columns[i].resize(keys_size);
			aggregate_columns[i].resize(aggregates_size);
			key_sizes[i].resize(keys_size);
		}

		processor.process();
		processor.wait();

		rethrowFirstException(exceptions);

		if (isCancelled())
			return Block();

		double elapsed_seconds = watch.elapsedSeconds();

		size_t total_src_rows = 0;
		size_t total_src_bytes = 0;
		for (size_t i = 0; i < max_threads; ++i)
		{
			size_t rows = many_data[i]->size();
			LOG_TRACE(log, std::fixed << std::setprecision(3)
				<< "Aggregated. " << src_rows[i] << " to " << rows << " rows (from " << src_bytes[i] / 1048576.0 << " MiB)"
				<< " in " << elapsed_seconds << " sec."
				<< " (" << src_rows[i] / elapsed_seconds << " rows/sec., " << src_bytes[i] / elapsed_seconds / 1048576.0 << " MiB/sec.)");

			total_src_rows += src_rows[i];
			total_src_bytes += src_bytes[i];
		}
		LOG_TRACE(log, std::fixed << std::setprecision(3)
			<< "Total aggregated. " << total_src_rows << " rows (from " << total_src_bytes / 1048576.0 << " MiB)"
			<< " in " << elapsed_seconds << " sec."
			<< " (" << total_src_rows / elapsed_seconds << " rows/sec., " << total_src_bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");

		AggregatedDataVariantsPtr res = aggregator->merge(many_data);

		if (isCancelled())
			return Block();

		return aggregator->convertToBlock(*res, final);
	}

private:
	SharedPtr<Aggregator> aggregator;
	bool has_been_read;
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

	Logger * log = &Logger::get("ParallelAggregatingBlockInputStream");


	struct Handler
	{
		Handler(ParallelAggregatingBlockInputStream & parent_)
			: parent(parent_) {}

		void onBlock(Block & block, size_t thread_num)
		{
			parent.aggregator->executeOnBlock(block, *parent.many_data[thread_num],
				parent.key_columns[thread_num], parent.aggregate_columns[thread_num],
				parent.key_sizes[thread_num], parent.key[thread_num], parent.no_more_keys);

			parent.src_rows[thread_num] += block.rowsInFirstColumn();
			parent.src_bytes[thread_num] += block.bytes();
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

	ManyAggregatedDataVariants many_data;
	Exceptions exceptions;

	std::vector<size_t> src_rows;
	std::vector<size_t> src_bytes;

	std::vector<StringRefs> key;
	std::vector<ConstColumnPlainPtrs> key_columns;
	std::vector<Aggregator::AggregateColumns> aggregate_columns;
	std::vector<Sizes> key_sizes;
};

}
