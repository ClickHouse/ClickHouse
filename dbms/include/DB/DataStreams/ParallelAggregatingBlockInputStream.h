#pragma once

#include <statdaemons/threadpool.hpp>

#include <DB/Interpreters/Aggregator.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Агрегирует несколько источников параллельно.
  * Запускает агрегацию отдельных источников в отдельных потоках, затем объединяет результаты.
  * Если final == false, агрегатные функции не финализируются, то есть, не заменяются на своё значение, а содержат промежуточное состояние вычислений.
  * Это необходимо, чтобы можно было продолжить агрегацию (например, объединяя потоки частично агрегированных данных).
  */
class ParallelAggregatingBlockInputStream : public IProfilingBlockInputStream
{
public:
	ParallelAggregatingBlockInputStream(BlockInputStreams inputs_, const ColumnNumbers & keys_,
		AggregateDescriptions & aggregates_, bool overflow_row_, bool final_, unsigned max_threads_ = 1,
		size_t max_rows_to_group_by_ = 0, OverflowMode group_by_overflow_mode_ = OverflowMode::THROW)
		: aggregator(new Aggregator(keys_, aggregates_, overflow_row_, max_rows_to_group_by_, group_by_overflow_mode_)),
		has_been_read(false), final(final_), max_threads(max_threads_), pool(std::min(max_threads, inputs_.size()))
	{
		children.insert(children.end(), inputs_.begin(), inputs_.end());
	}

	/** Столбцы из key_names и аргументы агрегатных функций, уже должны быть вычислены.
	  */
	ParallelAggregatingBlockInputStream(BlockInputStreams inputs_, const Names & key_names,
		const AggregateDescriptions & aggregates,	bool overflow_row_, bool final_, unsigned max_threads_ = 1,
		size_t max_rows_to_group_by_ = 0, OverflowMode group_by_overflow_mode_ = OverflowMode::THROW)
		: has_been_read(false), final(final_), max_threads(max_threads_), pool(std::min(max_threads, inputs_.size()))
	{
		children.insert(children.end(), inputs_.begin(), inputs_.end());

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

protected:
	Block readImpl() override
	{
		if (has_been_read)
			return Block();

		has_been_read = true;

		ManyAggregatedDataVariants many_data(children.size());
		Exceptions exceptions(children.size());

		for (size_t i = 0, size = many_data.size(); i < size; ++i)
		{
			many_data[i] = new AggregatedDataVariants;
			pool.schedule(std::bind(&ParallelAggregatingBlockInputStream::calculate, this,
				std::ref(children[i]), std::ref(*many_data[i]), std::ref(exceptions[i]), current_memory_tracker));
		}
		pool.wait();

		rethrowFirstException(exceptions);

		if (isCancelled())
			return Block();

		AggregatedDataVariantsPtr res = aggregator->merge(many_data);
		return aggregator->convertToBlock(*res, final);
	}

private:
	SharedPtr<Aggregator> aggregator;
	bool has_been_read;
	bool final;
	size_t max_threads;
	boost::threadpool::pool pool;

	/// Вычисления, которые выполняются в отдельном потоке
	void calculate(BlockInputStreamPtr & input, AggregatedDataVariants & data, ExceptionPtr & exception, MemoryTracker * memory_tracker)
	{
		current_memory_tracker = memory_tracker;

		try
		{
			aggregator->execute(input, data);
		}
		catch (...)
		{
			exception = cloneCurrentException();
		}
	}
};

}
