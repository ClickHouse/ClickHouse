#pragma once

#include <statdaemons/threadpool.hpp>

#include <DB/Interpreters/Aggregator.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Агрегирует несколько источников параллельно.
  * Запускает агрегацию отдельных источников в отдельных потоках, затем объединяет результаты.
  * Агрегатные функции не финализируются, то есть, не заменяются на своё значение, а содержат промежуточное состояние вычислений.
  * Это необходимо, чтобы можно было продолжить агрегацию (например, объединяя потоки частично агрегированных данных).
  */
class ParallelAggregatingBlockInputStream : public IProfilingBlockInputStream
{
public:
	ParallelAggregatingBlockInputStream(BlockInputStreams inputs_, const ColumnNumbers & keys_, AggregateDescriptions & aggregates_,
		bool with_totals_, unsigned max_threads_ = 1, size_t max_rows_to_group_by_ = 0, Limits::OverflowMode group_by_overflow_mode_ = Limits::THROW)
		: aggregator(new Aggregator(keys_, aggregates_, with_totals_, max_rows_to_group_by_, group_by_overflow_mode_)),
		has_been_read(false), max_threads(max_threads_), pool(max_threads)
	{
		children.insert(children.end(), inputs_.begin(), inputs_.end());
	}

	/** keys берутся из GROUP BY части запроса
	  * Агрегатные функции ищутся везде в выражении.
	  * Столбцы, соответствующие keys и аргументам агрегатных функций, уже должны быть вычислены.
	  */
	ParallelAggregatingBlockInputStream(BlockInputStreams inputs_, const Names & key_names, const AggregateDescriptions & aggregates,
		bool with_totals_, unsigned max_threads_ = 1, size_t max_rows_to_group_by_ = 0, Limits::OverflowMode group_by_overflow_mode_ = Limits::THROW)
		: has_been_read(false), max_threads(max_threads_), pool(max_threads)
	{
		children.insert(children.end(), inputs_.begin(), inputs_.end());
		
		aggregator = new Aggregator(key_names, aggregates, with_totals_, max_rows_to_group_by_, group_by_overflow_mode_);
	}

	String getName() const { return "ParallelAggregatingBlockInputStream"; }

	String getID() const
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
	Block readImpl()
	{
		if (has_been_read)
			return Block();

		has_been_read = true;

		ManyAggregatedDataVariants many_data(children.size());
		Exceptions exceptions(children.size());

		for (size_t i = 0, size = many_data.size(); i < size; ++i)
		{
			many_data[i] = new AggregatedDataVariants;
			pool.schedule(boost::bind(&ParallelAggregatingBlockInputStream::calculate, this, boost::ref(children[i]), boost::ref(*many_data[i]), boost::ref(exceptions[i])));
		}
		pool.wait();

		for (size_t i = 0, size = exceptions.size(); i < size; ++i)
			if (exceptions[i])
				exceptions[i]->rethrow();

		if (isCancelled())
			return Block();

		AggregatedDataVariantsPtr res = aggregator->merge(many_data);
		return aggregator->convertToBlock(*res);
	}

private:
	SharedPtr<Aggregator> aggregator;
	bool has_been_read;
	size_t max_threads;
	boost::threadpool::pool pool;

	/// Вычисления, которые выполняться в отдельном потоке
	void calculate(BlockInputStreamPtr & input, AggregatedDataVariants & data, ExceptionPtr & exception)
	{
		try
		{
			aggregator->execute(input, data);
		}
		catch (const Exception & e)
		{
			exception = e.clone();
		}
		catch (const Poco::Exception & e)
		{
			exception = e.clone();
		}
		catch (const std::exception & e)
		{
			exception = new Exception(e.what(), ErrorCodes::STD_EXCEPTION);
		}
		catch (...)
		{
			exception = new Exception("Unknown exception", ErrorCodes::UNKNOWN_EXCEPTION);
		}
	}
};

}
