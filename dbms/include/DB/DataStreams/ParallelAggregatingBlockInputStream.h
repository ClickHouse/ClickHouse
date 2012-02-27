#pragma once

#include <statdaemons/threadpool.hpp>

#include <DB/Interpreters/Aggregator.h>
#include <DB/Interpreters/Expression.h>
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
	ParallelAggregatingBlockInputStream(BlockInputStreams inputs_, const ColumnNumbers & keys_, AggregateDescriptions & aggregates_, unsigned max_threads_ = 1)
		: inputs(inputs_), aggregator(new Aggregator(keys_, aggregates_)), has_been_read(false), max_threads(max_threads_), pool(max_threads)
	{
		children.insert(children.end(), inputs_.begin(), inputs_.end());
	}

	/** keys берутся из GROUP BY части запроса
	  * Агрегатные функции ищутся везде в выражении.
	  * Столбцы, соответствующие keys и аргументам агрегатных функций, уже должны быть вычислены.
	  */
	ParallelAggregatingBlockInputStream(BlockInputStreams inputs_, SharedPtr<Expression> expression, unsigned max_threads_ = 1)
		: inputs(inputs_), has_been_read(false), max_threads(max_threads_), pool(max_threads)
	{
		children.insert(children.end(), inputs_.begin(), inputs_.end());
		
		Names key_names;
		AggregateDescriptions aggregates;
		expression->getAggregateInfo(key_names, aggregates);
		aggregator = new Aggregator(key_names, aggregates);
	}

	Block readImpl()
	{
		if (has_been_read)
			return Block();

		has_been_read = true;

		ManyAggregatedDataVariants many_data(inputs.size());
		Exceptions exceptions(inputs.size());
		
		for (size_t i = 0, size = many_data.size(); i < size; ++i)
		{
			many_data[i] = new AggregatedDataVariants;
			pool.schedule(boost::bind(&ParallelAggregatingBlockInputStream::calculate, this, boost::ref(inputs[i]), boost::ref(*many_data[i]), boost::ref(exceptions[i])));
		}
		pool.wait();
		
		AggregatedDataVariantsPtr res = aggregator->merge(many_data);
		return aggregator->convertToBlock(*res);
	}

	String getName() const { return "ParallelAggregatingBlockInputStream"; }

	BlockInputStreamPtr clone() { return new ParallelAggregatingBlockInputStream(*this); }

private:
	ParallelAggregatingBlockInputStream(const ParallelAggregatingBlockInputStream & src)
		: inputs(src.inputs), aggregator(src.aggregator), has_been_read(src.has_been_read) {}
	
	BlockInputStreams inputs;
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
			exception = new Exception(e);
		}
		catch (const Poco::Exception & e)
		{
			exception = new Exception(e.message(), ErrorCodes::POCO_EXCEPTION);
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
