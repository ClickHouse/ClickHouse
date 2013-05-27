#pragma once

#include <DB/Interpreters/Aggregator.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Агрегирует поток блоков, используя заданные столбцы-ключи и агрегатные функции.
  * Столбцы с агрегатными функциями добавляет в конец блока.
  * Агрегатные функции не финализируются, то есть, не заменяются на своё значение, а содержат промежуточное состояние вычислений.
  * Это необходимо, чтобы можно было продолжить агрегацию (например, объединяя потоки частично агрегированных данных).
  */
class AggregatingBlockInputStream : public IProfilingBlockInputStream
{
public:
	AggregatingBlockInputStream(BlockInputStreamPtr input_, const ColumnNumbers & keys_, AggregateDescriptions & aggregates_,
		bool with_totals_, size_t max_rows_to_group_by_, Limits::OverflowMode group_by_overflow_mode_)
		: aggregator(new Aggregator(keys_, aggregates_, with_totals_, max_rows_to_group_by_, group_by_overflow_mode_)), has_been_read(false)
	{
		children.push_back(input_);
	}

	/** keys берутся из GROUP BY части запроса
	  * Агрегатные функции ищутся везде в выражении.
	  * Столбцы, соответствующие keys и аргументам агрегатных функций, уже должны быть вычислены.
	  */
	AggregatingBlockInputStream(BlockInputStreamPtr input_, const Names & key_names, const AggregateDescriptions & aggregates,
		bool with_totals_, size_t max_rows_to_group_by_, Limits::OverflowMode group_by_overflow_mode_);

	String getName() const { return "AggregatingBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "Aggregating(" << children.back()->getID() << ", " << aggregator->getID() << ")";
		return res.str();
	}

protected:
	Block readImpl();

	SharedPtr<Aggregator> aggregator;
	bool has_been_read;
};

}
