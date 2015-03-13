#pragma once

#include <DB/Interpreters/Aggregator.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Агрегирует поток блоков, используя заданные столбцы-ключи и агрегатные функции.
  * Столбцы с агрегатными функциями добавляет в конец блока.
  * Если final=false, агрегатные функции не финализируются, то есть, не заменяются на своё значение, а содержат промежуточное состояние вычислений.
  * Это необходимо, чтобы можно было продолжить агрегацию (например, объединяя потоки частично агрегированных данных).
  */
class AggregatingBlockInputStream : public IProfilingBlockInputStream
{
public:
	/** keys берутся из GROUP BY части запроса
	  * Агрегатные функции ищутся везде в выражении.
	  * Столбцы, соответствующие keys и аргументам агрегатных функций, уже должны быть вычислены.
	  */
	AggregatingBlockInputStream(BlockInputStreamPtr input_, const Names & key_names, const AggregateDescriptions & aggregates,
		bool overflow_row_, bool final_, size_t max_rows_to_group_by_, OverflowMode group_by_overflow_mode_,
		Compiler * compiler_, UInt32 min_count_to_compile_, size_t group_by_two_level_threshold_)
		: aggregator(key_names, aggregates, overflow_row_, max_rows_to_group_by_, group_by_overflow_mode_,
			compiler_, min_count_to_compile_, group_by_two_level_threshold_),
		final(final_)
	{
		children.push_back(input_);
	}

	String getName() const override { return "AggregatingBlockInputStream"; }

	String getID() const override
	{
		std::stringstream res;
		res << "Aggregating(" << children.back()->getID() << ", " << aggregator.getID() << ")";
		return res.str();
	}

protected:
	Block readImpl() override;

	Aggregator aggregator;
	bool final;

	bool executed = false;
	BlocksList blocks;
	BlocksList::iterator it;
};

}
