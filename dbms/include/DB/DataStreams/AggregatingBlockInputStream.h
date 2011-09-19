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
	AggregatingBlockInputStream(BlockInputStreamPtr input_, const ColumnNumbers & keys_, AggregateDescriptions & aggregates_)
		: input(input_), keys(keys_), aggregates(aggregates_), aggregator(keys_, aggregates_), has_been_read(false)
	{
		children.push_back(input);
	}

	Block readImpl();

	String getName() const { return "AggregatingBlockInputStream"; }

private:
	BlockInputStreamPtr input;
	const ColumnNumbers keys;
	AggregateDescriptions aggregates;
	Aggregator aggregator;
	bool has_been_read;
};

}
