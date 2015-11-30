#pragma once

#include <DB/Interpreters/Aggregator.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{


/** Доагрегирует потоки блоков, держа в оперативной памяти только по одному блоку из каждого потока.
  * Это экономит оперативку в случае использования двухуровневой агрегации, где в каждом потоке будет до 256 блоков с частями результата.
  *
  * Агрегатные функции в блоках не должны быть финализированы, чтобы их состояния можно было объединить.
  *
  * Замечания:
  *
  * На хорошей сети (10Gbit) может работать заметно медленнее, так как чтения блоков с разных
  *  удалённых серверов делаются последовательно, при этом, чтение упирается в CPU.
  * Это несложно исправить.
  *
  * Также, чтения и вычисления (слияние состояний) делаются по очереди.
  * Есть возможность делать чтения асинхронно - при этом будет расходоваться в два раза больше памяти, но всё-равно немного.
  * Это можно сделать с помощью UnionBlockInputStream.
  *
  * Можно держать в памяти не по одному блоку из каждого источника, а по несколько, и распараллелить мердж.
  * При этом будет расходоваться кратно больше оперативки.
  */
class MergingAggregatedMemoryEfficientBlockInputStream : public IProfilingBlockInputStream
{
public:
	MergingAggregatedMemoryEfficientBlockInputStream(BlockInputStreams inputs_, const Aggregator::Params & params, bool final_);

	String getName() const override { return "MergingAggregatedMemoryEfficient"; }

	String getID() const override;

protected:
	Block readImpl() override;

private:
	Aggregator aggregator;
	bool final;

	bool started = false;
	bool has_two_level = false;
	bool has_overflows = false;
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
};

}
