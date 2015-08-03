#pragma once

#include <DB/Interpreters/Aggregator.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{


/** Доагрегирует потоки блоков, держа в оперативной памяти только по одному блоку из каждого потока.
  * Это экономит оперативку в случае использования двухуровневой агрегации, где в каждом потоке будет до 256 блоков с частями результата.
  *
  * Агрегатные функции в блоках не должны быть финализированы, чтобы их состояния можно было объединить.
  */
class MergingAggregatedMemoryEfficientBlockInputStream : public IProfilingBlockInputStream
{
public:
	MergingAggregatedMemoryEfficientBlockInputStream(BlockInputStreams inputs_, const Names & keys_names_,
		const AggregateDescriptions & aggregates_, bool overflow_row_, bool final_)
		: aggregator(keys_names_, aggregates_, overflow_row_, 0, OverflowMode::THROW, nullptr, 0, 0),
		final(final_)
	{
		children = inputs_;
		current_blocks.resize(children.size());
		overflow_blocks.resize(children.size());
		is_exhausted.resize(children.size());
	}

	String getName() const override { return "MergingAggregatedMemorySavvy"; }

	String getID() const override
	{
		std::stringstream res;
		res << "MergingAggregatedMemorySavvy(" << aggregator.getID();
		for (size_t i = 0, size = children.size(); i < size; ++i)
			res << ", " << children.back()->getID();
		res << ")";
		return res.str();
	}

protected:
	Block readImpl() override
	{
		/// Если child - RemoteBlockInputStream, то отправляет запрос на все удалённые серверы, инициируя вычисления.
		if (current_bucket_num == -1)
			for (auto & child : children)
				child->readPrefix();

		/// Всё прочитали.
		if (current_bucket_num > 255)
			return {};

		/// Читаем следующие блоки для current_bucket_num
		for (size_t i = 0, size = children.size(); i < size; ++i)
		{
			while (!is_exhausted[i] && (!current_blocks[i] || current_blocks[i].info.bucket_num < current_bucket_num))
			{
				current_blocks[i] = children[i]->read();

				if (!current_blocks[i])
				{
					is_exhausted[i] = true;
				}
				else if (current_blocks[i].info.is_overflows)
				{
					overflow_blocks[i].swap(current_blocks[i]);
				}
			}
		}

		/// Может быть, нет блоков для current_bucket_num, а все блоки имеют больший bucket_num.
		Int32 min_bucket_num = 256;
		for (size_t i = 0, size = children.size(); i < size; ++i)
			if (!is_exhausted[i] && current_blocks[i].info.bucket_num < min_bucket_num)
				min_bucket_num = current_blocks[i].info.bucket_num;

		current_bucket_num = min_bucket_num;

		/// Все потоки исчерпаны.
		if (current_bucket_num > 255)
			return {};	/// TODO overflow_blocks.

		/// TODO Если есть single_level и two_level блоки.

		/// Объединяем все блоки с current_bucket_num.

		BlocksList blocks_to_merge;
		for (size_t i = 0, size = children.size(); i < size; ++i)
			if (current_blocks[i].info.bucket_num == current_bucket_num)
				blocks_to_merge.emplace_back(std::move(current_blocks[i]));

		Block res = aggregator.mergeBlocks(blocks_to_merge, final);

		++current_bucket_num;
		return res;
	}

private:
	Aggregator aggregator;
	bool final;

	Int32 current_bucket_num = -1;
	std::vector<Block> current_blocks;
	std::vector<UInt8> is_exhausted;

	std::vector<Block> overflow_blocks;
};

}
