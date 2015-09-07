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
		final(final_),
		inputs(inputs_.begin(), inputs_.end())
	{
		children = inputs_;
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
		if (!started)
		{
			started = true;
			for (auto & child : children)
				child->readPrefix();
		}

		/** Имеем несколько источников.
		  * Из каждого из них могут приходить следующие данные:
		  *
		  * 1. Блок, с указанным bucket_num.
		  * Это значит, что на удалённом сервере, данные были разрезаны по корзинам.
		  * И данные для одного bucket_num с разных серверов можно независимо объединять.
		  * При этом, даннные для разных bucket_num будут идти по возрастанию.
		  *
		  * 2. Блок без указания bucket_num.
		  * Это значит, что на удалённом сервере, данные не были разрезаны по корзинам.
		  * В случае, когда со всех серверов прийдут такие данные, их можно всех объединить.
		  * А если с другой части серверов прийдут данные, разрезанные по корзинам,
		  *  то данные, не разрезанные по корзинам, нужно сначала разрезать, а потом объединять.
		  *
		  * 3. Блоки с указанием is_overflows.
		  * Это дополнительные данные для строк, не прошедших через max_rows_to_group_by.
		  * Они должны объединяться друг с другом отдельно.
		  */

		constexpr size_t NUM_BUCKETS = 256;

		++current_bucket_num;

		for (auto & input : inputs)
		{
			if (input.is_exhausted)
				continue;

			if (input.block.info.bucket_num >= current_bucket_num)
				continue;

			//std::cerr << "reading block\n";
			Block block = input.stream->read();

			if (!block)
			{
				//std::cerr << "input is exhausted\n";
				input.is_exhausted = true;
				continue;
			}

			if (block.info.bucket_num != -1)
			{
				//std::cerr << "block for bucket " << block.info.bucket_num << "\n";

				has_two_level = true;
				input.block = block;
			}
			else if (block.info.is_overflows)
			{
				//std::cerr << "block for overflows\n";

				has_overflows = true;
				input.overflow_block = block;
			}
			else
			{
				//std::cerr << "block without bucket\n";

				input.block = block;
			}
		}

		while (true)
		{
			if (current_bucket_num == NUM_BUCKETS)
			{
				//std::cerr << "at end\n";

				if (has_overflows)
				{
					//std::cerr << "merging overflows\n";

					has_overflows = false;
					BlocksList blocks_to_merge;

					for (auto & input : inputs)
						if (input.overflow_block)
							blocks_to_merge.emplace_back(std::move(input.overflow_block));

					return aggregator.mergeBlocks(blocks_to_merge, final);
				}
				else
					return {};
			}
			else if (has_two_level)
			{
				//std::cerr << "has two level\n";

				int min_bucket_num = NUM_BUCKETS;

				for (auto & input : inputs)
				{
					if (input.block.info.bucket_num != -1 && input.block.info.bucket_num < min_bucket_num)
						min_bucket_num = input.block.info.bucket_num;

					if (input.block.info.bucket_num == -1 && input.block && input.splitted_blocks.empty())
					{
						//std::cerr << "having block without bucket; will split\n";

						input.splitted_blocks = aggregator.convertBlockToTwoLevel(input.block);
						/// Нельзя уничтожать исходный блок.
					}

					if (!input.splitted_blocks.empty())
					{
						for (const auto & block : input.splitted_blocks)
						{
							if (block && block.info.bucket_num < min_bucket_num)
							{
								min_bucket_num = block.info.bucket_num;
								break;
							}
						}
					}
				}

				current_bucket_num = min_bucket_num;

				//std::cerr << "current_bucket_num = " << current_bucket_num << "\n";

				if (current_bucket_num == NUM_BUCKETS)
					continue;

				BlocksList blocks_to_merge;

				for (auto & input : inputs)
				{
					if (input.block.info.bucket_num == current_bucket_num)
					{
						//std::cerr << "having block for current_bucket_num\n";

						blocks_to_merge.emplace_back(std::move(input.block));
						input.block = Block();
					}
					else if (!input.splitted_blocks.empty() && input.splitted_blocks[min_bucket_num])
					{
						//std::cerr << "having splitted data for bucket\n";

						blocks_to_merge.emplace_back(std::move(input.splitted_blocks[min_bucket_num]));
						input.splitted_blocks[min_bucket_num] = Block();
					}
				}

				return aggregator.mergeBlocks(blocks_to_merge, final);
			}
			else
			{
				//std::cerr << "don't have two level\n";

				BlocksList blocks_to_merge;

				for (auto & input : inputs)
					if (input.block)
						blocks_to_merge.emplace_back(std::move(input.block));

				current_bucket_num = NUM_BUCKETS;
				return aggregator.mergeBlocks(blocks_to_merge, final);
			}
		}
	}

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
