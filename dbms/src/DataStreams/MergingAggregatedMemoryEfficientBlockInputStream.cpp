#include <DB/DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>


namespace DB
{


MergingAggregatedMemoryEfficientBlockInputStream::MergingAggregatedMemoryEfficientBlockInputStream(
	BlockInputStreams inputs_, const Names & keys_names_,
	const AggregateDescriptions & aggregates_, bool overflow_row_, bool final_)
	: aggregator(keys_names_, aggregates_, overflow_row_, 0, OverflowMode::THROW, nullptr, 0, 0),
	final(final_),
	inputs(inputs_.begin(), inputs_.end())
{
	children = inputs_;
}

String MergingAggregatedMemoryEfficientBlockInputStream::getID() const
{
	std::stringstream res;
	res << "MergingAggregatedMemoryEfficient(" << aggregator.getID();
	for (size_t i = 0, size = children.size(); i < size; ++i)
		res << ", " << children.back()->getID();
	res << ")";
	return res.str();
}

Block MergingAggregatedMemoryEfficientBlockInputStream::readImpl()
{
	/// Если child - RemoteBlockInputStream, то отправляет запрос на все удалённые серверы, инициируя вычисления.
	/** NOTE: Если соединения ещё не установлены, то устанавливает их последовательно.
	  * И отправляет запрос последовательно. Это медленно.
	  */
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

		/// Если придёт блок не с основными данными, а с overflows, то запомним его и повторим чтение.
		while (true)
		{
//			std::cerr << "reading block\n";
			Block block = input.stream->read();

			if (!block)
			{
//				std::cerr << "input is exhausted\n";
				input.is_exhausted = true;
				break;
			}

			if (block.info.bucket_num != -1)
			{
				/// Один из разрезанных блоков для двухуровневых данных.
//				std::cerr << "block for bucket " << block.info.bucket_num << "\n";

				has_two_level = true;
				input.block = block;
			}
			else if (block.info.is_overflows)
			{
//				std::cerr << "block for overflows\n";

				has_overflows = true;
				input.overflow_block = block;

				continue;
			}
			else
			{
				/// Блок для неразрезанных (одноуровневых) данных.
//				std::cerr << "block without bucket\n";

				input.block = block;
			}

			break;
		}
	}

	while (true)
	{
		if (current_bucket_num == NUM_BUCKETS)
		{
			/// Обработали все основные данные. Остались, возможно, только overflows-блоки.
//			std::cerr << "at end\n";

			if (has_overflows)
			{
//				std::cerr << "merging overflows\n";

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
			/** Есть двухуровневые данные.
				* Будем обрабатывать номера корзин по возрастанию.
				* Найдём минимальный номер корзины, для которой есть данные,
				*  затем померджим эти данные.
				*/
//			std::cerr << "has two level\n";

			int min_bucket_num = NUM_BUCKETS;

			for (auto & input : inputs)
			{
				/// Изначально разрезанные (двухуровневые) блоки.
				if (input.block.info.bucket_num != -1 && input.block.info.bucket_num < min_bucket_num)
					min_bucket_num = input.block.info.bucket_num;

				/// Ещё не разрезанный по корзинам блок. Разрезаем его и кладём результат в splitted_blocks.
				if (input.block.info.bucket_num == -1 && input.block && input.splitted_blocks.empty())
				{
					LOG_TRACE(&Logger::get("MergingAggregatedMemoryEfficient"), "Having block without bucket: will split.");

					input.splitted_blocks = aggregator.convertBlockToTwoLevel(input.block);
					input.block = Block();
				}

				/// Блоки, которые мы получили разрезанием одноуровневых блоков.
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

//			std::cerr << "current_bucket_num = " << current_bucket_num << "\n";

			/// Блоков с основными данными больше нет.
			if (current_bucket_num == NUM_BUCKETS)
				continue;

			/// Теперь собираем блоки для current_bucket_num, чтобы их померджить.
			BlocksList blocks_to_merge;

			for (auto & input : inputs)
			{
				if (input.block.info.bucket_num == current_bucket_num)
				{
//					std::cerr << "having block for current_bucket_num\n";

					blocks_to_merge.emplace_back(std::move(input.block));
					input.block = Block();
				}
				else if (!input.splitted_blocks.empty() && input.splitted_blocks[min_bucket_num])
				{
//					std::cerr << "having splitted data for bucket\n";

					blocks_to_merge.emplace_back(std::move(input.splitted_blocks[min_bucket_num]));
					input.splitted_blocks[min_bucket_num] = Block();
				}
			}

			return aggregator.mergeBlocks(blocks_to_merge, final);
		}
		else
		{
			/// Есть только одноуровневые данные. Просто мерджим их.
//			std::cerr << "don't have two level\n";

			BlocksList blocks_to_merge;

			for (auto & input : inputs)
				if (input.block)
					blocks_to_merge.emplace_back(std::move(input.block));

			current_bucket_num = NUM_BUCKETS;
			return aggregator.mergeBlocks(blocks_to_merge, final);
		}
	}
}

}
