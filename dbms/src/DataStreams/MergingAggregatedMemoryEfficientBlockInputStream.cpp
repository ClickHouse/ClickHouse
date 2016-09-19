#include <future>
#include <DB/Common/setThreadName.h>
#include <DB/Common/CurrentMetrics.h>
#include <DB/DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>


namespace DB
{


/** Scheme of operation:
  *
  * We have to output blocks in specific order: by bucket number:
  *
  *  o o o o ... o
  *  0 1 2 3     255
  *
  * Each block is the result of merge of blocks with same bucket number from several sources:
  *
  *  src1   o o ...
  *         | |
  *  src2   o o
  *
  *         | |
  *         v v
  *
  *  result o o
  *         0 1
  *
  * (we must merge 0th block from src1 with 0th block from src2 to form 0th result block and so on)
  *
  * We may read (request over network) blocks from different sources in parallel.
  * It is done by getNextBlocksToMerge method. Number of threads is 'reading_threads'.
  *
  * Also, we may do merges for different buckets in parallel.
  * For example, we may
  *      merge 1th block from src1 with 1th block from src2 in one thread
  *  and merge 2nd block from src1 with 2nd block from src2 in other thread.
  * Number of threads is 'merging_threads'
  * And we must keep only 'merging_threads' buckets of blocks in memory simultaneously,
  *  because our goal is to limit memory usage: not to keep all result in memory, but return it in streaming form.
  *
  * So, we return result sequentially, but perform calculations of resulting blocks in parallel.
  *  (calculation - is doing merge of source blocks for same buckets)
  *
  * Example:
  *
  *  src1   . . o o . . .
  *             | |
  *  src2       o o
  *
  *             | |
  *             v v
  *
  *  result . . o o . . .
  *
  * In this picture, we do only two merges in parallel.
  * When a merge is done, method 'getNextBlocksToMerge' is called to get blocks from sources for next bucket.
  * Then next merge is performed.
  *
  * Main ('readImpl') method is waiting for merged blocks for next bucket and returns it.
  */


MergingAggregatedMemoryEfficientBlockInputStream::MergingAggregatedMemoryEfficientBlockInputStream(
	BlockInputStreams inputs_, const Aggregator::Params & params, bool final_, size_t reading_threads_, size_t merging_threads_)
	: aggregator(params), final(final_),
	reading_threads(std::min(reading_threads_, inputs_.size())), merging_threads(merging_threads_),
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


void MergingAggregatedMemoryEfficientBlockInputStream::readPrefix()
{
	start();
}


void MergingAggregatedMemoryEfficientBlockInputStream::readSuffix()
{
	if (!all_read && !is_cancelled.load(std::memory_order_seq_cst))
		throw Exception("readSuffix called before all data is read", ErrorCodes::LOGICAL_ERROR);

	finalize();

	for (size_t i = 0; i < children.size(); ++i)
		children[i]->readSuffix();
}


void MergingAggregatedMemoryEfficientBlockInputStream::cancel()
{
	bool old_val = false;
	if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
		return;

	if (parallel_merge_data)
	{
		{
			std::unique_lock<std::mutex> lock(parallel_merge_data->merged_blocks_mutex);
			parallel_merge_data->finish = true;
		}
		parallel_merge_data->merged_blocks_changed.notify_one();	/// readImpl method must stop waiting and exit.
		parallel_merge_data->have_space.notify_all();				/// Merging threads must stop waiting and exit.
	}

	for (auto & input : inputs)
	{
		if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(input.stream.get()))
		{
			try
			{
				child->cancel();
			}
			catch (...)
			{
				/** Если не удалось попросить остановиться одного или несколько источников.
				  * (например, разорвано соединение при распределённой обработке запроса)
				  * - то пофиг.
				  */
				LOG_ERROR(log, "Exception while cancelling " << child->getName());
			}
		}
	}
}


void MergingAggregatedMemoryEfficientBlockInputStream::start()
{
	if (started)
		return;

	started = true;

	/// Если child - RemoteBlockInputStream, то child->readPrefix() отправляет запрос на удалённый сервер, инициируя вычисления.

	if (reading_threads == 1)
	{
		for (auto & child : children)
			child->readPrefix();
	}
	else
	{
		reading_pool = std::make_unique<ThreadPool>(reading_threads);

		size_t num_children = children.size();
		for (size_t i = 0; i < num_children; ++i)
		{
			auto & child = children[i];

			auto memory_tracker = current_memory_tracker;
			reading_pool->schedule([&child, memory_tracker]
			{
				current_memory_tracker = memory_tracker;
				setThreadName("MergeAggReadThr");
				CurrentMetrics::Increment metric_increment{CurrentMetrics::QueryThread};
				child->readPrefix();
			});
		}

		reading_pool->wait();
	}

	if (merging_threads > 1)
	{
		/** Create several threads. Each of them will pull next set of blocks to merge in a loop,
		  *  then merge them and place result in a queue (in fact, ordered map), from where we will read ready result blocks.
		  */
		parallel_merge_data = std::make_unique<ParallelMergeData>(merging_threads);

		auto & pool = parallel_merge_data->pool;

		/** Create threads that will receive and merge blocks.
		  */

		for (size_t i = 0; i < merging_threads; ++i)
			pool.schedule(std::bind(&MergingAggregatedMemoryEfficientBlockInputStream::mergeThread,
				this, current_memory_tracker));
	}
}


Block MergingAggregatedMemoryEfficientBlockInputStream::readImpl()
{
	start();

	if (merging_threads == 1)
	{
		if (BlocksToMerge blocks_to_merge = getNextBlocksToMerge())
			return aggregator.mergeBlocks(*blocks_to_merge, final);
		return {};
	}
	else
	{
		Block res;

		while (true)
		{
			std::unique_lock<std::mutex> lock(parallel_merge_data->merged_blocks_mutex);

			parallel_merge_data->merged_blocks_changed.wait(lock, [this]
			{
				return parallel_merge_data->finish					/// Requested to finish early.
					|| parallel_merge_data->exception				/// An error in merging thread.
					|| parallel_merge_data->exhausted				/// No more data in sources.
					|| !parallel_merge_data->merged_blocks.empty();	/// Have another merged block.
			});

			if (parallel_merge_data->exception)
				std::rethrow_exception(parallel_merge_data->exception);

			if (parallel_merge_data->finish)
				break;

			bool have_merged_block_or_merging_in_progress = !parallel_merge_data->merged_blocks.empty();

			if (parallel_merge_data->exhausted && !have_merged_block_or_merging_in_progress)
				break;

			if (have_merged_block_or_merging_in_progress)
			{
				auto it = parallel_merge_data->merged_blocks.begin();

				if (it->second)
				{
					res.swap(it->second);
					parallel_merge_data->merged_blocks.erase(it);

					lock.unlock();
					parallel_merge_data->have_space.notify_one();	/// We consumed block. Merging thread may merge next block for us.
					break;
				}
			}
		}

		if (!res)
			all_read = true;

		return res;
	}
}


MergingAggregatedMemoryEfficientBlockInputStream::~MergingAggregatedMemoryEfficientBlockInputStream()
{
	try
	{
		if (!all_read)
			cancel();

		finalize();
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}
}


void MergingAggregatedMemoryEfficientBlockInputStream::finalize()
{
	if (!started)
		return;

	LOG_TRACE(log, "Waiting for threads to finish");

	if (reading_pool)
		reading_pool->wait();

	if (parallel_merge_data)
		parallel_merge_data->pool.wait();

	LOG_TRACE(log, "Waited for threads to finish");
}


void MergingAggregatedMemoryEfficientBlockInputStream::mergeThread(MemoryTracker * memory_tracker)
{
	setThreadName("MergeAggMergThr");
	current_memory_tracker = memory_tracker;
	CurrentMetrics::Increment metric_increment{CurrentMetrics::QueryThread};

	try
	{
		while (!parallel_merge_data->finish)
		{
			/** Получение следующих блоков делается в одном пуле потоков, а мердж - в другом.
			  * Это весьма сложное взаимодействие.
			  * Каждый раз,
			  * - reading_threads читают по одному следующему блоку из каждого источника;
			  * - из этих блоков составляется группа блоков для слияния;
			  * - один из merging_threads выполняет слияние этой группы блоков;
			  */
			BlocksToMerge blocks_to_merge;
			int output_order = -1;

			/** Synchronously:
			  * - fetch next blocks from sources,
			  *    wait for space in 'merged_blocks'
			  *    and reserve a place in 'merged_blocks' to do merge of them;
			  * - or, if no next blocks, set 'exhausted' flag.
			  */
			{
				std::lock_guard<std::mutex> lock(parallel_merge_data->get_next_blocks_mutex);

				if (parallel_merge_data->exhausted || parallel_merge_data->finish)
					break;

				blocks_to_merge = getNextBlocksToMerge();

				if (!blocks_to_merge || blocks_to_merge->empty())
				{
					{
						std::unique_lock<std::mutex> lock(parallel_merge_data->merged_blocks_mutex);
						parallel_merge_data->exhausted = true;
					}

					/// No new blocks has been read from sources. (But maybe, in another mergeThread, some previous block is still prepared.)
					parallel_merge_data->merged_blocks_changed.notify_one();
					break;
				}

				output_order = blocks_to_merge->front().info.is_overflows
					? NUM_BUCKETS 	/// "Overflow" blocks returned by 'getNextBlocksToMerge' after all other blocks.
					: blocks_to_merge->front().info.bucket_num;

				{
					std::unique_lock<std::mutex> lock(parallel_merge_data->merged_blocks_mutex);

					parallel_merge_data->have_space.wait(lock, [this]
					{
						return parallel_merge_data->merged_blocks.size() < merging_threads
							|| parallel_merge_data->finish;
					});

					if (parallel_merge_data->finish)
						break;

					/** Place empty block. It is promise to do merge and fill it.
					  * Main thread knows, that there will be result for 'output_order' place.
					  * Main thread must return results exactly in 'output_order', so that is important.
					  */
					parallel_merge_data->merged_blocks[output_order];
				}
			}

			/// At this point, several merge threads may work in parallel.
			Block res = aggregator.mergeBlocks(*blocks_to_merge, final);

			{
				std::lock_guard<std::mutex> lock(parallel_merge_data->merged_blocks_mutex);

				if (parallel_merge_data->finish)
					break;

				parallel_merge_data->merged_blocks[output_order] = res;
			}

			/// Notify that we have another merged block.
			parallel_merge_data->merged_blocks_changed.notify_one();
		}
	}
	catch (...)
	{
		{
			std::lock_guard<std::mutex> lock(parallel_merge_data->merged_blocks_mutex);
			parallel_merge_data->exception = std::current_exception();
		}

		parallel_merge_data->merged_blocks_changed.notify_one();
		cancel();
	}
}


MergingAggregatedMemoryEfficientBlockInputStream::BlocksToMerge MergingAggregatedMemoryEfficientBlockInputStream::getNextBlocksToMerge()
{
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
	++current_bucket_num;

	/// Получить из источника следующий блок с номером корзины не больше current_bucket_num.

	auto need_that_input = [this] (Input & input)
	{
		return !input.is_exhausted
			&& input.block.info.bucket_num < current_bucket_num;
	};

	auto read_from_input = [this] (Input & input)
	{
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
	};

	if (reading_threads == 1)
	{
		for (auto & input : inputs)
			if (need_that_input(input))
				read_from_input(input);
	}
	else
	{
		for (auto & input : inputs)
		{
			if (need_that_input(input))
			{
				auto memory_tracker = current_memory_tracker;
				reading_pool->schedule([&input, &read_from_input, memory_tracker]
				{
					current_memory_tracker = memory_tracker;
					setThreadName("MergeAggReadThr");
					CurrentMetrics::Increment metric_increment{CurrentMetrics::QueryThread};
					read_from_input(input);
				});
			}
		}

		reading_pool->wait();
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
				BlocksToMerge blocks_to_merge = std::make_unique<BlocksList>();

				for (auto & input : inputs)
					if (input.overflow_block)
						blocks_to_merge->emplace_back(std::move(input.overflow_block));

				return blocks_to_merge;
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
			BlocksToMerge blocks_to_merge = std::make_unique<BlocksList>();

			for (auto & input : inputs)
			{
				if (input.block.info.bucket_num == current_bucket_num)
				{
//					std::cerr << "having block for current_bucket_num\n";

					blocks_to_merge->emplace_back(std::move(input.block));
					input.block = Block();
				}
				else if (!input.splitted_blocks.empty() && input.splitted_blocks[min_bucket_num])
				{
//					std::cerr << "having splitted data for bucket\n";

					blocks_to_merge->emplace_back(std::move(input.splitted_blocks[min_bucket_num]));
					input.splitted_blocks[min_bucket_num] = Block();
				}
			}

			return blocks_to_merge;
		}
		else
		{
			/// Есть только одноуровневые данные. Просто мерджим их.
//			std::cerr << "don't have two level\n";

			BlocksToMerge blocks_to_merge = std::make_unique<BlocksList>();

			for (auto & input : inputs)
				if (input.block)
					blocks_to_merge->emplace_back(std::move(input.block));

			current_bucket_num = NUM_BUCKETS;
			return blocks_to_merge;
		}
	}
}

}
