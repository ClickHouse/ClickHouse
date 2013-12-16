#include <iomanip>

#include <statdaemons/Stopwatch.h>

#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>

#include <DB/Interpreters/SplittingAggregator.h>


namespace DB
{


void SplittingAggregator::execute(BlockInputStreamPtr stream, ManyAggregatedDataVariants & results)
{
	Stopwatch watch;

	/// Читаем все данные
	while (Block block = stream->read())
	{
		initialize(block);

		src_rows += block.rows();
		src_bytes += block.bytes();

		for (size_t i = 0; i < aggregates_size; ++i)
			aggregate_columns[i].resize(aggregates[i].arguments.size());
		
		/// Запоминаем столбцы, с которыми будем работать
		for (size_t i = 0; i < keys_size; ++i)
			key_columns[i] = block.getByPosition(keys[i]).column;

		for (size_t i = 0; i < aggregates_size; ++i)
		{
			for (size_t j = 0; j < aggregate_columns[i].size(); ++j)
			{
				aggregate_columns[i][j] = block.getByPosition(aggregates[i].arguments[j]).column;

				/** Агрегатные функции рассчитывают, что в них передаются полноценные столбцы.
				  * Поэтому, стобцы-константы не разрешены в качестве аргументов агрегатных функций.
				  */
				if (aggregate_columns[i][j]->isConst())
					throw Exception("Constants is not allowed as arguments of aggregate functions", ErrorCodes::ILLEGAL_COLUMN);
			}
		}

		rows = block.rows();

		/// Каким способом выполнять агрегацию?
		if (method == AggregatedDataVariants::EMPTY)
		{
			method = chooseAggregationMethod(key_columns, key_sizes);
	//		LOG_TRACE(log, "Aggregation method: " << result.getMethodName());
		}

		/// Подготавливаем массивы, куда будут складываться ключи или хэши от ключей.
		if (method == AggregatedDataVariants::KEY_64)
		{
			keys64.resize(rows);
		}
		else if (method == AggregatedDataVariants::KEY_STRING)
		{
			hashes64.resize(rows);
			string_refs.resize(rows);
		}
		else if (method == AggregatedDataVariants::KEYS_128)
		{
			keys128.resize(rows);
		}
		else if (method == AggregatedDataVariants::HASHED)
		{
			hashes128.resize(rows);
		}
		else
			throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

		thread_nums.resize(rows);

		if (results.empty())
		{
			results.resize(threads);
			for (size_t i = 0; i < threads; ++i)
			{
				results[i] = new AggregatedDataVariants;
				results[i]->init(method);
				results[i]->keys_size = keys_size;
				results[i]->key_sizes = key_sizes;
			}
		}

		Exceptions exceptions(threads);

		/// Параллельно вычисляем хэши и ключи.

		LOG_TRACE(log, "Calculating keys and hashes.");

		watch.start();

		for (size_t thread_no = 0; thread_no < threads; ++thread_no)
			pool.schedule(boost::bind(&SplittingAggregator::calculateHashesThread, this,
				boost::ref(block),
				rows * thread_no / threads,
				rows * (thread_no + 1) / threads,
				boost::ref(exceptions[thread_no])));

		pool.wait();

		rethrowFirstException(exceptions);

		LOG_TRACE(log, "Calculated keys and hashes in " << std::fixed << std::setprecision(2) << watch.elapsedSeconds() << " sec.");
		watch.restart();

		/// Параллельно агрегируем в независимые хэш-таблицы

		LOG_TRACE(log, "Parallel aggregating.");

		for (size_t thread_no = 0; thread_no < threads; ++thread_no)
			pool.schedule(boost::bind(&SplittingAggregator::aggregateThread, this,
				boost::ref(block),
				boost::ref(*results[thread_no]),
				thread_no,
				boost::ref(exceptions[thread_no])));

		pool.wait();

		rethrowFirstException(exceptions);

		LOG_TRACE(log, "Parallel aggregated in " << std::fixed << std::setprecision(2) << watch.elapsedSeconds() << " sec.");

		/// Проверка ограничений

		if (max_rows_to_group_by && size_of_all_results > max_rows_to_group_by && group_by_overflow_mode == Limits::BREAK)
			break;
	}

/*	double elapsed_seconds = watch.elapsedSeconds();
	size_t rows = result.size();
	LOG_TRACE(log, std::fixed << std::setprecision(3)
		<< "Aggregated. " << src_rows << " to " << rows << " rows (from " << src_bytes / 1048576.0 << " MiB)"
		<< " in " << elapsed_seconds << " sec."
		<< " (" << src_rows / elapsed_seconds << " rows/sec., " << src_bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");*/
}


void SplittingAggregator::convertToBlocks(ManyAggregatedDataVariants & data_variants, Blocks & blocks, bool final)
{
	if (data_variants.empty())
		return;

	blocks.resize(data_variants.size());
	Exceptions exceptions(threads);

	/// Параллельно конвертируем в блоки.

	for (size_t thread_no = 0; thread_no < threads; ++thread_no)
		pool.schedule(boost::bind(&SplittingAggregator::convertToBlockThread, this,
			boost::ref(*data_variants[thread_no]),
			boost::ref(blocks[thread_no]),
			final,
			boost::ref(exceptions[thread_no])));

	pool.wait();

	rethrowFirstException(exceptions);
}


void SplittingAggregator::calculateHashesThread(Block & block, size_t begin, size_t end, ExceptionPtr & exception)
{
	try
	{
		if (method == AggregatedDataVariants::KEY_64)
		{
			const IColumn & column = *key_columns[0];

			for (size_t i = begin; i < end; ++i)
			{
				keys64[i] = get<UInt64>(column[i]);
				thread_nums[i] = intHash32<0xd1f93e3190506c7cULL>(keys64[i]) % threads;
			}
		}
		else if (method == AggregatedDataVariants::KEY_STRING)
		{
			const IColumn & column = *key_columns[0];

			if (const ColumnString * column_string = dynamic_cast<const ColumnString *>(&column))
			{
				const ColumnString::Offsets_t & offsets = column_string->getOffsets();
	            const ColumnString::Chars_t & data = column_string->getChars();

				for (size_t i = begin; i < end; ++i)
				{
					string_refs[i] = StringRef(&data[i == 0 ? 0 : offsets[i - 1]], (i == 0 ? offsets[i] : (offsets[i] - offsets[i - 1])) - 1);
					hashes64[i] = hash_func_string(string_refs[i]);
					thread_nums[i] = (hashes64[i] >> 32) % threads;
				}
			}
			else if (const ColumnFixedString * column_string = dynamic_cast<const ColumnFixedString *>(&column))
			{
				size_t n = column_string->getN();
				const ColumnFixedString::Chars_t & data = column_string->getChars();

				for (size_t i = begin; i < end; ++i)
				{
					string_refs[i] = StringRef(&data[i * n], n);
					hashes64[i] = hash_func_string(string_refs[i]);
					thread_nums[i] = (hashes64[i] >> 32) % threads;
				}
			}
			else
				throw Exception("Illegal type of column when aggregating by string key: " + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
		}
		else if (method == AggregatedDataVariants::KEYS_128)
		{
			for (size_t i = begin; i < end; ++i)
			{
				keys128[i] = pack128(i, keys_size, key_columns, key_sizes);
				thread_nums[i] = (intHash32<0xd1f93e3190506c7cULL>(intHash32<0x271e6f39e4bd34c3ULL>(keys128[i].first) ^ keys128[i].second)) % threads;
			}
		}
		else if (method == AggregatedDataVariants::HASHED)
		{
			for (size_t i = begin; i < end; ++i)
			{
				hashes128[i] = hash128(i, keys_size, key_columns);
				thread_nums[i] = hashes128[i].second % threads;
			}
		}
		else
			throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
	}
	catch (...)
	{
		exception = cloneCurrentException();
	}
}


void SplittingAggregator::aggregateThread(Block & block, AggregatedDataVariants & result, size_t thread_no, ExceptionPtr & exception)
{
	try
	{
		result.aggregator = this;

		/** Используется, если есть ограничение на максимальное количество строк при агрегации,
		  *  и если group_by_overflow_mode == ANY.
		  * В этом случае, новые ключи не добавляются в набор, а производится агрегация только по
		  *  ключам, которые уже успели попасть в набор.
		  */
		bool no_more_keys = max_rows_to_group_by && size_of_all_results > max_rows_to_group_by;
		size_t old_result_size = result.size();

		if (method == AggregatedDataVariants::KEY_64)
		{
			AggregatedDataWithUInt64Key & res = *result.key64;

			for (size_t i = 0; i < rows; ++i)
			{
				if (thread_nums[i] != thread_no)
					continue;

				/// Берём ключ
				UInt64 key = keys64[i];

				AggregatedDataWithUInt64Key::iterator it;
				bool inserted;

				if (!no_more_keys)
					res.emplace(key, it, inserted);
				else
				{
					inserted = false;
					it = res.find(key);
					if (res.end() == it)
						continue;
				}

				if (inserted)
				{
					it->second = result.aggregates_pool->alloc(total_size_of_aggregate_states);

					for (size_t j = 0; j < aggregates_size; ++j)
						aggregate_functions[j]->create(it->second + offsets_of_aggregate_states[j]);
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
					aggregate_functions[j]->add(it->second + offsets_of_aggregate_states[j], &aggregate_columns[j][0], i);
			}
		}
		else if (method == AggregatedDataVariants::KEY_STRING)
		{
			AggregatedDataWithStringKey & res = *result.key_string;

			for (size_t i = 0; i < rows; ++i)
			{
				if (thread_nums[i] != thread_no)
					continue;

				AggregatedDataWithStringKey::iterator it;
				bool inserted;

				StringRef ref = string_refs[i];

				if (!no_more_keys)
					res.emplace(ref, it, inserted, hashes64[i]);
				else
				{
					inserted = false;
					it = res.find(ref);
					if (res.end() == it)
						continue;
				}
				
				if (inserted)
				{
					it->first.data = result.string_pool.insert(ref.data, ref.size);
					it->second = result.aggregates_pool->alloc(total_size_of_aggregate_states);

					for (size_t j = 0; j < aggregates_size; ++j)
						aggregate_functions[j]->create(it->second + offsets_of_aggregate_states[j]);
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
					aggregate_functions[j]->add(it->second + offsets_of_aggregate_states[j], &aggregate_columns[j][0], i);
			}
		}
		else if (method == AggregatedDataVariants::KEYS_128)
		{
			AggregatedDataWithKeys128 & res = *result.keys128;

			for (size_t i = 0; i < rows; ++i)
			{
				if (thread_nums[i] != thread_no)
					continue;

				AggregatedDataWithKeys128::iterator it;
				bool inserted;
				UInt128 key128 = keys128[i];

				if (!no_more_keys)
					res.emplace(key128, it, inserted);
				else
				{
					inserted = false;
					it = res.find(key128);
					if (res.end() == it)
						continue;
				}

				if (inserted)
				{
					it->second = result.aggregates_pool->alloc(total_size_of_aggregate_states);

					for (size_t j = 0; j < aggregates_size; ++j)
						aggregate_functions[j]->create(it->second + offsets_of_aggregate_states[j]);
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
					aggregate_functions[j]->add(it->second + offsets_of_aggregate_states[j], &aggregate_columns[j][0], i);
			}
		}
		else if (method == AggregatedDataVariants::HASHED)
		{
			StringRefs key(keys_size);
			AggregatedDataHashed & res = *result.hashed;

			for (size_t i = 0; i < rows; ++i)
			{
				if (thread_nums[i] != thread_no)
					continue;

				AggregatedDataHashed::iterator it;
				bool inserted;
				UInt128 key128 = hashes128[i];

				if (!no_more_keys)
					res.emplace(key128, it, inserted);
				else
				{
					inserted = false;
					it = res.find(key128);
					if (res.end() == it)
						continue;
				}

				if (inserted)
				{
					it->second.first = extractKeysAndPlaceInPool(i, keys_size, key_columns, key, result.keys_pool);
					it->second.second = result.aggregates_pool->alloc(total_size_of_aggregate_states);

					for (size_t j = 0; j < aggregates_size; ++j)
						aggregate_functions[j]->create(it->second.second + offsets_of_aggregate_states[j]);
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
					aggregate_functions[j]->add(it->second.second + offsets_of_aggregate_states[j], &aggregate_columns[j][0], i);
			}
		}
		else
			throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

		/// Проверка ограничений.
		size_t current_size_of_all_results = __sync_add_and_fetch(&size_of_all_results, result.size() - old_result_size);

		if (max_rows_to_group_by && current_size_of_all_results > max_rows_to_group_by && group_by_overflow_mode == Limits::THROW)
			throw Exception("Limit for rows to GROUP BY exceeded: has " + toString(current_size_of_all_results)
				+ " rows, maximum: " + toString(max_rows_to_group_by),
				ErrorCodes::TOO_MUCH_ROWS);
	}
	catch (...)
	{
		exception = cloneCurrentException();
	}
}


void SplittingAggregator::convertToBlockThread(AggregatedDataVariants & data_variant, Block & block, bool final, ExceptionPtr & exception)
{
	try
	{
		Block totals;
		block = convertToBlock(data_variant, false, totals, final);
	}
	catch (...)
	{
		exception = cloneCurrentException();
	}
}


}
