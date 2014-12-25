#include <iomanip>

#include <statdaemons/Stopwatch.h>

#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/AggregateFunctions/AggregateFunctionCount.h>

#include <DB/Interpreters/Aggregator.h>


namespace DB
{


AggregatedDataVariants::~AggregatedDataVariants()
{
	if (aggregator && !aggregator->all_aggregates_has_trivial_destructor)
	{
		try
		{
			aggregator->destroyAllAggregateStates(*this);
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}
	}
}


void Aggregator::initialize(Block & block)
{
	Poco::ScopedLock<Poco::FastMutex> lock(mutex);

	if (initialized)
		return;

	initialized = true;

	aggregate_functions.resize(aggregates_size);
	for (size_t i = 0; i < aggregates_size; ++i)
		aggregate_functions[i] = &*aggregates[i].function;

	/// Инициализируем размеры состояний и смещения для агрегатных функций.
	offsets_of_aggregate_states.resize(aggregates_size);
	total_size_of_aggregate_states = 0;
	all_aggregates_has_trivial_destructor = true;

	for (size_t i = 0; i < aggregates_size; ++i)
	{
		offsets_of_aggregate_states[i] = total_size_of_aggregate_states;
		total_size_of_aggregate_states += aggregates[i].function->sizeOfData();

		if (!aggregates[i].function->hasTrivialDestructor())
			all_aggregates_has_trivial_destructor = false;
	}

	/** Всё остальное - только если передан непустой block.
	  * (всё остальное не нужно в методе merge блоков с готовыми состояниями агрегатных функций).
	  */
	if (!block)
		return;

	/// Преобразуем имена столбцов в номера, если номера не заданы
	if (keys.empty() && !key_names.empty())
		for (Names::const_iterator it = key_names.begin(); it != key_names.end(); ++it)
			keys.push_back(block.getPositionByName(*it));

	for (AggregateDescriptions::iterator it = aggregates.begin(); it != aggregates.end(); ++it)
		if (it->arguments.empty() && !it->argument_names.empty())
			for (Names::const_iterator jt = it->argument_names.begin(); jt != it->argument_names.end(); ++jt)
				it->arguments.push_back(block.getPositionByName(*jt));

	/// Создадим пример блока, описывающего результат
	if (!sample)
	{
		for (size_t i = 0; i < keys_size; ++i)
		{
			sample.insert(block.getByPosition(keys[i]).cloneEmpty());
			if (sample.getByPosition(i).column->isConst())
				sample.getByPosition(i).column = dynamic_cast<IColumnConst &>(*sample.getByPosition(i).column).convertToFullColumn();
		}

		for (size_t i = 0; i < aggregates_size; ++i)
		{
			ColumnWithNameAndType col;
			col.name = aggregates[i].column_name;

			size_t arguments_size = aggregates[i].arguments.size();
			DataTypes argument_types(arguments_size);
			for (size_t j = 0; j < arguments_size; ++j)
				argument_types[j] = block.getByPosition(aggregates[i].arguments[j]).type;

			col.type = new DataTypeAggregateFunction(aggregates[i].function, argument_types, aggregates[i].parameters);
			col.column = col.type->createColumn();

			sample.insert(col);
		}
	}
}


AggregatedDataVariants::Type Aggregator::chooseAggregationMethod(const ConstColumnPlainPtrs & key_columns, Sizes & key_sizes)
{
	bool keys_fit_128_bits = true;
	size_t keys_bytes = 0;
	key_sizes.resize(keys_size);
	for (size_t j = 0; j < keys_size; ++j)
	{
		if (!key_columns[j]->isFixed())
		{
			keys_fit_128_bits = false;
			break;
		}
		key_sizes[j] = key_columns[j]->sizeOfField();
		keys_bytes += key_sizes[j];
	}
	if (keys_bytes > 16)
		keys_fit_128_bits = false;

	/// Если ключей нет
	if (keys_size == 0)
		return AggregatedDataVariants::WITHOUT_KEY;

	/// Если есть один числовой ключ, который помещается в 64 бита
	if (keys_size == 1 && key_columns[0]->isNumeric())
	{
		size_t size_of_field = key_columns[0]->sizeOfField();
		if (size_of_field == 1)
			return AggregatedDataVariants::KEY_8;
		if (size_of_field == 2)
			return AggregatedDataVariants::KEY_16;
		if (size_of_field == 4)
			return AggregatedDataVariants::KEY_32;
		if (size_of_field == 8)
			return AggregatedDataVariants::KEY_64;
		throw Exception("Logical error: numeric column has sizeOfField not in 1, 2, 4, 8.", ErrorCodes::LOGICAL_ERROR);
	}

	/// Если ключи помещаются в 128 бит, будем использовать хэш-таблицу по упакованным в 128-бит ключам
	if (keys_fit_128_bits)
		return AggregatedDataVariants::KEYS_128;

	/// Если есть один строковый ключ, то используем хэш-таблицу с ним
	if (keys_size == 1 && typeid_cast<const ColumnString *>(key_columns[0]))
		return AggregatedDataVariants::KEY_STRING;

	if (keys_size == 1 && typeid_cast<const ColumnFixedString *>(key_columns[0]))
		return AggregatedDataVariants::KEY_FIXED_STRING;

	/// Иначе будем агрегировать по хэшу от ключей.
	return AggregatedDataVariants::HASHED;
}


void Aggregator::createAggregateStates(AggregateDataPtr & aggregate_data) const
{
	for (size_t j = 0; j < aggregates_size; ++j)
	{
		try
		{
			/** Может возникнуть исключение при нехватке памяти.
			  * Для того, чтобы потом всё правильно уничтожилось, "откатываем" часть созданных состояний.
			  * Код не очень удобный.
			  */
			aggregate_functions[j]->create(aggregate_data + offsets_of_aggregate_states[j]);
		}
		catch (...)
		{
			for (size_t rollback_j = 0; rollback_j < j; ++rollback_j)
				aggregate_functions[rollback_j]->destroy(aggregate_data + offsets_of_aggregate_states[rollback_j]);

			aggregate_data = nullptr;
			throw;
		}
	}
}


/** Интересно - если убрать noinline, то gcc зачем-то инлайнит эту функцию, и производительность уменьшается (~10%).
  * (Возможно из-за того, что после инлайна этой функции, перестают инлайниться более внутренние функции.)
  * Инлайнить не имеет смысла, так как внутренний цикл находится целиком внутри этой функции.
  */
template <typename Method>
void NO_INLINE Aggregator::executeImpl(
	Method & method,
	Arena * aggregates_pool,
	size_t rows,
	ConstColumnPlainPtrs & key_columns,
	AggregateColumns & aggregate_columns,
	const Sizes & key_sizes,
	StringRefs & keys,
	bool no_more_keys,
	AggregateDataPtr overflow_row) const
{
	method.init(key_columns);

	/// Для всех строчек.
	for (size_t i = 0; i < rows; ++i)
	{
		typename Method::iterator it;
		bool inserted;			/// Вставили новый ключ, или такой ключ уже был?
		bool overflow = false;	/// Новый ключ не поместился в хэш-таблицу из-за no_more_keys.

		/// Получаем ключ для вставки в хэш-таблицу.
		typename Method::Key key = method.getKey(key_columns, keys_size, i, key_sizes, keys);

		if (Method::never_overflows || !no_more_keys)	/// Вставляем.
			method.data.emplace(key, it, inserted);
		else
		{
			/// Будем добавлять только если ключ уже есть.
			inserted = false;
			it = method.data.find(key);
			if (method.data.end() == it)
				overflow = true;
		}

		/// Если ключ не поместился, и данные не надо агрегировать в отдельную строку, то делать нечего.
		if (!Method::never_overflows && overflow && !overflow_row)
			continue;

		/// Если вставили новый ключ - инициализируем состояния агрегатных функций, и возможно, что-нибудь связанное с ключом.
		if (inserted)
		{
			method.onNewKey(it, keys_size, i, keys, *aggregates_pool);

			AggregateDataPtr & aggregate_data = Method::getAggregateData(it->second);
			aggregate_data = aggregates_pool->alloc(total_size_of_aggregate_states);
			createAggregateStates(aggregate_data);
		}

		AggregateDataPtr value = (Method::never_overflows || !overflow) ? Method::getAggregateData(it->second) : overflow_row;

		/// Добавляем значения в агрегатные функции.
		for (size_t j = 0; j < aggregates_size; ++j)
			aggregate_functions[j]->add(value + offsets_of_aggregate_states[j], &aggregate_columns[j][0], i);
	}
}


template <typename Method>
void NO_INLINE Aggregator::convertToBlockImpl(
	Method & method,
	ColumnPlainPtrs & key_columns,
	AggregateColumnsData & aggregate_columns,
	ColumnPlainPtrs & final_aggregate_columns,
	const Sizes & key_sizes,
	size_t start_row,
	bool final) const
{
	if (!final)
	{
		size_t j = start_row;
		for (typename Method::const_iterator it = method.data.begin(); it != method.data.end(); ++it, ++j)
		{
			method.insertKeyIntoColumns(it, key_columns, keys_size, key_sizes);

			for (size_t i = 0; i < aggregates_size; ++i)
				(*aggregate_columns[i])[j] = Method::getAggregateData(it->second) + offsets_of_aggregate_states[i];
		}
	}
	else
	{
		for (typename Method::const_iterator it = method.data.begin(); it != method.data.end(); ++it)
		{
			method.insertKeyIntoColumns(it, key_columns, keys_size, key_sizes);

			for (size_t i = 0; i < aggregates_size; ++i)
				aggregate_functions[i]->insertResultInto(
					Method::getAggregateData(it->second) + offsets_of_aggregate_states[i],
					*final_aggregate_columns[i]);
		}
	}
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeDataImpl(
	Table & table_dst,
	Table & table_src) const
{
	for (auto it = table_src.begin(); it != table_src.end(); ++it)
	{
		decltype(it) res_it;
		bool inserted;
		table_dst.emplace(it->first, res_it, inserted);

		if (!inserted)
		{
			for (size_t i = 0; i < aggregates_size; ++i)
				aggregate_functions[i]->merge(
					Method::getAggregateData(res_it->second) + offsets_of_aggregate_states[i],
					Method::getAggregateData(it->second) + offsets_of_aggregate_states[i]);

			for (size_t i = 0; i < aggregates_size; ++i)
				aggregate_functions[i]->destroy(
					Method::getAggregateData(it->second) + offsets_of_aggregate_states[i]);

			Method::getAggregateData(it->second) = nullptr;
		}
		else
		{
			res_it->second = it->second;
		}
	}
}


void NO_INLINE Aggregator::mergeWithoutKeyDataImpl(
	ManyAggregatedDataVariants & non_empty_data) const
{
	AggregatedDataVariantsPtr & res = non_empty_data[0];

	/// Все результаты агрегации соединяем с первым.
	for (size_t i = 1, size = non_empty_data.size(); i < size; ++i)
	{
		AggregatedDataWithoutKey & res_data = res->without_key;
		AggregatedDataWithoutKey & current_data = non_empty_data[i]->without_key;

		for (size_t i = 0; i < aggregates_size; ++i)
			aggregate_functions[i]->merge(res_data + offsets_of_aggregate_states[i], current_data + offsets_of_aggregate_states[i]);

		for (size_t i = 0; i < aggregates_size; ++i)
			aggregate_functions[i]->destroy(current_data + offsets_of_aggregate_states[i]);

		current_data = nullptr;
	}
}


template <typename Method>
void NO_INLINE Aggregator::mergeSingleLevelDataImpl(
	ManyAggregatedDataVariants & non_empty_data) const
{
	AggregatedDataVariantsPtr & res = non_empty_data[0];

	/// Все результаты агрегации соединяем с первым.
	for (size_t i = 1, size = non_empty_data.size(); i < size; ++i)
	{
		AggregatedDataVariants & current = *non_empty_data[i];

		mergeDataImpl<Method>(
			getDataVariant<Method>(*res).data,
			getDataVariant<Method>(current).data);

		/// current не будет уничтожать состояния агрегатных функций в деструкторе
		current.aggregator = nullptr;
	}
}


template <typename Method>
void NO_INLINE Aggregator::mergeTwoLevelDataImpl(
	ManyAggregatedDataVariants & non_empty_data,
	boost::threadpool::pool * thread_pool) const
{
	AggregatedDataVariantsPtr & res = non_empty_data[0];

	/// Слияние распараллеливается по корзинам - первому уровню TwoLevelHashMap.
	auto merge_bucket = [&non_empty_data, &res, this](size_t bucket)
	{
		/// Все результаты агрегации соединяем с первым.
		for (size_t i = 1, size = non_empty_data.size(); i < size; ++i)
		{
			AggregatedDataVariants & current = *non_empty_data[i];

			mergeDataImpl<Method>(
				getDataVariant<Method>(*res).data.impls[bucket],
				getDataVariant<Method>(current).data.impls[bucket]);

			/// current не будет уничтожать состояния агрегатных функций в деструкторе
			current.aggregator = nullptr;
		}
	};

	for (size_t bucket = 0; bucket < Method::Data::NUM_BUCKETS; ++bucket)
	{
		if (thread_pool)
			thread_pool->schedule(std::bind(merge_bucket, bucket));
		else
			merge_bucket(bucket);
	}

	if (thread_pool)
		thread_pool->wait();
}


template <typename Method>
void NO_INLINE Aggregator::mergeStreamsImpl(
	Method & method,
	Arena * aggregates_pool,
	size_t start_row,
	size_t rows,
	ConstColumnPlainPtrs & key_columns,
	AggregateColumnsData & aggregate_columns,
	const Sizes & key_sizes,
	StringRefs & keys) const
{
	method.init(key_columns);

	/// Для всех строчек.
	for (size_t i = start_row; i < rows; ++i)
	{
		typename Method::iterator it;
		bool inserted;			/// Вставили новый ключ, или такой ключ уже был?

		/// Получаем ключ для вставки в хэш-таблицу.
		typename Method::Key key = method.getKey(key_columns, keys_size, i, key_sizes, keys);

		method.data.emplace(key, it, inserted);

		if (inserted)
		{
			method.onNewKey(it, keys_size, i, keys, *aggregates_pool);

			AggregateDataPtr & aggregate_data = Method::getAggregateData(it->second);
			aggregate_data = aggregates_pool->alloc(total_size_of_aggregate_states);
			createAggregateStates(aggregate_data);
		}

		/// Мерджим состояния агрегатных функций.
		for (size_t j = 0; j < aggregates_size; ++j)
			aggregate_functions[j]->merge(
				Method::getAggregateData(it->second) + offsets_of_aggregate_states[j],
				(*aggregate_columns[j])[i]);
	}
}


template <typename Method>
void NO_INLINE Aggregator::destroyImpl(
	Method & method) const
{
	for (typename Method::const_iterator it = method.data.begin(); it != method.data.end(); ++it)
	{
		char * data = Method::getAggregateData(it->second);

		/** Если исключение (обычно нехватка памяти, кидается MemoryTracker-ом) возникло
		  *  после вставки ключа в хэш-таблицу, но до создания всех состояний агрегатных функций,
		  *  то data будет равен nullptr-у.
		  */
		if (nullptr == data)
			continue;

		for (size_t i = 0; i < aggregates_size; ++i)
			if (!aggregate_functions[i]->isState())
				aggregate_functions[i]->destroy(data + offsets_of_aggregate_states[i]);
	}
}


bool Aggregator::executeOnBlock(Block & block, AggregatedDataVariants & result,
	ConstColumnPlainPtrs & key_columns, AggregateColumns & aggregate_columns,
	Sizes & key_sizes, StringRefs & key,
	bool & no_more_keys)
{
	initialize(block);

	/// result будет уничтожать состояния агрегатных функций в деструкторе
	result.aggregator = this;

	for (size_t i = 0; i < aggregates_size; ++i)
		aggregate_columns[i].resize(aggregates[i].arguments.size());

	/// Запоминаем столбцы, с которыми будем работать
	for (size_t i = 0; i < keys_size; ++i)
	{
		key_columns[i] = block.getByPosition(keys[i]).column;

		if (key_columns[i]->isConst())
			throw Exception("Constants is not allowed as GROUP BY keys"
				" (but all of them must be eliminated in ExpressionAnalyzer)", ErrorCodes::ILLEGAL_COLUMN);
	}

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

	size_t rows = block.rows();

	/// Каким способом выполнять агрегацию?
	if (result.empty())
	{
		result.init(chooseAggregationMethod(key_columns, key_sizes));
		result.keys_size = keys_size;
		result.key_sizes = key_sizes;
		LOG_TRACE(log, "Aggregation method: " << result.getMethodName());
	}

	if (overflow_row && !result.without_key)
	{
		result.without_key = result.aggregates_pool->alloc(total_size_of_aggregate_states);
		createAggregateStates(result.without_key);
	}

	if (result.type == AggregatedDataVariants::WITHOUT_KEY)
	{
		AggregatedDataWithoutKey & res = result.without_key;
		if (!res)
		{
			res = result.aggregates_pool->alloc(total_size_of_aggregate_states);
			createAggregateStates(res);
		}

		/// Оптимизация в случае единственной агрегатной функции count.
		AggregateFunctionCount * agg_count = aggregates_size == 1
			? typeid_cast<AggregateFunctionCount *>(aggregate_functions[0])
			: NULL;

		if (agg_count)
			agg_count->addDelta(res, rows);
		else
		{
			for (size_t i = 0; i < rows; ++i)
			{
				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
					aggregate_functions[j]->add(res + offsets_of_aggregate_states[j], &aggregate_columns[j][0], i);
			}
		}
	}

	AggregateDataPtr overflow_row_ptr = overflow_row ? result.without_key : nullptr;

	if (result.type == AggregatedDataVariants::KEY_8)
		executeImpl(*result.key8, result.aggregates_pool, rows, key_columns, aggregate_columns,
			result.key_sizes, key, no_more_keys, overflow_row_ptr);
	else if (result.type == AggregatedDataVariants::KEY_16)
		executeImpl(*result.key16, result.aggregates_pool, rows, key_columns, aggregate_columns,
			result.key_sizes, key, no_more_keys, overflow_row_ptr);
	else if (result.type == AggregatedDataVariants::KEY_32)
		executeImpl(*result.key32, result.aggregates_pool, rows, key_columns, aggregate_columns,
			result.key_sizes, key, no_more_keys, overflow_row_ptr);
	else if (result.type == AggregatedDataVariants::KEY_64)
		executeImpl(*result.key64, result.aggregates_pool, rows, key_columns, aggregate_columns,
			result.key_sizes, key, no_more_keys, overflow_row_ptr);
	else if (result.type == AggregatedDataVariants::KEY_STRING)
		executeImpl(*result.key_string, result.aggregates_pool, rows, key_columns, aggregate_columns,
			result.key_sizes, key, no_more_keys, overflow_row_ptr);
	else if (result.type == AggregatedDataVariants::KEY_FIXED_STRING)
		executeImpl(*result.key_fixed_string, result.aggregates_pool, rows, key_columns, aggregate_columns,
			result.key_sizes, key, no_more_keys, overflow_row_ptr);
	else if (result.type == AggregatedDataVariants::KEYS_128)
		executeImpl(*result.keys128, result.aggregates_pool, rows, key_columns, aggregate_columns,
			result.key_sizes, key, no_more_keys, overflow_row_ptr);
	else if (result.type == AggregatedDataVariants::HASHED)
		executeImpl(*result.hashed, result.aggregates_pool, rows, key_columns, aggregate_columns,
			result.key_sizes, key, no_more_keys, overflow_row_ptr);
	else if (result.type != AggregatedDataVariants::WITHOUT_KEY)
		throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

	/// Проверка ограничений.
	if (!no_more_keys && max_rows_to_group_by && result.size() > max_rows_to_group_by)
	{
		if (group_by_overflow_mode == OverflowMode::THROW)
			throw Exception("Limit for rows to GROUP BY exceeded: has " + toString(result.size())
				+ " rows, maximum: " + toString(max_rows_to_group_by),
				ErrorCodes::TOO_MUCH_ROWS);
		else if (group_by_overflow_mode == OverflowMode::BREAK)
			return false;
		else if (group_by_overflow_mode == OverflowMode::ANY)
			no_more_keys = true;
		else
			throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
	}

	return true;
}


/** Результат хранится в оперативке и должен полностью помещаться в оперативку.
  */
void Aggregator::execute(BlockInputStreamPtr stream, AggregatedDataVariants & result)
{
	StringRefs key(keys_size);
	ConstColumnPlainPtrs key_columns(keys_size);
	AggregateColumns aggregate_columns(aggregates_size);
	Sizes key_sizes;

	/** Используется, если есть ограничение на максимальное количество строк при агрегации,
	  *  и если group_by_overflow_mode == ANY.
	  * В этом случае, новые ключи не добавляются в набор, а производится агрегация только по
	  *  ключам, которые уже успели попасть в набор.
	  */
	bool no_more_keys = false;

	LOG_TRACE(log, "Aggregating");

	Stopwatch watch;

	size_t src_rows = 0;
	size_t src_bytes = 0;

	/// Читаем все данные
	while (Block block = stream->read())
	{
		src_rows += block.rows();
		src_bytes += block.bytes();

		if (!executeOnBlock(block, result,
			key_columns, aggregate_columns, key_sizes, key,
			no_more_keys))
			break;
	}

	double elapsed_seconds = watch.elapsedSeconds();
	size_t rows = result.size();
	LOG_TRACE(log, std::fixed << std::setprecision(3)
		<< "Aggregated. " << src_rows << " to " << rows << " rows (from " << src_bytes / 1048576.0 << " MiB)"
		<< " in " << elapsed_seconds << " sec."
		<< " (" << src_rows / elapsed_seconds << " rows/sec., " << src_bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");
}


Block Aggregator::convertToBlock(AggregatedDataVariants & data_variants, bool final)
{
	Block res = sample.cloneEmpty();
	size_t rows = data_variants.size();

	LOG_TRACE(log, "Converting aggregated data to block");

	Stopwatch watch;

	/// В какой структуре данных агрегированы данные?
	if (data_variants.empty())
		return Block();

	ColumnPlainPtrs key_columns(keys_size);
	AggregateColumnsData aggregate_columns(aggregates_size);
	ColumnPlainPtrs final_aggregate_columns(aggregates_size);

	for (size_t i = 0; i < keys_size; ++i)
	{
		key_columns[i] = res.getByPosition(i).column;
		key_columns[i]->reserve(rows);
	}

	try
	{
		for (size_t i = 0; i < aggregates_size; ++i)
		{
			if (!final)
			{
				/// Столбец ColumnAggregateFunction захватывает разделяемое владение ареной с состояниями агрегатных функций.
				ColumnAggregateFunction & column_aggregate_func = static_cast<ColumnAggregateFunction &>(*res.getByPosition(i + keys_size).column);

				for (size_t j = 0; j < data_variants.aggregates_pools.size(); ++j)
					column_aggregate_func.addArena(data_variants.aggregates_pools[j]);

				aggregate_columns[i] = &column_aggregate_func.getData();
				aggregate_columns[i]->resize(rows);
			}
			else
			{
				ColumnWithNameAndType & column = res.getByPosition(i + keys_size);
				column.type = aggregate_functions[i]->getReturnType();
				column.column = column.type->createColumn();
				column.column->reserve(rows);

				if (aggregate_functions[i]->isState())
				{
					/// Столбец ColumnAggregateFunction захватывает разделяемое владение ареной с состояниями агрегатных функций.
					ColumnAggregateFunction & column_aggregate_func = static_cast<ColumnAggregateFunction &>(*column.column);

					for (size_t j = 0; j < data_variants.aggregates_pools.size(); ++j)
						column_aggregate_func.addArena(data_variants.aggregates_pools[j]);
				}

				final_aggregate_columns[i] = column.column;
			}
		}

		if (data_variants.type == AggregatedDataVariants::WITHOUT_KEY || overflow_row)
		{
			AggregatedDataWithoutKey & data = data_variants.without_key;

			for (size_t i = 0; i < aggregates_size; ++i)
				if (!final)
					(*aggregate_columns[i])[0] = data + offsets_of_aggregate_states[i];
				else
					aggregate_functions[i]->insertResultInto(data + offsets_of_aggregate_states[i], *final_aggregate_columns[i]);

			if (overflow_row)
				for (size_t i = 0; i < keys_size; ++i)
					key_columns[i]->insertDefault();
		}

		size_t start_row = overflow_row ? 1 : 0;

		if (data_variants.type == AggregatedDataVariants::KEY_8)
			convertToBlockImpl(*data_variants.key8, key_columns, aggregate_columns,
							final_aggregate_columns, data_variants.key_sizes, start_row, final);
		else if (data_variants.type == AggregatedDataVariants::KEY_16)
			convertToBlockImpl(*data_variants.key16, key_columns, aggregate_columns,
							final_aggregate_columns, data_variants.key_sizes, start_row, final);
		else if (data_variants.type == AggregatedDataVariants::KEY_32)
			convertToBlockImpl(*data_variants.key32, key_columns, aggregate_columns,
							final_aggregate_columns, data_variants.key_sizes, start_row, final);
		else if (data_variants.type == AggregatedDataVariants::KEY_64)
			convertToBlockImpl(*data_variants.key64, key_columns, aggregate_columns,
							final_aggregate_columns, data_variants.key_sizes, start_row, final);
		else if (data_variants.type == AggregatedDataVariants::KEY_STRING)
			convertToBlockImpl(*data_variants.key_string, key_columns, aggregate_columns,
							final_aggregate_columns, data_variants.key_sizes, start_row, final);
		else if (data_variants.type == AggregatedDataVariants::KEY_FIXED_STRING)
			convertToBlockImpl(*data_variants.key_fixed_string, key_columns, aggregate_columns,
							final_aggregate_columns, data_variants.key_sizes, start_row, final);
		else if (data_variants.type == AggregatedDataVariants::KEYS_128)
			convertToBlockImpl(*data_variants.keys128, key_columns, aggregate_columns,
							final_aggregate_columns, data_variants.key_sizes, start_row, final);
		else if (data_variants.type == AggregatedDataVariants::HASHED)
			convertToBlockImpl(*data_variants.hashed, key_columns, aggregate_columns,
							final_aggregate_columns, data_variants.key_sizes, start_row, final);
		else if (data_variants.type != AggregatedDataVariants::WITHOUT_KEY)
			throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
	}
	catch (...)
	{
		/** Работа с состояниями агрегатных функций недостаточно exception-safe.
		  * Если часть столбцов aggregate_columns была resize-на, но значения не были вставлены,
		  *  то эти столбцы будут в некорректном состоянии
		  *  (ColumnAggregateFunction попытаются в деструкторе вызвать деструкторы у элементов, которых нет),
		  *  а также деструкторы будут вызываться у AggregatedDataVariants.
		  * Поэтому, вручную "откатываем" их.
		  */
		for (size_t i = 0; i < aggregates_size; ++i)
			if (aggregate_columns[i])
				aggregate_columns[i]->clear();

		throw;
	}

	if (!final)
	{
		/// data_variants не будет уничтожать состояния агрегатных функций в деструкторе. Теперь состояниями владеют ColumnAggregateFunction.
		data_variants.aggregator = nullptr;
	}

	/// Изменяем размер столбцов-констант в блоке.
	size_t columns = res.columns();
	for (size_t i = 0; i < columns; ++i)
		if (res.getByPosition(i).column->isConst())
			res.getByPosition(i).column = res.getByPosition(i).column->cut(0, rows);

	double elapsed_seconds = watch.elapsedSeconds();
	LOG_TRACE(log, std::fixed << std::setprecision(3)
		<< "Converted aggregated data to block. "
		<< rows << " rows, " << res.bytes() / 1048576.0 << " MiB"
		<< " in " << elapsed_seconds << " sec."
		<< " (" << rows / elapsed_seconds << " rows/sec., " << res.bytes() / elapsed_seconds / 1048576.0 << " MiB/sec.)");

	return res;
}


AggregatedDataVariantsPtr Aggregator::merge(ManyAggregatedDataVariants & data_variants, size_t max_threads)
{
	if (data_variants.empty())
 		throw Exception("Empty data passed to Aggregator::merge().", ErrorCodes::EMPTY_DATA_PASSED);

	LOG_TRACE(log, "Merging aggregated data");

	Stopwatch watch;

	ManyAggregatedDataVariants non_empty_data;
	non_empty_data.reserve(data_variants.size());
	for (auto & data : data_variants)
		if (!data->empty())
			non_empty_data.push_back(data);

	if (non_empty_data.empty())
		return data_variants[0];

	if (non_empty_data.size() == 1)
		return non_empty_data[0];

	AggregatedDataVariantsPtr res = non_empty_data[0];

	size_t rows = res->size();
	for (size_t i = 1, size = non_empty_data.size(); i < size; ++i)
	{
		rows += non_empty_data[i]->size();
		AggregatedDataVariants & current = *non_empty_data[i];

		if (res->type != current.type)
			throw Exception("Cannot merge different aggregated data variants.", ErrorCodes::CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS);

		res->aggregates_pools.insert(res->aggregates_pools.end(), current.aggregates_pools.begin(), current.aggregates_pools.end());
	}

	/// В какой структуре данных агрегированы данные?
	if (res->type == AggregatedDataVariants::WITHOUT_KEY || overflow_row)
		mergeWithoutKeyDataImpl(non_empty_data);

	boost::threadpool::pool * thread_pool = nullptr;
	if (max_threads > 1 && rows > 100000	/// TODO Сделать настраиваемый порог.
		&& (   res->type == AggregatedDataVariants::KEY_32
			|| res->type == AggregatedDataVariants::KEY_64
			|| res->type == AggregatedDataVariants::KEY_STRING
			|| res->type == AggregatedDataVariants::KEY_FIXED_STRING
			|| res->type == AggregatedDataVariants::KEYS_128
			|| res->type == AggregatedDataVariants::HASHED))
		thread_pool = new boost::threadpool::pool(max_threads);

	if (res->type == AggregatedDataVariants::KEY_8)
		mergeSingleLevelDataImpl<AggregationMethodOneNumber<UInt8>>(non_empty_data);
	else if (res->type == AggregatedDataVariants::KEY_16)
		mergeSingleLevelDataImpl<AggregationMethodOneNumber<UInt16>>(non_empty_data);
	else if (res->type == AggregatedDataVariants::KEY_32)
		mergeTwoLevelDataImpl<AggregationMethodOneNumber<UInt32>>(non_empty_data, thread_pool);
	else if (res->type == AggregatedDataVariants::KEY_64)
		mergeTwoLevelDataImpl<AggregationMethodOneNumber<UInt64>>(non_empty_data, thread_pool);
	else if (res->type == AggregatedDataVariants::KEY_STRING)
		mergeTwoLevelDataImpl<AggregationMethodString>(non_empty_data, thread_pool);
	else if (res->type == AggregatedDataVariants::KEY_FIXED_STRING)
		mergeTwoLevelDataImpl<AggregationMethodFixedString>(non_empty_data, thread_pool);
	else if (res->type == AggregatedDataVariants::KEYS_128)
		mergeTwoLevelDataImpl<AggregationMethodKeys128>(non_empty_data, thread_pool);
	else if (res->type == AggregatedDataVariants::HASHED)
		mergeTwoLevelDataImpl<AggregationMethodHashed>(non_empty_data, thread_pool);
	else if (res->type != AggregatedDataVariants::WITHOUT_KEY)
		throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

	double elapsed_seconds = watch.elapsedSeconds();
	size_t res_rows = res->size();

	LOG_TRACE(log, std::fixed << std::setprecision(3)
		<< "Merged aggregated data. "
		<< "From " << rows << " to " << res_rows << " rows (efficiency: " << static_cast<double>(rows) / res_rows << ")"
		<< " in " << elapsed_seconds << " sec."
		<< " (" << rows / elapsed_seconds << " rows/sec.)");

	return res;
}


void Aggregator::merge(BlockInputStreamPtr stream, AggregatedDataVariants & result)
{
	StringRefs key(keys_size);
	ConstColumnPlainPtrs key_columns(keys_size);

	AggregateColumnsData aggregate_columns(aggregates_size);

	Block empty_block;
	initialize(empty_block);

	/// result будет уничтожать состояния агрегатных функций в деструкторе
	result.aggregator = this;

	/// Читаем все данные
	while (Block block = stream->read())
	{
		LOG_TRACE(log, "Merging aggregated block");

		if (!sample)
			for (size_t i = 0; i < keys_size + aggregates_size; ++i)
				sample.insert(block.getByPosition(i).cloneEmpty());

		/// Запоминаем столбцы, с которыми будем работать
		for (size_t i = 0; i < keys_size; ++i)
			key_columns[i] = block.getByPosition(i).column;

		for (size_t i = 0; i < aggregates_size; ++i)
			aggregate_columns[i] = &typeid_cast<ColumnAggregateFunction &>(*block.getByPosition(keys_size + i).column).getData();

		size_t rows = block.rows();

		/// Каким способом выполнять агрегацию?
		Sizes key_sizes;
		AggregatedDataVariants::Type method = chooseAggregationMethod(key_columns, key_sizes);

		if (result.empty())
		{
			result.init(method);
			result.keys_size = keys_size;
			result.key_sizes = key_sizes;
		}

		if (result.type == AggregatedDataVariants::WITHOUT_KEY || overflow_row)
		{
			AggregatedDataWithoutKey & res = result.without_key;
			if (!res)
			{
				res = result.aggregates_pool->alloc(total_size_of_aggregate_states);
				createAggregateStates(res);
			}

			/// Добавляем значения
			for (size_t i = 0; i < aggregates_size; ++i)
				aggregate_functions[i]->merge(res + offsets_of_aggregate_states[i], (*aggregate_columns[i])[0]);
		}

		size_t start_row = overflow_row ? 1 : 0;

		if (result.type == AggregatedDataVariants::KEY_8)
			mergeStreamsImpl(*result.key8, result.aggregates_pool, start_row, rows, key_columns, aggregate_columns, key_sizes, key);
		else if (result.type == AggregatedDataVariants::KEY_16)
			mergeStreamsImpl(*result.key16, result.aggregates_pool, start_row, rows, key_columns, aggregate_columns, key_sizes, key);
		else if (result.type == AggregatedDataVariants::KEY_32)
			mergeStreamsImpl(*result.key32, result.aggregates_pool, start_row, rows, key_columns, aggregate_columns, key_sizes, key);
		else if (result.type == AggregatedDataVariants::KEY_64)
			mergeStreamsImpl(*result.key64, result.aggregates_pool, start_row, rows, key_columns, aggregate_columns, key_sizes, key);
		else if (result.type == AggregatedDataVariants::KEY_STRING)
			mergeStreamsImpl(*result.key_string, result.aggregates_pool, start_row, rows, key_columns, aggregate_columns, key_sizes, key);
		else if (result.type == AggregatedDataVariants::KEY_FIXED_STRING)
			mergeStreamsImpl(*result.key_fixed_string, result.aggregates_pool, start_row, rows, key_columns, aggregate_columns, key_sizes, key);
		else if (result.type == AggregatedDataVariants::KEYS_128)
			mergeStreamsImpl(*result.keys128, result.aggregates_pool, start_row, rows, key_columns, aggregate_columns, key_sizes, key);
		else if (result.type == AggregatedDataVariants::HASHED)
			mergeStreamsImpl(*result.hashed, result.aggregates_pool, start_row, rows, key_columns, aggregate_columns, key_sizes, key);
		else if (result.type != AggregatedDataVariants::WITHOUT_KEY)
			throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

		LOG_TRACE(log, "Merged aggregated block");
	}
}


void Aggregator::destroyAllAggregateStates(AggregatedDataVariants & result)
{
	if (result.size() == 0)
		return;

	LOG_TRACE(log, "Destroying aggregate states");

	/// В какой структуре данных агрегированы данные?
	if (result.type == AggregatedDataVariants::WITHOUT_KEY || overflow_row)
	{
		AggregatedDataWithoutKey & res_data = result.without_key;

		if (nullptr != res_data)
			for (size_t i = 0; i < aggregates_size; ++i)
				if (!aggregate_functions[i]->isState())
					aggregate_functions[i]->destroy(res_data + offsets_of_aggregate_states[i]);
	}

	if (result.type == AggregatedDataVariants::KEY_8)
		destroyImpl(*result.key8);
	else if (result.type == AggregatedDataVariants::KEY_16)
		destroyImpl(*result.key16);
	else if (result.type == AggregatedDataVariants::KEY_32)
		destroyImpl(*result.key32);
	else if (result.type == AggregatedDataVariants::KEY_64)
		destroyImpl(*result.key64);
	else if (result.type == AggregatedDataVariants::KEY_STRING)
		destroyImpl(*result.key_string);
	else if (result.type == AggregatedDataVariants::KEY_FIXED_STRING)
		destroyImpl(*result.key_fixed_string);
	else if (result.type == AggregatedDataVariants::KEYS_128)
		destroyImpl(*result.keys128);
	else if (result.type == AggregatedDataVariants::HASHED)
		destroyImpl(*result.hashed);
	else if (result.type != AggregatedDataVariants::WITHOUT_KEY)
		throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
}


String Aggregator::getID() const
{
	std::stringstream res;

	if (keys.empty())
	{
		res << "key_names";
		for (size_t i = 0; i < key_names.size(); ++i)
			res << ", " << key_names[i];
	}
	else
	{
		res << "keys";
		for (size_t i = 0; i < keys.size(); ++i)
			res << ", " << keys[i];
	}

	res << ", aggregates";
	for (size_t i = 0; i < aggregates.size(); ++i)
		res << ", " << aggregates[i].column_name;

	return res.str();
}

}
