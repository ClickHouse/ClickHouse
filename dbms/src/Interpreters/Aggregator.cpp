#include <openssl/md5.h>

#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/Columns/ColumnAggregateFunction.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/AggregateFunctions/AggregateFunctionCount.h>

#include <DB/Interpreters/Aggregator.h>


namespace DB
{


void Aggregator::initialize(Block & block)
{
	Poco::ScopedLock<Poco::FastMutex> lock(mutex);

	if (initialized)
		return;

	initialized = true;
	
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
		for (size_t i = 0, size = keys.size(); i < size; ++i)
		{
			sample.insert(block.getByPosition(keys[i]).cloneEmpty());
			if (sample.getByPosition(i).column->isConst())
				sample.getByPosition(i).column = dynamic_cast<IColumnConst &>(*sample.getByPosition(i).column).convertToFullColumn();
		}

		for (size_t i = 0, size = aggregates.size(); i < size; ++i)
		{
			ColumnWithNameAndType col;
			col.name = aggregates[i].column_name;

			size_t arguments_size = aggregates[i].arguments.size();
			DataTypes argument_types(arguments_size);
			for (size_t j = 0; j < arguments_size; ++j)
				argument_types[j] = block.getByPosition(aggregates[i].arguments[j]).type;
			
			col.type = new DataTypeAggregateFunction(aggregates[i].function, argument_types);
			col.column = new ColumnAggregateFunction;

			sample.insert(col);
		}

		/// Вставим в блок результата все столбцы-константы из исходного блока, так как они могут ещё пригодиться.
		size_t columns = block.columns();
		for (size_t i = 0; i < columns; ++i)
			if (block.getByPosition(i).column->isConst())
				sample.insert(block.getByPosition(i).cloneEmpty());
	}
}


AggregatedDataVariants::Type Aggregator::chooseAggregationMethod(const ConstColumnPlainPtrs & key_columns, bool & keys_fit_128_bits, Sizes & key_sizes)
{
	keys_fit_128_bits = true;
	size_t keys_bytes = 0;
	key_sizes.resize(keys_size);
	for (size_t j = 0; j < keys_size; ++j)
	{
		if (!key_columns[j]->isNumeric())
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

	/// Если есть один ключ, который помещается в 64 бита, и это не число с плавающей запятой
	if (keys_size == 1 && key_columns[0]->isNumeric()
		&& !dynamic_cast<const ColumnFloat32 *>(key_columns[0]) && !dynamic_cast<const ColumnFloat64 *>(key_columns[0])
		&& !dynamic_cast<const ColumnConstFloat32 *>(key_columns[0]) && !dynamic_cast<const ColumnConstFloat64 *>(key_columns[0]))
		return AggregatedDataVariants::KEY_64;

	/// Если есть один строковый ключ, то используем хэш-таблицу с ним
	if (keys_size == 1
		&& (dynamic_cast<const ColumnString *>(key_columns[0]) || dynamic_cast<const ColumnFixedString *>(key_columns[0])
			|| dynamic_cast<const ColumnConstString *>(key_columns[0])))
		return AggregatedDataVariants::KEY_STRING;

	/// Если много ключей - будем агрегировать по хэшу от них
	return AggregatedDataVariants::HASHED;
}


/** Результат хранится в оперативке и должен полностью помещаться в оперативку.
  */
void Aggregator::execute(BlockInputStreamPtr stream, AggregatedDataVariants & result)
{
	size_t aggregates_size = aggregates.size();
	Row key(keys_size);
	ConstColumnPlainPtrs key_columns(keys_size);

	typedef std::vector<ConstColumnPlainPtrs> AggregateColumns;
	AggregateColumns aggregate_columns(aggregates_size);

	typedef AutoArray<Row> Rows;
	Rows aggregate_arguments(aggregates_size);

	/** Используется, если есть ограничение на максимальное количество строк при агрегации,
	  *  и если group_by_overflow_mode == ANY.
	  * В этом случае, новые ключи не добавляются в набор, а производится агрегация только по
	  *  ключам, которые уже успели попасть в набор.
	  */
	bool no_more_keys = false;

	/// Читаем все данные
	while (Block block = stream->read())
	{
		initialize(block);

		for (size_t i = 0; i < aggregates_size; ++i)
		{
			aggregate_arguments[i].resize(aggregates[i].arguments.size());
			aggregate_columns[i].resize(aggregates[i].arguments.size());
		}
		
		/// Запоминаем столбцы, с которыми будем работать
		for (size_t i = 0; i < keys_size; ++i)
			key_columns[i] = block.getByPosition(keys[i]).column;

		for (size_t i = 0; i < aggregates_size; ++i)
			for (size_t j = 0; j < aggregate_columns[i].size(); ++j)
				aggregate_columns[i][j] = block.getByPosition(aggregates[i].arguments[j]).column;

		size_t rows = block.rows();

		/// Каким способом выполнять агрегацию?
		bool keys_fit_128_bits = false;
		Sizes key_sizes;
		result.type = chooseAggregationMethod(key_columns, keys_fit_128_bits, key_sizes);

		if (result.type == AggregatedDataVariants::WITHOUT_KEY)
		{
			AggregatedDataWithoutKey & res = result.without_key;
			if (res.empty())
			{
				res.resize(aggregates_size);
				for (size_t i = 0; i < aggregates_size; ++i)
					res[i] = aggregates[i].function->cloneEmpty();
			}

			/// Оптимизация в случае единственной агрегатной функции count.
			AggregateFunctionCount * agg_count = dynamic_cast<AggregateFunctionCount *>(res[0]);
			if (aggregates_size == 1 && agg_count)
				agg_count->addDelta(rows);
			else
			{
				for (size_t i = 0; i < rows; ++i)
				{
					/// Добавляем значения
					for (size_t j = 0; j < aggregates_size; ++j)
					{
						for (size_t k = 0, size = aggregate_arguments[j].size(); k < size; ++k)
							aggregate_columns[j][k]->get(i, aggregate_arguments[j][k]);

						res[j]->add(aggregate_arguments[j]);
					}
				}
			}
		}
		else if (result.type == AggregatedDataVariants::KEY_64)
		{
			AggregatedDataWithUInt64Key & res = result.key64;
			const FieldVisitorToUInt64 visitor;
			const IColumn & column = *key_columns[0];

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				Field field = column[i];
				UInt64 key = apply_visitor(visitor, field);

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
					new(&it->second) AggregateFunctionsPlainPtrs(aggregates_size);
					
					for (size_t j = 0; j < aggregates_size; ++j)
						it->second[j] = aggregates[j].function->cloneEmpty();
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
				{
					for (size_t k = 0, size = aggregate_arguments[j].size(); k < size; ++k)
						aggregate_columns[j][k]->get(i, aggregate_arguments[j][k]);

					it->second[j]->add(aggregate_arguments[j]);
				}
			}
		}
		else if (result.type == AggregatedDataVariants::KEY_STRING)
		{
			AggregatedDataWithStringKey & res = result.key_string;
			const IColumn & column = *key_columns[0];

			if (const ColumnString * column_string = dynamic_cast<const ColumnString *>(&column))
			{
				const ColumnString::Offsets_t & offsets = column_string->getOffsets();
	            const ColumnUInt8::Container_t & data = dynamic_cast<const ColumnUInt8 &>(column_string->getData()).getData();

				/// Для всех строчек
				for (size_t i = 0; i < rows; ++i)
				{
					/// Строим ключ
					StringRef ref(&data[i == 0 ? 0 : offsets[i - 1]], (i == 0 ? offsets[i] : (offsets[i] - offsets[i - 1])) - 1);

					AggregatedDataWithStringKey::iterator it;
					bool inserted;

					if (!no_more_keys)
						res.emplace(ref, it, inserted);
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
						new(&it->second) AggregateFunctionsPlainPtrs(aggregates_size);

						for (size_t j = 0; j < aggregates_size; ++j)
							it->second[j] = aggregates[j].function->cloneEmpty();
					}

					/// Добавляем значения
					for (size_t j = 0; j < aggregates_size; ++j)
					{
						for (size_t k = 0, size = aggregate_arguments[j].size(); k < size; ++k)
							aggregate_columns[j][k]->get(i, aggregate_arguments[j][k]);

						it->second[j]->add(aggregate_arguments[j]);
					}
				}
			}
			else if (const ColumnFixedString * column_string = dynamic_cast<const ColumnFixedString *>(&column))
			{
				size_t n = column_string->getN();
				const ColumnUInt8::Container_t & data = dynamic_cast<const ColumnUInt8 &>(column_string->getData()).getData();

				/// Для всех строчек
				for (size_t i = 0; i < rows; ++i)
				{
					/// Строим ключ
					StringRef ref(&data[i * n], n);

					AggregatedDataWithStringKey::iterator it;
					bool inserted;

					if (!no_more_keys)
						res.emplace(ref, it, inserted);
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
						new(&it->second) AggregateFunctionsPlainPtrs(aggregates_size);

						for (size_t j = 0; j < aggregates_size; ++j)
							it->second[j] = aggregates[j].function->cloneEmpty();
					}

					/// Добавляем значения
					for (size_t j = 0; j < aggregates_size; ++j)
					{
						for (size_t k = 0, size = aggregate_arguments[j].size(); k < size; ++k)
							aggregate_columns[j][k]->get(i, aggregate_arguments[j][k]);

						it->second[j]->add(aggregate_arguments[j]);
					}
				}
			}
			else
				throw Exception("Illegal type of column when aggregating by string key: " + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
		}
		else if (result.type == AggregatedDataVariants::HASHED)
		{
			AggregatedDataHashed & res = result.hashed;

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				AggregatedDataHashed::iterator it;
				bool inserted;
				UInt128 key128 = pack128(i, keys_fit_128_bits, keys_size, key, key_columns, key_sizes);

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
					new(&it->second) AggregatedDataHashed::mapped_type(key, AggregateFunctionsPlainPtrs(aggregates_size));
					key.resize(keys_size);

					for (size_t j = 0; j < aggregates_size; ++j)
						it->second.second[j] = aggregates[j].function->cloneEmpty();
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
				{
					for (size_t k = 0, size = aggregate_arguments[j].size(); k < size; ++k)
						aggregate_columns[j][k]->get(i, aggregate_arguments[j][k]);

					it->second.second[j]->add(aggregate_arguments[j]);
				}
			}
		}
		else if (result.type == AggregatedDataVariants::GENERIC)
		{
			/// Общий способ
			AggregatedData & res = result.generic;
			
			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				for (size_t j = 0; j < keys_size; ++j)
					key_columns[j]->get(i, key[j]);

				AggregatedData::iterator it = res.find(key);
				if (it == res.end())
				{
					if (no_more_keys)
						continue;
					
					it = res.insert(std::make_pair(key, AggregateFunctionsPlainPtrs(aggregates_size))).first;
					key.resize(keys_size);

					for (size_t j = 0; j < aggregates_size; ++j)
						it->second[j] = aggregates[j].function->cloneEmpty();
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
				{
					for (size_t k = 0, size = aggregate_arguments[j].size(); k < size; ++k)
						aggregate_columns[j][k]->get(i, aggregate_arguments[j][k]);

					it->second[j]->add(aggregate_arguments[j]);
				}
			}
		}
		else
			throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

		/// Проверка ограничений.
		if (!no_more_keys && max_rows_to_group_by && result.size() > max_rows_to_group_by)
		{
			if (group_by_overflow_mode == Limits::THROW)
				throw Exception("Limit for rows to GROUP BY exceeded: has " + Poco::NumberFormatter::format(result.size())
					+ " rows, maximum: " + Poco::NumberFormatter::format(max_rows_to_group_by),
					ErrorCodes::TOO_MUCH_ROWS);
			else if (group_by_overflow_mode == Limits::BREAK)
				break;
			else if (group_by_overflow_mode == Limits::ANY)
				no_more_keys = true;
			else
				throw Exception("Logical error: unkown overflow mode", ErrorCodes::LOGICAL_ERROR);
		}
	}
}


Block Aggregator::convertToBlock(AggregatedDataVariants & data_variants)
{
	Block res = getSampleBlock();
	size_t rows = 0;

	LOG_TRACE(log, "Converting aggregated data to block");

	/// В какой структуре данных агрегированы данные?
	if (data_variants.empty())
		return res;

	if (data_variants.type == AggregatedDataVariants::WITHOUT_KEY)
	{
		AggregatedDataWithoutKey & data = data_variants.without_key;
		rows = 1;

		size_t i = 0;
		for (AggregateFunctionsPlainPtrs::const_iterator jt = data.begin(); jt != data.end(); ++jt, ++i)
			res.getByPosition(i).column->insert(AggregateFunctionPtr(*jt));
	}
	else if (data_variants.type == AggregatedDataVariants::KEY_64)
	{
		AggregatedDataWithUInt64Key & data = data_variants.key64;
		rows = data.size();

		IColumn & first_column = *res.getByPosition(0).column;
		bool is_signed = dynamic_cast<ColumnInt8 *>(&first_column) || dynamic_cast<ColumnInt16 *>(&first_column)
			|| dynamic_cast<ColumnInt32 *>(&first_column) || dynamic_cast<ColumnInt64 *>(&first_column);

		for (AggregatedDataWithUInt64Key::const_iterator it = data.begin(); it != data.end(); ++it)
		{
			if (is_signed)
				first_column.insert(static_cast<Int64>(it->first));
			else
				first_column.insert(it->first);

			size_t i = 1;
			for (AggregateFunctionsPlainPtrs::const_iterator jt = it->second.begin(); jt != it->second.end(); ++jt, ++i)
				res.getByPosition(i).column->insert(AggregateFunctionPtr(*jt));
		}
	}
	else if (data_variants.type == AggregatedDataVariants::KEY_STRING)
	{
		AggregatedDataWithStringKey & data = data_variants.key_string;
		rows = data.size();
		IColumn & first_column = *res.getByPosition(0).column;

		for (AggregatedDataWithStringKey::const_iterator it = data.begin(); it != data.end(); ++it)
		{
			first_column.insert(String(it->first.data, it->first.size));

			size_t i = 1;
			for (AggregateFunctionsPlainPtrs::const_iterator jt = it->second.begin(); jt != it->second.end(); ++jt, ++i)
				res.getByPosition(i).column->insert(AggregateFunctionPtr(*jt));
		}
	}
	else if (data_variants.type == AggregatedDataVariants::HASHED)
	{
		AggregatedDataHashed & data = data_variants.hashed;
		rows = data.size();
		for (AggregatedDataHashed::const_iterator it = data.begin(); it != data.end(); ++it)
		{
			size_t i = 0;
			for (Row::const_iterator jt = it->second.first.begin(); jt != it->second.first.end(); ++jt, ++i)
				res.getByPosition(i).column->insert(*jt);

			for (AggregateFunctionsPlainPtrs::const_iterator jt = it->second.second.begin(); jt != it->second.second.end(); ++jt, ++i)
				res.getByPosition(i).column->insert(AggregateFunctionPtr(*jt));
		}
	}
	else if (data_variants.type == AggregatedDataVariants::GENERIC)
	{
		AggregatedData & data = data_variants.generic;
		rows = data.size();
		for (AggregatedData::const_iterator it = data.begin(); it != data.end(); ++it)
		{
			size_t i = 0;
			for (Row::const_iterator jt = it->first.begin(); jt != it->first.end(); ++jt, ++i)
				res.getByPosition(i).column->insert(*jt);

			for (AggregateFunctionsPlainPtrs::const_iterator jt = it->second.begin(); jt != it->second.end(); ++jt, ++i)
				res.getByPosition(i).column->insert(AggregateFunctionPtr(*jt));
		}
	}
	else
		throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

	/// Изменяем размер столбцов-констант в блоке.
	size_t columns = res.columns();
	for (size_t i = 0; i < columns; ++i)
		if (res.getByPosition(i).column->isConst())
			res.getByPosition(i).column->cut(0, rows);

	LOG_TRACE(log, "Converted aggregated data to block");

	return res;
}


AggregatedDataVariantsPtr Aggregator::merge(ManyAggregatedDataVariants & data_variants)
{
	if (data_variants.empty())
 		throw Exception("Empty data passed to Aggregator::merge().", ErrorCodes::EMPTY_DATA_PASSED);

	LOG_TRACE(log, "Merging aggregated data");

	AggregatedDataVariantsPtr res = data_variants[0];

	/// Все результаты агрегации соединяем с первым.
	for (size_t i = 1, size = data_variants.size(); i < size; ++i)
	{
		AggregatedDataVariants & current = *data_variants[i];

		if (current.empty())
			continue;

		if (res->empty())
		{
			res = data_variants[i];
			continue;
		}

		if (res->type != current.type)
			throw Exception("Cannot merge different aggregated data variants.", ErrorCodes::CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS);

		/// В какой структуре данных агрегированы данные?
		if (res->type == AggregatedDataVariants::WITHOUT_KEY)
		{
			AggregatedDataWithoutKey & res_data = res->without_key;
			AggregatedDataWithoutKey & current_data = current.without_key;

			size_t i = 0;
			for (AggregateFunctionsPlainPtrs::const_iterator jt = current_data.begin(); jt != current_data.end(); ++jt, ++i)
			{
				res_data[i]->merge(**jt);
				delete *jt;
			}
		}
		else if (res->type == AggregatedDataVariants::KEY_64)
		{
			AggregatedDataWithUInt64Key & res_data = res->key64;
			AggregatedDataWithUInt64Key & current_data = current.key64;

			for (AggregatedDataWithUInt64Key::const_iterator it = current_data.begin(); it != current_data.end(); ++it)
			{
				AggregatedDataWithUInt64Key::iterator res_it;
				bool inserted;
				res_data.emplace(it->first, res_it, inserted);

				if (!inserted)
				{
					size_t i = 0;
					for (AggregateFunctionsPlainPtrs::const_iterator jt = it->second.begin(); jt != it->second.end(); ++jt, ++i)
					{
						res_it->second[i]->merge(**jt);
						delete *jt;
					}
				}
				else
					new(&res_it->second) AggregateFunctionsPlainPtrs(it->second);
			}
		}
		else if (res->type == AggregatedDataVariants::KEY_STRING)
		{
			AggregatedDataWithStringKey & res_data = res->key_string;
			AggregatedDataWithStringKey & current_data = current.key_string;
			
			for (AggregatedDataWithStringKey::const_iterator it = current_data.begin(); it != current_data.end(); ++it)
			{
				AggregatedDataWithStringKey::iterator res_it;
				bool inserted;
				res_data.emplace(it->first, res_it, inserted);

				if (!inserted)
				{
					size_t i = 0;
					for (AggregateFunctionsPlainPtrs::const_iterator jt = it->second.begin(); jt != it->second.end(); ++jt, ++i)
					{
						res_it->second[i]->merge(**jt);
						delete *jt;
					}
				}
				else
					new(&res_it->second) AggregateFunctionsPlainPtrs(it->second);
			}
		}
		else if (res->type == AggregatedDataVariants::HASHED)
		{
			AggregatedDataHashed & res_data = res->hashed;
			AggregatedDataHashed & current_data = current.hashed;

			for (AggregatedDataHashed::const_iterator it = current_data.begin(); it != current_data.end(); ++it)
			{
				AggregatedDataHashed::iterator res_it;
				bool inserted;
				res_data.emplace(it->first, res_it, inserted);

				if (!inserted)
				{
					size_t i = 0;
					for (AggregateFunctionsPlainPtrs::const_iterator jt = it->second.second.begin(); jt != it->second.second.end(); ++jt, ++i)
					{
						res_it->second.second[i]->merge(**jt);
						delete *jt;
					}
				}
				else
					new(&res_it->second) AggregatedDataHashed::mapped_type(it->second);
			}
		}
		else if (res->type == AggregatedDataVariants::GENERIC)
		{
			AggregatedData & res_data = res->generic;
			AggregatedData & current_data = current.generic;

			for (AggregatedData::const_iterator it = current_data.begin(); it != current_data.end(); ++it)
			{
				AggregateFunctionsPlainPtrs & res_row = res_data[it->first];
				if (!res_row.empty())
				{
					size_t i = 0;
					for (AggregateFunctionsPlainPtrs::const_iterator jt = it->second.begin(); jt != it->second.end(); ++jt, ++i)
					{
						res_row[i]->merge(**jt);
						delete *jt;
					}
				}
				else
					res_row = it->second;
			}
		}
		else
			throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
	}

	LOG_TRACE(log, "Merged aggregated data");

	return res;
}


void Aggregator::merge(BlockInputStreamPtr stream, AggregatedDataVariants & result)
{
	size_t aggregates_size = aggregates.size();
	Row key(keys_size);
	ConstColumnPlainPtrs key_columns(keys_size);

	typedef ColumnAggregateFunction::Container_t * AggregateColumn;
	typedef std::vector<AggregateColumn> AggregateColumns;
	AggregateColumns aggregate_columns(aggregates_size);

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
			aggregate_columns[i] = &dynamic_cast<ColumnAggregateFunction &>(*block.getByPosition(keys_size + i).column).getData();

		size_t rows = block.rows();

		/// Каким способом выполнять агрегацию?
		bool keys_fit_128_bits = false;
		Sizes key_sizes;
		result.type = chooseAggregationMethod(key_columns, keys_fit_128_bits, key_sizes);

		if (result.type == AggregatedDataVariants::WITHOUT_KEY)
		{
			AggregatedDataWithoutKey & res = result.without_key;
			if (res.empty())
			{
				res.resize(aggregates_size);
				for (size_t i = 0; i < aggregates_size; ++i)
					res[i] = aggregates[i].function->cloneEmpty();
			}

			/// Добавляем значения
			for (size_t i = 0; i < aggregates_size; ++i)
				res[i]->merge(*(*aggregate_columns[i])[0]);
		}
		else if (result.type == AggregatedDataVariants::KEY_64)
		{
			AggregatedDataWithUInt64Key & res = result.key64;
			const FieldVisitorToUInt64 visitor;
			const IColumn & column = *key_columns[0];

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				Field field = column[i];
				UInt64 key = apply_visitor(visitor, field);

				AggregatedDataWithUInt64Key::iterator it;
				bool inserted;
				res.emplace(key, it, inserted);

				if (inserted)
				{
					new(&it->second) AggregateFunctionsPlainPtrs(aggregates_size);

					for (size_t j = 0; j < aggregates_size; ++j)
						it->second[j] = aggregates[j].function->cloneEmpty();
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
					it->second[j]->merge(*(*aggregate_columns[j])[i]);
			}
		}
		else if (result.type == AggregatedDataVariants::KEY_STRING)
		{
			AggregatedDataWithStringKey & res = result.key_string;
			const IColumn & column = *key_columns[0];

			if (const ColumnString * column_string = dynamic_cast<const ColumnString *>(&column))
            {
                const ColumnString::Offsets_t & offsets = column_string->getOffsets();
                const ColumnUInt8::Container_t & data = dynamic_cast<const ColumnUInt8 &>(column_string->getData()).getData();

				/// Для всех строчек
				for (size_t i = 0; i < rows; ++i)
				{
					/// Строим ключ
					StringRef ref(&data[i == 0 ? 0 : offsets[i - 1]], (i == 0 ? offsets[i] : (offsets[i] - offsets[i - 1])) - 1);

					AggregatedDataWithStringKey::iterator it;
					bool inserted;
					res.emplace(ref, it, inserted);

					if (inserted)
					{
						it->first.data = result.string_pool.insert(ref.data, ref.size);
						new(&it->second) AggregateFunctionsPlainPtrs(aggregates_size);

						for (size_t j = 0; j < aggregates_size; ++j)
							it->second[j] = aggregates[j].function->cloneEmpty();
					}

					/// Добавляем значения
					for (size_t j = 0; j < aggregates_size; ++j)
						it->second[j]->merge(*(*aggregate_columns[j])[i]);
				}
			}
			else if (const ColumnFixedString * column_string = dynamic_cast<const ColumnFixedString *>(&column))
            {
                size_t n = column_string->getN();
                const ColumnUInt8::Container_t & data = dynamic_cast<const ColumnUInt8 &>(column_string->getData()).getData();

				/// Для всех строчек
				for (size_t i = 0; i < rows; ++i)
				{
					/// Строим ключ
					StringRef ref(&data[i * n], n);

					AggregatedDataWithStringKey::iterator it;
					bool inserted;
					res.emplace(ref, it, inserted);

					if (inserted)
					{
						it->first.data = result.string_pool.insert(ref.data, ref.size);
						new(&it->second) AggregateFunctionsPlainPtrs(aggregates_size);

						for (size_t j = 0; j < aggregates_size; ++j)
							it->second[j] = aggregates[j].function->cloneEmpty();
					}

					/// Добавляем значения
					for (size_t j = 0; j < aggregates_size; ++j)
						it->second[j]->merge(*(*aggregate_columns[j])[i]);
				}
			}
			else
				throw Exception("Illegal type of column when aggregating by string key: " + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
		}
		else if (result.type == AggregatedDataVariants::HASHED)
		{
			AggregatedDataHashed & res = result.hashed;

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				AggregatedDataHashed::iterator it;
				bool inserted;
				res.emplace(pack128(i, keys_fit_128_bits, keys_size, key, key_columns, key_sizes), it, inserted);

				if (inserted)
				{
					new(&it->second) AggregatedDataHashed::mapped_type(key, AggregateFunctionsPlainPtrs(aggregates_size));
					key.resize(keys_size);

					for (size_t j = 0; j < aggregates_size; ++j)
						it->second.second[j] = aggregates[j].function->cloneEmpty();
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
					it->second.second[j]->merge(*(*aggregate_columns[j])[i]);
			}
		}
		else if (result.type == AggregatedDataVariants::GENERIC)
		{
			/// Общий способ
			AggregatedData & res = result.generic;

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				for (size_t j = 0; j < keys_size; ++j)
					key_columns[j]->get(i, key[j]);

				AggregatedData::iterator it = res.find(key);
				if (it == res.end())
				{
					it = res.insert(std::make_pair(key, AggregateFunctionsPlainPtrs(aggregates_size))).first;
					key.resize(keys_size);

					for (size_t j = 0; j < aggregates_size; ++j)
						it->second[j] = aggregates[j].function->cloneEmpty();
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
					it->second[j]->merge(*(*aggregate_columns[j])[i]);
			}
		}
		else
			throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

		LOG_TRACE(log, "Merged aggregated block");
	}
}


}
