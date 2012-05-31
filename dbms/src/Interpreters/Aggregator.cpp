#include <openssl/md5.h>

#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/Columns/ColumnAggregateFunction.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnsNumber.h>

#include <DB/Interpreters/Aggregator.h>


namespace DB
{

class FieldVisitorHash : public boost::static_visitor<>
{
public:
	MD5_CTX state;
	
	FieldVisitorHash()
	{
		MD5_Init(&state);
	}

	void finalize(unsigned char * place)
	{
		MD5_Final(place, &state);
	}

	void operator() (const Null 	& x)
	{
		char type = FieldType::Null;
		MD5_Update(&state, reinterpret_cast<const char *>(&type), sizeof(type));
	}
	
	void operator() (const UInt64 	& x)
	{
		char type = FieldType::UInt64;
		MD5_Update(&state, reinterpret_cast<const char *>(&type), sizeof(type));
		MD5_Update(&state, reinterpret_cast<const char *>(&x), sizeof(x));
	}
	
 	void operator() (const Int64 	& x)
	{
		char type = FieldType::Int64;
		MD5_Update(&state, reinterpret_cast<const char *>(&type), sizeof(type));
		MD5_Update(&state, reinterpret_cast<const char *>(&x), sizeof(x));
	}

	void operator() (const Float64 	& x)
	{
		char type = FieldType::Float64;
		MD5_Update(&state, reinterpret_cast<const char *>(&type), sizeof(type));
		MD5_Update(&state, reinterpret_cast<const char *>(&x), sizeof(x));
	}

	void operator() (const String 	& x)
	{
		char type = FieldType::String;
		MD5_Update(&state, reinterpret_cast<const char *>(&type), sizeof(type));
		/// Используем ноль на конце.
		MD5_Update(&state, x.c_str(), x.size() + 1);
	}

	void operator() (const Array 	& x)
	{
		throw Exception("Cannot aggregate by array", ErrorCodes::ILLEGAL_KEY_OF_AGGREGATION);
	}

	void operator() (const SharedPtr<IAggregateFunction> & x)
	{
		throw Exception("Cannot aggregate by state of aggregate function", ErrorCodes::ILLEGAL_KEY_OF_AGGREGATION);
	}
};


/** Преобразование значения в 64 бита. Для чисел - однозначное, для строк - некриптографический хэш. */
class FieldVisitorToUInt64 : public boost::static_visitor<UInt64>
{
public:
	FieldVisitorToUInt64() {}
	
	UInt64 operator() (const Null 		& x) const { return 0; }
	UInt64 operator() (const UInt64 	& x) const { return x; }
	UInt64 operator() (const Int64 		& x) const { return x; }

	UInt64 operator() (const Float64 	& x) const
	{
		UInt64 res = 0;
		memcpy(reinterpret_cast<char *>(&res), reinterpret_cast<const char *>(&x), sizeof(x));
		return res;
	}

	UInt64 operator() (const String 	& x) const
	{
		return std::tr1::hash<String>()(x);
	}

	UInt64 operator() (const Array 	& x) const
	{
		throw Exception("Cannot aggregate by array", ErrorCodes::ILLEGAL_KEY_OF_AGGREGATION);
	}

	UInt64 operator() (const SharedPtr<IAggregateFunction> & x) const
	{
		throw Exception("Cannot aggregate by state of aggregate function", ErrorCodes::ILLEGAL_KEY_OF_AGGREGATION);
	}
};


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
			col.type = new DataTypeAggregateFunction;
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


AggregatedDataVariants::Type Aggregator::chooseAggregationMethod(Columns & key_columns, bool & keys_fit_128_bits, Sizes & key_sizes)
{
	size_t keys_size = key_columns.size();
	
/*	bool has_strings = false;
	for (size_t j = 0; j < keys_size; ++j)
		if (dynamic_cast<const ColumnString *>(&*key_columns[j]) || dynamic_cast<const ColumnFixedString *>(&*key_columns[j]))
			has_strings = true;*/

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
		&& !dynamic_cast<ColumnFloat32 *>(&*key_columns[0]) && !dynamic_cast<ColumnFloat64 *>(&*key_columns[0]))
		return AggregatedDataVariants::KEY_64;

	/// Если есть один строковый ключ, то используем хэш-таблицу с ним
	if (keys_size == 1
		&& (dynamic_cast<ColumnString *>(&*key_columns[0]) || dynamic_cast<ColumnFixedString *>(&*key_columns[0])))
		return AggregatedDataVariants::KEY_STRING;

	/// Если много ключей - будем агрегировать по хэшу от них
	return AggregatedDataVariants::HASHED;
}


/** Результат хранится в оперативке и должен полностью помещаться в оперативку.
  */
void Aggregator::execute(BlockInputStreamPtr stream, AggregatedDataVariants & result)
{
	size_t keys_size = keys.empty() ? key_names.size() : keys.size();
	size_t aggregates_size = aggregates.size();
	Row key(keys_size);
	Columns key_columns(keys_size);

	typedef std::vector<Columns> AggregateColumns;
	AggregateColumns aggregate_columns(aggregates_size);

	typedef std::vector<Row> Rows;
	Rows aggregate_arguments(aggregates_size);

	/// Читаем все данные
	while (Block block = stream->read())
	{
		LOG_TRACE(log, "Aggregating block");
		
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
			
			for (size_t i = 0; i < rows; ++i)
			{
				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
				{
					for (size_t k = 0, size = aggregate_arguments[j].size(); k < size; ++k)
						aggregate_arguments[j][k] = (*aggregate_columns[j][k])[i];

					res[j]->add(aggregate_arguments[j]);
				}
			}
		}
		else if (result.type == AggregatedDataVariants::KEY_64)
		{
			AggregatedDataWithUInt64Key & res = result.key64;
			const FieldVisitorToUInt64 visitor;
			IColumn & column = *key_columns[0];

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				Field field = column[i];
				UInt64 key = boost::apply_visitor(visitor, field);

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
				{
					for (size_t k = 0, size = aggregate_arguments[j].size(); k < size; ++k)
						aggregate_arguments[j][k] = (*aggregate_columns[j][k])[i];

					it->second[j]->add(aggregate_arguments[j]);
				}
			}
		}
		else if (result.type == AggregatedDataVariants::KEY_STRING)
		{
			AggregatedDataWithStringKey & res = result.key_string;
			IColumn & column = *key_columns[0];

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				String key = boost::get<String>(column[i]);

				AggregatedDataWithStringKey::iterator it = res.find(key);
				if (it == res.end())
				{
					it = res.insert(std::make_pair(key, AggregateFunctionsPlainPtrs(aggregates_size))).first;

					for (size_t j = 0; j < aggregates_size; ++j)
						it->second[j] = aggregates[j].function->cloneEmpty();
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
				{
					for (size_t k = 0, size = aggregate_arguments[j].size(); k < size; ++k)
						aggregate_arguments[j][k] = (*aggregate_columns[j][k])[i];

					it->second[j]->add(aggregate_arguments[j]);
				}
			}
		}
		else if (result.type == AggregatedDataVariants::HASHED)
		{
			AggregatedDataHashed & res = result.hashed;
			const FieldVisitorToUInt64 to_uint64_visitor;

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				union
				{
					UInt128 key_hash;
					unsigned char bytes[16];
				} key_hash_union;

				/// Если все ключи числовые и помещаются в 128 бит
				if (keys_fit_128_bits)
				{
					memset(key_hash_union.bytes, 0, 16);
					size_t offset = 0;
					for (size_t j = 0; j < keys_size; ++j)
					{
						key[j] = (*key_columns[j])[i];
						UInt64 tmp = boost::apply_visitor(to_uint64_visitor, key[j]);
						/// Работает только на little endian
						memcpy(key_hash_union.bytes + offset, reinterpret_cast<const char *>(&tmp), key_sizes[j]);
						offset += key_sizes[j];
					}
				}
				else	/// Иначе используем md5.
				{
					FieldVisitorHash key_hash_visitor;

					for (size_t j = 0; j < keys_size; ++j)
					{
						key[j] = (*key_columns[j])[i];
						boost::apply_visitor(key_hash_visitor, key[j]);
					}

					key_hash_visitor.finalize(key_hash_union.bytes);
				}

				AggregatedDataHashed::iterator it;
				bool inserted;
				res.emplace(key_hash_union.key_hash, it, inserted);

				if (inserted)
				{
					new(&it->second) AggregatedDataHashed::mapped_type(key, AggregateFunctionsPlainPtrs(aggregates_size));

					for (size_t j = 0; j < aggregates_size; ++j)
						it->second.second[j] = aggregates[j].function->cloneEmpty();
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
				{
					for (size_t k = 0, size = aggregate_arguments[j].size(); k < size; ++k)
						aggregate_arguments[j][k] = (*aggregate_columns[j][k])[i];

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
					key[j] = (*key_columns[j])[i];

				AggregatedData::iterator it = res.find(key);
				if (it == res.end())
				{
					it = res.insert(std::make_pair(key, AggregateFunctionsPlainPtrs(aggregates_size))).first;

					for (size_t j = 0; j < aggregates_size; ++j)
						it->second[j] = aggregates[j].function->cloneEmpty();
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
				{
					for (size_t k = 0, size = aggregate_arguments[j].size(); k < size; ++k)
						aggregate_arguments[j][k] = (*aggregate_columns[j][k])[i];

					it->second[j]->add(aggregate_arguments[j]);
				}
			}
		}
		else
			throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

		LOG_TRACE(log, "Aggregated block");
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
			res.getByPosition(i).column->insert(*jt);
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
				res.getByPosition(i).column->insert(*jt);
		}
	}
	else if (data_variants.type == AggregatedDataVariants::KEY_STRING)
	{
		AggregatedDataWithStringKey & data = data_variants.key_string;
		rows = data.size();
		IColumn & first_column = *res.getByPosition(0).column;

		for (AggregatedDataWithStringKey::const_iterator it = data.begin(); it != data.end(); ++it)
		{
			first_column.insert(it->first);

			size_t i = 1;
			for (AggregateFunctionsPlainPtrs::const_iterator jt = it->second.begin(); jt != it->second.end(); ++jt, ++i)
				res.getByPosition(i).column->insert(*jt);
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
				res.getByPosition(i).column->insert(*jt);
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
				res.getByPosition(i).column->insert(*jt);
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

	AggregatedDataVariants & res = *data_variants[0];

	/// Все результаты агрегации соединяем с первым.
	for (size_t i = 1, size = data_variants.size(); i < size; ++i)
	{
		AggregatedDataVariants & current = *data_variants[i];

		if (current.empty())
			continue;

		if (res.empty())
		{
			data_variants[0] = data_variants[i];
			continue;
		}

		if (res.type != current.type)
			throw Exception("Cannot merge different aggregated data variants.", ErrorCodes::CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS);

		/// В какой структуре данных агрегированы данные?
		if (res.type == AggregatedDataVariants::WITHOUT_KEY)
		{
			AggregatedDataWithoutKey & res_data = res.without_key;
			AggregatedDataWithoutKey & current_data = current.without_key;

			size_t i = 0;
			for (AggregateFunctionsPlainPtrs::const_iterator jt = current_data.begin(); jt != current_data.end(); ++jt, ++i)
			{
				res_data[i]->merge(**jt);
				delete *jt;
			}
		}
		else if (res.type == AggregatedDataVariants::KEY_64)
		{
			AggregatedDataWithUInt64Key & res_data = res.key64;
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
					res_it->second = it->second;
			}
		}
		else if (res.type == AggregatedDataVariants::KEY_STRING)
		{
			AggregatedDataWithStringKey & res_data = res.key_string;
			AggregatedDataWithStringKey & current_data = current.key_string;
			
			for (AggregatedDataWithStringKey::const_iterator it = current_data.begin(); it != current_data.end(); ++it)
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
		else if (res.type == AggregatedDataVariants::HASHED)
		{
			AggregatedDataHashed & res_data = res.hashed;
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
					res_it->second = it->second;
			}
		}
		else if (res.type == AggregatedDataVariants::GENERIC)
		{
			AggregatedData & res_data = res.generic;
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

	return data_variants[0];
}


void Aggregator::merge(BlockInputStreamPtr stream, AggregatedDataVariants & result)
{
	size_t keys_size = keys.empty() ? key_names.size() : keys.size();
	size_t aggregates_size = aggregates.size();
	Row key(keys_size);
	Columns key_columns(keys_size);

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
			IColumn & column = *key_columns[0];

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				Field field = column[i];
				UInt64 key = boost::apply_visitor(visitor, field);

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
			IColumn & column = *key_columns[0];

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				String key = boost::get<String>(column[i]);

				AggregatedDataWithStringKey::iterator it = res.find(key);
				if (it == res.end())
				{
					it = res.insert(std::make_pair(key, AggregateFunctionsPlainPtrs(aggregates_size))).first;

					for (size_t j = 0; j < aggregates_size; ++j)
						it->second[j] = aggregates[j].function->cloneEmpty();
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
					it->second[j]->merge(*(*aggregate_columns[j])[i]);
			}
		}
		else if (result.type == AggregatedDataVariants::HASHED)
		{
			AggregatedDataHashed & res = result.hashed;
			const FieldVisitorToUInt64 to_uint64_visitor;

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				union
				{
					UInt128 key_hash;
					unsigned char bytes[16];
				} key_hash_union;

				/// Если все ключи числовые и помещаются в 128 бит
				if (keys_fit_128_bits)
				{
					memset(key_hash_union.bytes, 0, 16);
					size_t offset = 0;
					for (size_t j = 0; j < keys_size; ++j)
					{
						key[j] = (*key_columns[j])[i];
						UInt64 tmp = boost::apply_visitor(to_uint64_visitor, key[j]);
						/// Работает только на little endian
						memcpy(key_hash_union.bytes + offset, reinterpret_cast<const char *>(&tmp), key_sizes[j]);
						offset += key_sizes[j];
					}
				}
				else	/// Иначе используем md5.
				{
					FieldVisitorHash key_hash_visitor;

					for (size_t j = 0; j < keys_size; ++j)
					{
						key[j] = (*key_columns[j])[i];
						boost::apply_visitor(key_hash_visitor, key[j]);
					}

					key_hash_visitor.finalize(key_hash_union.bytes);
				}

				AggregatedDataHashed::iterator it;
				bool inserted;
				res.emplace(key_hash_union.key_hash, it, inserted);

				if (inserted)
				{
					new(&it->second) AggregatedDataHashed::mapped_type(key, AggregateFunctionsPlainPtrs(aggregates_size));

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
					key[j] = (*key_columns[j])[i];

				AggregatedData::iterator it = res.find(key);
				if (it == res.end())
				{
					it = res.insert(std::make_pair(key, AggregateFunctionsPlainPtrs(aggregates_size))).first;

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
