#include <openssl/md5.h>
#include <strconvert/hash64.h>

#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/Columns/ColumnAggregateFunction.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnsNumber.h>

#include <DB/Interpreters/Aggregator.h>


namespace DB
{

class FieldVisitorMD5 : public boost::static_visitor<>
{
public:
	MD5_CTX state;
	
	FieldVisitorMD5()
	{
		MD5_Init(&state);
	}

	void operator() (const Null 	& x)
	{
		int type = FieldType::Null;
		MD5_Update(&state, reinterpret_cast<const char *>(&type), sizeof(type));
	}
	
	void operator() (const UInt64 	& x)
	{
		int type = FieldType::UInt64;
		MD5_Update(&state, reinterpret_cast<const char *>(&type), sizeof(type));
		MD5_Update(&state, reinterpret_cast<const char *>(&x), sizeof(x));
	}
	
 	void operator() (const Int64 	& x)
	{
		int type = FieldType::Int64;
		MD5_Update(&state, reinterpret_cast<const char *>(&type), sizeof(type));
		MD5_Update(&state, reinterpret_cast<const char *>(&x), sizeof(x));
	}

	void operator() (const Float64 	& x)
	{
		int type = FieldType::Float64;
		MD5_Update(&state, reinterpret_cast<const char *>(&type), sizeof(type));
		MD5_Update(&state, reinterpret_cast<const char *>(&x), sizeof(x));
	}

	void operator() (const String 	& x)
	{
		int type = FieldType::String;
		MD5_Update(&state, reinterpret_cast<const char *>(&type), sizeof(type));
		MD5_Update(&state, x.data(), x.size());
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


/** Преобразование значения в 64 бита; если значение - строка, то используется относительно стойкая хэш-функция. */
class FieldVisitorHash64 : public boost::static_visitor<UInt64>
{
public:
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
		return strconvert::hash64(x);
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


/** Простой алгоритм (агрегация с помощью std::map).
  * Без оптимизации для агрегатных функций, принимающих не более одного значения.
  * Результат хранится в оперативке и должен полностью помещаться в оперативку.
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
		/// Преобразуем имена столбцов в номера, если номера не заданы
		if (keys.empty() && !key_names.empty())
			for (Names::const_iterator it = key_names.begin(); it != key_names.end(); ++it)
				keys.push_back(block.getPositionByName(*it));

		for (AggregateDescriptions::iterator it = aggregates.begin(); it != aggregates.end(); ++it)
			if (it->arguments.empty() && !it->argument_names.empty())
				for (Names::const_iterator jt = it->argument_names.begin(); jt != it->argument_names.end(); ++jt)
					it->arguments.push_back(block.getPositionByName(*jt));

		for (size_t i = 0; i < aggregates_size; ++i)
		{
			aggregate_arguments[i].resize(aggregates[i].arguments.size());
			aggregate_columns[i].resize(aggregates[i].arguments.size());
		}
		
		/// Запоминаем столбцы, с которыми будем работать
		for (size_t i = 0, size = keys_size; i < size; ++i)
			key_columns[i] = block.getByPosition(keys[i]).column;

		for (size_t i = 0; i < aggregates_size; ++i)
			for (size_t j = 0; j < aggregate_columns[i].size(); ++j)
				aggregate_columns[i][j] = block.getByPosition(aggregates[i].arguments[j]).column;

		/// Создадим пример блока, описывающего результат
		if (!sample)
		{
			for (size_t i = 0, size = keys_size; i < size; ++i)
				sample.insert(block.getByPosition(keys[i]).cloneEmpty());

			for (size_t i = 0; i < aggregates_size; ++i)
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

		size_t rows = block.rows();

		/// Каким способом выполнять агрегацию?
		bool has_strings = false;
		for (size_t j = 0; j < keys_size; ++j)
			if (dynamic_cast<const ColumnString *>(&*key_columns[j]) || dynamic_cast<const ColumnFixedString *>(&*key_columns[j]))
				has_strings = true;
		
		if (keys_size == 0)
		{
			/// Если ключей нет
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
		else if (keys_size == 1 && key_columns[0]->isNumeric()
			&& !dynamic_cast<ColumnFloat32 *>(&*key_columns[0]) && !dynamic_cast<ColumnFloat64 *>(&*key_columns[0]))
		{
			/// Если есть один ключ, который помещается в 64 бита, и это не число с плавающей запятой
			AggregatedDataWithUInt64Key & res = result.key64;
			const FieldVisitorHash64 visitor;
			IColumn & column = *key_columns[0];

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				Field field = column[i];
				UInt64 key = boost::apply_visitor(visitor, field);

				AggregatedDataWithUInt64Key::iterator it = res.find(key);
				if (it == res.end())
				{
					it = res.insert(std::make_pair(key, AggregateFunctions(aggregates_size))).first;

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
		else /*if (has_strings)*/
		{
			/// Если есть строки - будем агрегировать по хэшу от них
			AggregatedDataHashed & res = result.hashed;

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				FieldVisitorMD5 key_hash_visitor;
				
				/// Строим ключ
				for (size_t j = 0; j < keys_size; ++j)
				{
					key[j] = (*key_columns[j])[i];
					boost::apply_visitor(key_hash_visitor, key[j]);
				}

				union
				{
					UInt128 key_hash;
					unsigned char bytes[16];
				} key_hash_union;

				MD5_Final(key_hash_union.bytes, &key_hash_visitor.state);

				AggregatedDataHashed::iterator it = res.find(key_hash_union.key_hash);
				if (it == res.end())
				{
					it = res.insert(std::make_pair(key_hash_union.key_hash, std::make_pair(key, AggregateFunctions(aggregates_size)))).first;

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
/*		else
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
					it = res.insert(std::make_pair(key, AggregateFunctions(aggregates_size))).first;

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
		}*/
	}
}


}
