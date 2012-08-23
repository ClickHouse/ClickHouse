#include <DB/Core/Field.h>

#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnsNumber.h>

#include <DB/Interpreters/AggregationCommon.h>
#include <DB/Interpreters/Set.h>


namespace DB
{

Set::Type Set::chooseMethod(Columns & key_columns, bool & keys_fit_128_bits, Sizes & key_sizes)
{
	size_t keys_size = key_columns.size();

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

	/// Если есть один ключ, который помещается в 64 бита, и это не число с плавающей запятой
	if (keys_size == 1 && key_columns[0]->isNumeric()
		&& !dynamic_cast<ColumnFloat32 *>(&*key_columns[0]) && !dynamic_cast<ColumnFloat64 *>(&*key_columns[0]))
		return KEY_64;

	/// Если есть один строковый ключ, то используем хэш-таблицу с ним
	if (keys_size == 1
		&& (dynamic_cast<ColumnString *>(&*key_columns[0]) || dynamic_cast<ColumnFixedString *>(&*key_columns[0])))
		return KEY_STRING;

	/// Если много ключей - будем строить множество хэшей от них
	return HASHED;
}


void Set::create(BlockInputStreamPtr stream)
{
	LOG_TRACE(log, "Creating set");
	
	/// Читаем все данные
	while (Block block = stream->read())
	{
		size_t keys_size = block.columns();
		Row key(keys_size);
		Columns key_columns(keys_size);
		
		/// Запоминаем столбцы, с которыми будем работать
		for (size_t i = 0; i < keys_size; ++i)
			key_columns[i] = block.getByPosition(i).column;

		size_t rows = block.rows();

		/// Какую структуру данных для множества использовать?
		bool keys_fit_128_bits = false;
		Sizes key_sizes;
		type = chooseMethod(key_columns, keys_fit_128_bits, key_sizes);

		if (type == KEY_64)
		{
			SetUInt64 & res = key64;
			const FieldVisitorToUInt64 visitor;
			IColumn & column = *key_columns[0];

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				Field field = column[i];
				UInt64 key = boost::apply_visitor(visitor, field);
				res.insert(key);
			}
		}
		else if (type == KEY_STRING)
		{
			SetString & res = key_string;
			IColumn & column = *key_columns[0];

			if (const ColumnString * column_string = dynamic_cast<const ColumnString *>(&column))
			{
				const ColumnString::Offsets_t & offsets = column_string->getOffsets();
				const ColumnUInt8::Container_t & data = dynamic_cast<const ColumnUInt8 &>(column_string->getData()).getData();

				/// Для всех строчек
				for (size_t i = 0; i < rows; ++i)
				{
					/// Строим ключ
					StringRef ref(&data[i == 0 ? 0 : offsets[i - 1]], (i == 0 ? offsets[i] : (offsets[i] - offsets[i - 1])) - 1);

					SetString::iterator it;
					bool inserted;
					res.emplace(ref, it, inserted);

					if (inserted)
						it->data = string_pool.insert(ref.data, ref.size);
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

					SetString::iterator it;
					bool inserted;
					res.emplace(ref, it, inserted);

					if (inserted)
						it->data = string_pool.insert(ref.data, ref.size);
				}
			}
			else
				throw Exception("Illegal type of column when creating set with string key: " + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
		}
		else if (type == HASHED)
		{
			SetHashed & res = hashed;
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

				res.insert(key_hash_union.key_hash);
			}
		}
		else if (type == GENERIC)
		{
			/// Общий способ
			SetGeneric & res = generic;

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				for (size_t j = 0; j < keys_size; ++j)
					key[j] = (*key_columns[j])[i];

				res.insert(key);
			}
		}
		else
			throw Exception("Unknown set variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
	}

	LOG_TRACE(log, "Created set");
}


void Set::execute(Block & block, const ColumnNumbers & arguments, size_t result, bool negative) const
{
	ColumnUInt8 * c_res = new ColumnUInt8;
	block.getByPosition(result).column = c_res;
	ColumnUInt8::Container_t & vec_res = c_res->getData();
	vec_res.resize(block.getByPosition(arguments[0]).column->size());

	size_t keys_size = arguments.size();
	Row key(keys_size);
	Columns key_columns(keys_size);

	/// Запоминаем столбцы, с которыми будем работать
	for (size_t i = 0; i < keys_size; ++i)
		key_columns[i] = block.getByPosition(arguments[i]).column;

	size_t rows = block.rows();

	/// Какую структуру данных для множества использовать?
	bool keys_fit_128_bits = false;
	Sizes key_sizes;
	if (type != chooseMethod(key_columns, keys_fit_128_bits, key_sizes))
		throw Exception("Incompatible columns in IN section", ErrorCodes::INCOMPATIBLE_COLUMNS);

	if (type == KEY_64)
	{
		const SetUInt64 & set = key64;
		const FieldVisitorToUInt64 visitor;
		IColumn & column = *key_columns[0];

		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			/// Строим ключ
			Field field = column[i];
			UInt64 key = boost::apply_visitor(visitor, field);
			vec_res[i] = negative ^ (set.end() != set.find(key));
		}
	}
	else if (type == KEY_STRING)
	{
		const SetString & set = key_string;
		IColumn & column = *key_columns[0];

		if (const ColumnString * column_string = dynamic_cast<const ColumnString *>(&column))
		{
			const ColumnString::Offsets_t & offsets = column_string->getOffsets();
			const ColumnUInt8::Container_t & data = dynamic_cast<const ColumnUInt8 &>(column_string->getData()).getData();

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				StringRef ref(&data[i == 0 ? 0 : offsets[i - 1]], (i == 0 ? offsets[i] : (offsets[i] - offsets[i - 1])) - 1);
				vec_res[i] = negative ^ (set.end() != set.find(ref));
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
				vec_res[i] = negative ^ (set.end() != set.find(ref));
			}
		}
		else
			throw Exception("Illegal type of column when creating set with string key: " + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
	}
	else if (type == HASHED)
	{
		const SetHashed & set = hashed;
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

			vec_res[i] = negative ^ (set.end() != set.find(key_hash_union.key_hash));
		}
	}
	else if (type == GENERIC)
	{
		/// Общий способ
		const SetGeneric & set = generic;

		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			/// Строим ключ
			for (size_t j = 0; j < keys_size; ++j)
				key[j] = (*key_columns[j])[i];

			vec_res[i] = negative ^ (set.end() != set.find(key));
		}
	}
	else
		throw Exception("Unknown set variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
	
	/* TODO */
}

}
