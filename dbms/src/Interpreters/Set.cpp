#include <iomanip>

#include <DB/Core/Field.h>

#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/OneBlockInputStream.h>

#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTLiteral.h>

#include <DB/Interpreters/Set.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>


namespace DB
{
	
size_t Set::getTotalRowCount() const
{
	size_t rows = 0;
	rows += key64.size();
	rows += key_string.size();
	rows += hashed.size();
	return rows;
}


size_t Set::getTotalByteCount() const
{
	size_t bytes = 0;
	bytes += key64.getBufferSizeInBytes();
	bytes += key_string.getBufferSizeInBytes();
	bytes += hashed.getBufferSizeInBytes();
	bytes += string_pool.size();
	return bytes;
}
	
	
bool Set::checkSetSizeLimits() const
{
	if (max_rows && getTotalRowCount() > max_rows)
		return false;
	if (max_bytes && getTotalByteCount() > max_bytes)
		return false;
	return true;
}
	

Set::Type Set::chooseMethod(const ConstColumnPlainPtrs & key_columns, bool & keys_fit_128_bits, Sizes & key_sizes)
{
	size_t keys_size = key_columns.size();

	keys_fit_128_bits = true;
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

	/// Если есть один числовой ключ, который помещается в 64 бита
	if (keys_size == 1 && key_columns[0]->isNumeric())
		return KEY_64;

	/// Если есть один строковый ключ, то используем хэш-таблицу с ним
	if (keys_size == 1
		&& (dynamic_cast<const ColumnString *>(key_columns[0])
		|| dynamic_cast<const ColumnConstString *>(key_columns[0])
		|| (dynamic_cast<const ColumnFixedString *>(key_columns[0]) && !keys_fit_128_bits)))
		return KEY_STRING;

	/// Если много ключей - будем строить множество хэшей от них
	return HASHED;
}


void Set::create(BlockInputStreamPtr stream)
{
	LOG_TRACE(log, "Creating set");
	Stopwatch watch;
	size_t entries = 0;
	
	/// Читаем все данные
	while (Block block = stream->read())
	{
		size_t keys_size = block.columns();
		Row key(keys_size);
		ConstColumnPlainPtrs key_columns(keys_size);
		data_types.resize(keys_size);
		
		/// Запоминаем столбцы, с которыми будем работать
		for (size_t i = 0; i < keys_size; ++i)
		{
			key_columns[i] = block.getByPosition(i).column;
			data_types[i] = block.getByPosition(i).type;
		}

		size_t rows = block.rows();

		/// Какую структуру данных для множества использовать?
		keys_fit_128_bits = false;
		type = chooseMethod(key_columns, keys_fit_128_bits, key_sizes);

		if (type == KEY_64)
		{
			SetUInt64 & res = key64;
			const IColumn & column = *key_columns[0];

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
 				UInt64 key = get<UInt64>(column[i]);
				res.insert(key);
			}

			entries = res.size();
		}
		else if (type == KEY_STRING)
		{
			SetString & res = key_string;
			const IColumn & column = *key_columns[0];

			if (const ColumnString * column_string = dynamic_cast<const ColumnString *>(&column))
			{
				const ColumnString::Offsets_t & offsets = column_string->getOffsets();
				const ColumnString::Chars_t & data = column_string->getChars();

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
				const ColumnFixedString::Chars_t & data = column_string->getChars();

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

			entries = res.size();
		}
		else if (type == HASHED)
		{
			SetHashed & res = hashed;

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
				res.insert(keys_fit_128_bits ? pack128(i, keys_size, key_columns, key_sizes) : hash128(i, keys_size, key_columns));

			entries = res.size();
		}
		else
			throw Exception("Unknown set variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
		
		if (!checkSetSizeLimits())
		{
			if (overflow_mode == Limits::THROW)
					throw Exception("IN-Set size exceeded."
						" Rows: " + toString(getTotalRowCount()) +
						", limit: " + toString(max_rows) +
						". Bytes: " + toString(getTotalByteCount()) +
						", limit: " + toString(max_bytes) + ".",
						ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);

			if (overflow_mode == Limits::BREAK)
			{
				if (IProfilingBlockInputStream * profiling_in = dynamic_cast<IProfilingBlockInputStream *>(&*stream))
					profiling_in->cancel();
				break;
			}

			throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
		}
	}

	logProfileInfo(watch, *stream, entries);
}


void Set::logProfileInfo(Stopwatch & watch, IBlockInputStream & in, size_t entries)
{
	/// Выведем информацию о том, сколько считано строк и байт.
	size_t rows = 0;
	size_t bytes = 0;

	in.getLeafRowsBytes(rows, bytes);

	size_t head_rows = 0;
	if (IProfilingBlockInputStream * profiling_in = dynamic_cast<IProfilingBlockInputStream *>(&in))
		head_rows = profiling_in->getInfo().rows;

	if (rows != 0)
	{
		LOG_DEBUG(log, std::fixed << std::setprecision(3)
			<< "Created set with " << entries << " entries from " << head_rows << " rows."
			<< " Read " << rows << " rows, " << bytes / 1048576.0 << " MiB in " << watch.elapsedSeconds() << " sec., "
			<< static_cast<size_t>(rows / watch.elapsedSeconds()) << " rows/sec., " << bytes / 1048576.0 / watch.elapsedSeconds() << " MiB/sec.");
	}
}


void Set::create(DataTypes & types, ASTPtr node)
{
	data_types = types;

	/// Засунем множество в блок.
	Block block;
	for (size_t i = 0, size = data_types.size(); i < size; ++i)
	{
		ColumnWithNameAndType col;
		col.type = data_types[i];
		col.column = data_types[i]->createColumn();
		col.name = "_" + toString(i);

		block.insert(col);
	}

	ASTExpressionList & list = dynamic_cast<ASTExpressionList &>(*node);
	for (ASTs::iterator it = list.children.begin(); it != list.children.end(); ++it)
	{
		if (data_types.size() == 1)
		{
			if (ASTLiteral * lit = dynamic_cast<ASTLiteral *>(&**it))
				block.getByPosition(0).column->insert(lit->value);
			else
				throw Exception("Incorrect element of set. Must be literal.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
		}
		else if (ASTFunction * func = dynamic_cast<ASTFunction *>(&**it))
		{
			if (func->name != "tuple")
				throw Exception("Incorrect element of set. Must be tuple.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);

			size_t tuple_size = func->arguments->children.size();
			if (tuple_size != data_types.size())
				throw Exception("Incorrect size of tuple in set.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
			
			for (size_t j = 0; j < tuple_size; ++j)
			{
				if (ASTLiteral * lit = dynamic_cast<ASTLiteral *>(&*func->arguments->children[j]))
					block.getByPosition(j).column->insert(lit->value);
				else
					throw Exception("Incorrect element of tuple in set. Must be literal.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
			}
		}
		else
			throw Exception("Incorrect element of set", ErrorCodes::INCORRECT_ELEMENT_OF_SET);

		/// NOTE: Потом можно реализовать возможность задавать константные выражения в множествах.
	}

	create(new OneBlockInputStream(block));
}


void Set::execute(Block & block, const ColumnNumbers & arguments, size_t result, bool negative) const
{
	ColumnUInt8 * c_res = new ColumnUInt8;
	block.getByPosition(result).column = c_res;
	ColumnUInt8::Container_t & vec_res = c_res->getData();
	vec_res.resize(block.getByPosition(arguments[0]).column->size());

	/// Если множество пусто
	if (data_types.empty())
	{
		if (negative)
			memset(&vec_res[0], 1, vec_res.size());
		return;
	}
	
	DataTypeArray * array_type = dynamic_cast<DataTypeArray *>(&*block.getByPosition(arguments[0]).type);
	
	if (array_type)
	{
		if (data_types.size() != 1 || arguments.size() != 1)
			throw Exception("Number of columns in section IN doesn't match.", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);
		if (array_type->getNestedType()->getName() != data_types[0]->getName())
			throw Exception(std::string() + "Types in section IN don't match: " + data_types[0]->getName() + " on the right, " + array_type->getNestedType()->getName() + " on the left.", ErrorCodes::TYPE_MISMATCH);
		
		IColumn * in_column = &*block.getByPosition(arguments[0]).column;
		if (ColumnConstArray * col = dynamic_cast<ColumnConstArray *>(in_column))
			executeConstArray(col, vec_res, negative);
		else if (ColumnArray * col = dynamic_cast<ColumnArray *>(in_column))
			executeArray(col, vec_res, negative);
		else
			throw Exception("Unexpeced array column type: " + in_column->getName(), ErrorCodes::ILLEGAL_COLUMN);
	}
	else
	{
		if (data_types.size() != arguments.size())
			throw Exception("Number of columns in section IN doesn't match.", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);
		
		/// Запоминаем столбцы, с которыми будем работать. Также проверим, что типы данных правильные.
		ConstColumnPlainPtrs key_columns(arguments.size());
		for (size_t i = 0; i < arguments.size(); ++i)
		{
			key_columns[i] = block.getByPosition(arguments[i]).column;
			
			if (data_types[i]->getName() != block.getByPosition(arguments[i]).type->getName())
				throw Exception("Types of column " + toString(i + 1) + " in section IN don't match: " + data_types[i]->getName() + " on the right, " + block.getByPosition(arguments[i]).type->getName() + " on the left.", ErrorCodes::TYPE_MISMATCH);
		}
		
		executeOrdinary(key_columns, vec_res, negative);
	}
}

void Set::executeOrdinary(const ConstColumnPlainPtrs & key_columns, ColumnUInt8::Container_t & vec_res, bool negative) const
{
	size_t keys_size = data_types.size();
	size_t rows = key_columns[0]->size();
	Row key(keys_size);

	if (type == KEY_64)
	{
		const SetUInt64 & set = key64;
		const IColumn & column = *key_columns[0];
		
		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			/// Строим ключ
			UInt64 key = get<UInt64>(column[i]);
			vec_res[i] = negative ^ (set.end() != set.find(key));
		}
	}
	else if (type == KEY_STRING)
	{
		const SetString & set = key_string;
		const IColumn & column = *key_columns[0];
		
		if (const ColumnString * column_string = dynamic_cast<const ColumnString *>(&column))
		{
			const ColumnString::Offsets_t & offsets = column_string->getOffsets();
			const ColumnString::Chars_t & data = column_string->getChars();
			
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
			const ColumnFixedString::Chars_t & data = column_string->getChars();
			
			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				StringRef ref(&data[i * n], n);
				vec_res[i] = negative ^ (set.end() != set.find(ref));
			}
		}
		else if (const ColumnConstString * column_string = dynamic_cast<const ColumnConstString *>(&column))
		{
			bool res = negative ^ (set.end() != set.find(StringRef(column_string->getData())));
			
			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
				vec_res[i] = res;
		}
		else
			throw Exception("Illegal type of column when creating set with string key: " + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
	}
	else if (type == HASHED)
	{
		const SetHashed & set = hashed;
		
		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
			vec_res[i] = negative ^ (set.end() != set.find(keys_fit_128_bits ? pack128(i, keys_size, key_columns, key_sizes) : hash128(i, keys_size, key_columns)));
	}
	else
		throw Exception("Unknown set variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
}

void Set::executeArray(const ColumnArray * key_column, ColumnUInt8::Container_t & vec_res, bool negative) const
{
	size_t rows = key_column->size();
	const ColumnArray::Offsets_t & offsets = key_column->getOffsets();
	const IColumn & nested_column = key_column->getData();
	
	if (type == KEY_64)
	{
		const SetUInt64 & set = key64;

		size_t prev_offset = 0;
		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			UInt8 res = 0;
			/// Для всех элементов
			for (size_t j = prev_offset; j < offsets[i]; ++j)
			{
				/// Строим ключ
				UInt64 key = get<UInt64>(nested_column[j]);
				res |= negative ^ (set.end() != set.find(key));
				if (res)
					break;
			}
			vec_res[i] = res;
			prev_offset = offsets[i];
		}
	}
	else if (type == KEY_STRING)
	{
		const SetString & set = key_string;
		
		if (const ColumnString * column_string = dynamic_cast<const ColumnString *>(&nested_column))
		{
			const ColumnString::Offsets_t & nested_offsets = column_string->getOffsets();
			const ColumnString::Chars_t & data = column_string->getChars();
			
			size_t prev_offset = 0;
			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				UInt8 res = 0;
				/// Для всех элементов
				for (size_t j = prev_offset; j < offsets[i]; ++j)
				{
					/// Строим ключ
					size_t begin = j == 0 ? 0 : nested_offsets[j - 1];
					size_t end = nested_offsets[j];
					StringRef ref(&data[begin], end - begin - 1);
					res |= negative ^ (set.end() != set.find(ref));
					if (res)
						break;
				}
				vec_res[i] = res;
				prev_offset = offsets[i];
			}
		}
		else if (const ColumnFixedString * column_string = dynamic_cast<const ColumnFixedString *>(&nested_column))
		{
			size_t n = column_string->getN();
			const ColumnFixedString::Chars_t & data = column_string->getChars();
			
			size_t prev_offset = 0;
			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				UInt8 res = 0;
				/// Для всех элементов
				for (size_t j = prev_offset; j < offsets[i]; ++j)
				{
					/// Строим ключ
					StringRef ref(&data[j * n], n);
					res |= negative ^ (set.end() != set.find(ref));
					if (res)
						break;
				}
				vec_res[i] = res;
				prev_offset = offsets[i];
			}
		}
		else
			throw Exception("Illegal type of column when looking for Array(String) key: " + nested_column.getName(), ErrorCodes::ILLEGAL_COLUMN);
	}
	else if (type == HASHED)
	{
		const SetHashed & set = hashed;
		ConstColumnPlainPtrs nested_columns(1, &nested_column);
		
		size_t prev_offset = 0;
		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			UInt8 res = 0;
			/// Для всех элементов
			for (size_t j = prev_offset; j < offsets[i]; ++j)
			{
				/// Строим ключ
				res |= negative ^ (set.end() != set.find(keys_fit_128_bits ? pack128(i, 1, nested_columns, key_sizes) : hash128(i, 1, nested_columns)));
				if (res)
					break;
			}
			vec_res[i] = res;
			prev_offset = offsets[i];
		}
	}
	else
		throw Exception("Unknown set variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
}

void Set::executeConstArray(const ColumnConstArray * key_column, ColumnUInt8::Container_t & vec_res, bool negative) const
{
	if (type == HASHED)
	{
		ColumnPtr full_column = key_column->convertToFullColumn();
		executeArray(dynamic_cast<ColumnArray *>(&*full_column), vec_res, negative);
		return;
	}
	
	size_t rows = key_column->size();
	Array values = key_column->getData();
	UInt8 res = 0;
	
	/// Для всех элементов
	for (size_t j = 0; j < values.size(); ++j)
	{
		if (type == KEY_64)
		{
			const SetUInt64 & set = key64;
			UInt64 key = get<UInt64>(values[j]);
			res |= negative ^ (set.end() != set.find(key));
		}
		else if (type == KEY_STRING)
		{
			const SetString & set = key_string;
			res |= negative ^ (set.end() != set.find(StringRef(get<String>(values[j]))));
		}
		else
			throw Exception("Unknown set variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);

		if (res)
			break;
	}
	
	/// Для всех строчек
	for (size_t i = 0; i < rows; ++i)
		vec_res[i] = res;
}

}
