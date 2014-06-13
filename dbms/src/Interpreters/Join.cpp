#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>

#include <DB/Interpreters/Join.h>


namespace DB
{

size_t Join::getTotalRowCount() const
{
	size_t rows = 0;
	if (key64)
		rows += key64->size();
	if (key_string)
		rows += key_string->size();
	if (hashed)
		rows += hashed->size();
	return rows;
}


size_t Join::getTotalByteCount() const
{
	size_t bytes = 0;
	if (key64)
		bytes += key64->getBufferSizeInBytes();
	if (key_string)
		bytes += key_string->getBufferSizeInBytes();
	if (hashed)
		bytes += hashed->getBufferSizeInBytes();
	bytes += pool.size();
	return bytes;
}


bool Join::checkSizeLimits() const
{
	if (max_rows && getTotalRowCount() > max_rows)
		return false;
	if (max_bytes && getTotalByteCount() > max_bytes)
		return false;
	return true;
}


bool Join::checkExternalSizeLimits() const
{
	if (max_rows_to_transfer && rows_in_external_table > max_rows_to_transfer)
		return false;
	if (max_bytes_to_transfer && bytes_in_external_table > max_bytes_to_transfer)
		return false;
	return true;
}


bool Join::insertFromBlock(const Block & block)
{
	if (external_table)
	{
		BlockOutputStreamPtr output = external_table->write(ASTPtr());
		output->write(block);
		bytes_in_external_table += block.bytes();
		rows_in_external_table += block.rows();

		if (!checkExternalSizeLimits())
		{
			if (transfer_overflow_mode == OverflowMode::THROW)
				throw Exception("JOIN external table size limit exceeded."
					" Rows: " + toString(rows_in_external_table) +
					", limit: " + toString(max_rows_to_transfer) +
					". Bytes: " + toString(bytes_in_external_table) +
					", limit: " + toString(max_bytes_to_transfer) + ".",
					ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);

			if (transfer_overflow_mode == OverflowMode::BREAK)
				return false;

			throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
		}
	}

	if (only_external)
		return true;

	size_t keys_size = key_names.size();
	ConstColumnPlainPtrs key_columns(keys_size);

	/// Переводим имена столбцов в номера, если они ещё не вычислены.
	if (key_numbers_right.empty())
		for (const auto & name : key_names)
			key_numbers_right.push_back(block.getPositionByName(name));

	/// Запоминаем столбцы ключей, с которыми будем работать
	for (size_t i = 0; i < keys_size; ++i)
		key_columns[i] = block.getByPosition(key_numbers_right[i]).column;

	size_t rows = block.rows();

	/// Какую структуру данных для множества использовать?
	keys_fit_128_bits = false;

	if (empty())
		init(Set::chooseMethod(key_columns, keys_fit_128_bits, key_sizes));

	blocks.push_back(block);
	Block * stored_block = &blocks.back();

	/// Удаляем из stored_block ключевые столбцы, так как они не нужны.
	for (const auto & name : key_names)
		stored_block->erase(stored_block->getPositionByName(name));


	if (type == Set::KEY_64)
	{
		MapUInt64 & res = *key64;
		const IColumn & column = *key_columns[0];

		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			/// Строим ключ
			UInt64 key = column.get64(i);

			MapUInt64::iterator it;
			bool inserted;
			res.emplace(key, it, inserted);

			if (inserted)
				new (&it->second) RowRef(stored_block, i);
		}
	}
	else if (type == Set::KEY_STRING)
	{
		MapString & res = *key_string;
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

				MapString::iterator it;
				bool inserted;
				res.emplace(ref, it, inserted);

				if (inserted)
				{
					it->first.data = pool.insert(ref.data, ref.size);
					new (&it->second) RowRef(stored_block, i);
				}
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

				MapString::iterator it;
				bool inserted;
				res.emplace(ref, it, inserted);

				if (inserted)
				{
					it->first.data = pool.insert(ref.data, ref.size);
					new (&it->second) RowRef(stored_block, i);
				}
			}
		}
		else
			throw Exception("Illegal type of column when creating set with string key: " + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
	}
	else if (type == Set::HASHED)
	{
		MapHashed & res = *hashed;

		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			UInt128 key = keys_fit_128_bits
				? pack128(i, keys_size, key_columns, key_sizes)
				: hash128(i, keys_size, key_columns);

			MapHashed::iterator it;
			bool inserted;
			res.emplace(key, it, inserted);

			if (inserted)
				new (&it->second) RowRef(stored_block, i);
		}
	}
	else
		throw Exception("Unknown JOIN variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);

	if (!checkSizeLimits())
	{
		if (overflow_mode == OverflowMode::THROW)
			throw Exception("Join size limit exceeded."
				" Rows: " + toString(getTotalRowCount()) +
				", limit: " + toString(max_rows) +
				". Bytes: " + toString(getTotalByteCount()) +
				", limit: " + toString(max_bytes) + ".",
				ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);

		if (overflow_mode == OverflowMode::BREAK)
			return false;

		throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
	}

	return true;
}


void Join::anyLeftJoinBlock(Block & block)
{
	if (blocks.empty())
		throw Exception("Attempt to JOIN with empty table", ErrorCodes::EMPTY_DATA_PASSED);

	std::cerr << "!!! " << block.dumpNames() << std::endl;

	size_t keys_size = key_names.size();
	ConstColumnPlainPtrs key_columns(keys_size);

	/// Переводим имена столбцов в номера, если они ещё не вычислены.
	if (key_numbers_left.empty())
		for (const auto & name : key_names)
			key_numbers_left.push_back(block.getPositionByName(name));

	/// Запоминаем столбцы ключей, с которыми будем работать
	for (size_t i = 0; i < keys_size; ++i)
		key_columns[i] = block.getByPosition(key_numbers_left[i]).column;

	/// Добавляем в блок новые столбцы.
	const Block & first_mapped_block = blocks.front();
	size_t num_columns_to_add = first_mapped_block.columns();
	ColumnPlainPtrs added_columns(num_columns_to_add);

	for (size_t i = 0; i < num_columns_to_add; ++i)
	{
		const ColumnWithNameAndType & src_column = first_mapped_block.getByPosition(i);
		ColumnWithNameAndType new_column = src_column.cloneEmpty();
		block.insert(new_column);
		added_columns[i] = new_column.column;
		added_columns[i]->reserve(src_column.column->size());
	}

	std::cerr << "??? " << block.dumpNames() << std::endl;

	size_t rows = block.rowsInFirstColumn();

	if (type == Set::KEY_64)
	{
		const MapUInt64 & map = *key64;
		const IColumn & column = *key_columns[0];

		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			/// Строим ключ
			UInt64 key = column.get64(i);

			MapUInt64::const_iterator it = map.find(key);

			if (it != map.end())
			{
				for (size_t j = 0; j < num_columns_to_add; ++j)
					added_columns[j]->insertFrom(*it->second.block->unsafeGetByPosition(j).column.get(), it->second.row_num);
			}
			else
			{
				for (size_t j = 0; j < num_columns_to_add; ++j)
					added_columns[j]->insertDefault();
			}
		}
	}
	else
		throw Exception("Unknown JOIN variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
}


}
