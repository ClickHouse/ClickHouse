#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>

#include <DB/Parsers/ASTJoin.h>
#include <DB/Interpreters/Join.h>


namespace DB
{


template <typename Maps>
static void initImpl(Maps & maps, Set::Type type)
{
	switch (type)
	{
		case Set::EMPTY:																break;
		case Set::KEY_64:		maps.key64		.reset(new typename Maps::MapUInt64); 	break;
		case Set::KEY_STRING:	maps.key_string	.reset(new typename Maps::MapString); 	break;
		case Set::HASHED:		maps.hashed		.reset(new typename Maps::MapHashed);	break;

		default:
			throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
	}
}

template <typename Maps>
static size_t getTotalRowCountImpl(const Maps & maps)
{
	size_t rows = 0;
	if (maps.key64)
		rows += maps.key64->size();
	if (maps.key_string)
		rows += maps.key_string->size();
	if (maps.hashed)
		rows += maps.hashed->size();
	return rows;
}

template <typename Maps>
static size_t getTotalByteCountImpl(const Maps & maps)
{
	size_t bytes = 0;
	if (maps.key64)
		bytes += maps.key64->getBufferSizeInBytes();
	if (maps.key_string)
		bytes += maps.key_string->getBufferSizeInBytes();
	if (maps.hashed)
		bytes += maps.hashed->getBufferSizeInBytes();
	return bytes;
}


void Join::init(Set::Type type_)
{
	type = type_;

	if (strictness == ASTJoin::Any)
		initImpl(maps_any, type);
	else
		initImpl(maps_all, type);
}

size_t Join::getTotalRowCount() const
{
	if (strictness == ASTJoin::Any)
		return getTotalRowCountImpl(maps_any);
	else
		return getTotalRowCountImpl(maps_all);
}

size_t Join::getTotalByteCount() const
{
	size_t bytes;

	if (strictness == ASTJoin::Any)
		bytes = getTotalByteCountImpl(maps_any);
	else
		bytes = getTotalByteCountImpl(maps_all);

	return bytes + pool.size();
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


/// Вставка элемента в хэш-таблицу вида ключ -> ссылка на строку, которая затем будет использоваться при JOIN-е.
template <ASTJoin::Strictness STRICTNESS, typename Map>
struct Inserter
{
	static void insert(Map & map, const typename Map::key_type & key, Block * stored_block, size_t i, Arena & pool);
};

template <typename Map>
struct Inserter<ASTJoin::Any, Map>
{
	static void insert(Map & map, const typename Map::key_type & key, Block * stored_block, size_t i, Arena & pool)
	{
		typename Map::iterator it;
		bool inserted;
		map.emplace(key, it, inserted);

		if (inserted)
			new (&it->second) Join::RowRef(stored_block, i);
	}
};

/// Для строковых ключей отличается тем, что саму строчку надо разместить в пуле.
template <>
struct Inserter<ASTJoin::Any, Join::MapsAny::MapString>
{
	static void insert(Join::MapsAny::MapString & map, const Join::MapsAny::MapString::key_type & key, Block * stored_block, size_t i, Arena & pool)
	{
		Join::MapsAny::MapString::iterator it;
		bool inserted;
		map.emplace(key, it, inserted);

		if (inserted)
		{
			it->first.data = pool.insert(key.data, key.size);
			new (&it->second) Join::RowRef(stored_block, i);
		}
	}
};

template <typename Map>
struct Inserter<ASTJoin::All, Map>
{
	static void insert(Map & map, const typename Map::key_type & key, Block * stored_block, size_t i, Arena & pool)
	{
		typename Map::iterator it;
		bool inserted;
		map.emplace(key, it, inserted);

		if (inserted)
		{
			new (&it->second) Join::RowRefList(stored_block, i);
		}
		else
		{
			/** Первый элемент списка хранится в значении хэш-таблицы, остальные - в pool-е.
			  * Мы будем вставлять каждый раз элемент на место второго.
			  * То есть, бывший второй элемент, если он был, станет третьим, и т. п.
			  */
			Join::RowRefList * elem = reinterpret_cast<Join::RowRefList *>(pool.alloc(sizeof(Join::RowRefList)));

			it->second.next = elem;
			elem->next = it->second.next;
			elem->block = stored_block;
			elem->row_num = i;
		}
	}
};

template <>
struct Inserter<ASTJoin::All, Join::MapsAll::MapString>
{
	static void insert(Join::MapsAll::MapString & map, const Join::MapsAll::MapString::key_type & key, Block * stored_block, size_t i, Arena & pool)
	{
		typename Join::MapsAll::MapString::iterator it;
		bool inserted;
		map.emplace(key, it, inserted);

		if (inserted)
		{
			it->first.data = pool.insert(key.data, key.size);
			new (&it->second) Join::RowRefList(stored_block, i);
		}
		else
		{
			Join::RowRefList * elem = reinterpret_cast<Join::RowRefList *>(pool.alloc(sizeof(Join::RowRefList)));

			it->second.next = elem;
			elem->next = it->second.next;
			elem->block = stored_block;
			elem->row_num = i;
		}
	}
};


template <ASTJoin::Strictness STRICTNESS, typename Maps>
void Join::insertFromBlockImpl(Maps & maps, size_t rows, const ConstColumnPlainPtrs & key_columns, size_t keys_size, Block * stored_block)
{
	if (type == Set::KEY_64)
	{
		typedef typename Maps::MapUInt64 Map;
		Map & res = *maps.key64;
		const IColumn & column = *key_columns[0];

		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			/// Строим ключ
			UInt64 key = column.get64(i);
			Inserter<STRICTNESS, Map>::insert(res, key, stored_block, i, pool);
		}
	}
	else if (type == Set::KEY_STRING)
	{
		typedef typename Maps::MapString Map;
		Map & res = *maps.key_string;
		const IColumn & column = *key_columns[0];

		if (const ColumnString * column_string = dynamic_cast<const ColumnString *>(&column))
		{
			const ColumnString::Offsets_t & offsets = column_string->getOffsets();
			const ColumnString::Chars_t & data = column_string->getChars();

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				StringRef key(&data[i == 0 ? 0 : offsets[i - 1]], (i == 0 ? offsets[i] : (offsets[i] - offsets[i - 1])) - 1);
				Inserter<STRICTNESS, Map>::insert(res, key, stored_block, i, pool);
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
				StringRef key(&data[i * n], n);
				Inserter<STRICTNESS, Map>::insert(res, key, stored_block, i, pool);
			}
		}
		else
			throw Exception("Illegal type of column when creating set with string key: " + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
	}
	else if (type == Set::HASHED)
	{
		typedef typename Maps::MapHashed Map;
		Map & res = *maps.hashed;

		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			UInt128 key = keys_fit_128_bits
				? pack128(i, keys_size, key_columns, key_sizes)
				: hash128(i, keys_size, key_columns);

			Inserter<STRICTNESS, Map>::insert(res, key, stored_block, i, pool);
		}
	}
	else
		throw Exception("Unknown JOIN variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
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

	if (strictness == ASTJoin::Any)
		insertFromBlockImpl<ASTJoin::Any, MapsAny>(maps_any, rows, key_columns, keys_size, stored_block);
	else
		insertFromBlockImpl<ASTJoin::All, MapsAll>(maps_all, rows, key_columns, keys_size, stored_block);

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


template <typename Map>
static void addAnyLeftRow(
	const Map & map,
	const typename Map::key_type & key,
	size_t num_columns_to_add,
	ColumnPlainPtrs & added_columns)
{
	typename Map::const_iterator it = map.find(key);

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

template <typename Map>
static void addAnyInnerRow(
	const Map & map,
	const typename Map::key_type & key,
	size_t num_columns_to_add,
	ColumnPlainPtrs & added_columns,
	size_t i,
	IColumn::Filter & filter)
{
	typename Map::const_iterator it = map.find(key);

	if (it != map.end())
	{
		filter[i] = 1;

		for (size_t j = 0; j < num_columns_to_add; ++j)
			added_columns[j]->insertFrom(*it->second.block->unsafeGetByPosition(j).column.get(), it->second.row_num);
	}
	else
		filter[i] = 0;
}

template <ASTJoin::Kind KIND, typename Map>
static void addAnyRow(
	const Map & map,
	const typename Map::key_type & key,
	size_t num_columns_to_add,
	ColumnPlainPtrs & added_columns,
	size_t i,
	IColumn::Filter * filter)
{
	if (KIND == ASTJoin::Left)
		addAnyLeftRow(map, key, num_columns_to_add, added_columns);
	else
		addAnyInnerRow(map, key, num_columns_to_add, added_columns, i, *filter);
}


template <ASTJoin::Kind KIND>
void Join::anyJoinBlock(Block & block)
{
	if (blocks.empty())
		throw Exception("Attempt to JOIN with empty table", ErrorCodes::EMPTY_DATA_PASSED);

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

	size_t existing_columns = block.columns();

	for (size_t i = 0; i < num_columns_to_add; ++i)
	{
		const ColumnWithNameAndType & src_column = first_mapped_block.getByPosition(i);
		ColumnWithNameAndType new_column = src_column.cloneEmpty();
		block.insert(new_column);
		added_columns[i] = new_column.column;
		added_columns[i]->reserve(src_column.column->size());
	}

	size_t rows = block.rowsInFirstColumn();

	/// Используется при ANY INNER JOIN
	std::unique_ptr<IColumn::Filter> filter;

	if (kind == ASTJoin::Inner && strictness == ASTJoin::Any)
		filter.reset(new IColumn::Filter(rows));

	if (type == Set::KEY_64)
	{
		typedef MapsAny::MapUInt64 Map;
		const Map & map = *maps_any.key64;
		const IColumn & column = *key_columns[0];

		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			/// Строим ключ
			UInt64 key = column.get64(i);
			addAnyRow<KIND, Map>(map, key, num_columns_to_add, added_columns, i, filter.get());
		}
	}
	else if (type == Set::KEY_STRING)
	{
		typedef MapsAny::MapString Map;
		const Map & map = *maps_any.key_string;
		const IColumn & column = *key_columns[0];

		if (const ColumnString * column_string = dynamic_cast<const ColumnString *>(&column))
		{
			const ColumnString::Offsets_t & offsets = column_string->getOffsets();
			const ColumnString::Chars_t & data = column_string->getChars();

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				StringRef key(&data[i == 0 ? 0 : offsets[i - 1]], (i == 0 ? offsets[i] : (offsets[i] - offsets[i - 1])) - 1);
				addAnyRow<KIND, Map>(map, key, num_columns_to_add, added_columns, i, filter.get());
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
				StringRef key(&data[i * n], n);
				addAnyRow<KIND, Map>(map, key, num_columns_to_add, added_columns, i, filter.get());
			}
		}
		else
			throw Exception("Illegal type of column when creating set with string key: " + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
	}
	else if (type == Set::HASHED)
	{
		typedef MapsAny::MapHashed Map;
		Map & map = *maps_any.hashed;

		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			UInt128 key = keys_fit_128_bits
				? pack128(i, keys_size, key_columns, key_sizes)
				: hash128(i, keys_size, key_columns);

			addAnyRow<KIND, Map>(map, key, num_columns_to_add, added_columns, i, filter.get());
		}
	}
	else
		throw Exception("Unknown JOIN variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);

	/// Если ANY INNER JOIN - фильтруем все столбцы кроме новых.
	if (kind == ASTJoin::Inner && strictness == ASTJoin::Any)
		for (size_t i = 0; i < existing_columns; ++i)
			block.getByPosition(i).column = block.getByPosition(i).column->filter(*filter);
}


void Join::joinBlock(Block & block)
{
	if (kind == ASTJoin::Left)
		anyJoinBlock<ASTJoin::Left>(block);
	else
		anyJoinBlock<ASTJoin::Inner>(block);
}


}
