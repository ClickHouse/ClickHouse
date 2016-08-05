#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>

#include <DB/Interpreters/Join.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Core/ColumnNumbers.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int UNKNOWN_SET_DATA_VARIANT;
	extern const int LOGICAL_ERROR;
	extern const int SET_SIZE_LIMIT_EXCEEDED;
	extern const int TYPE_MISMATCH;
	extern const int ILLEGAL_COLUMN;
}


Join::Type Join::chooseMethod(const ConstColumnPlainPtrs & key_columns, bool & keys_fit_128_bits, Sizes & key_sizes)
{
	size_t keys_size = key_columns.size();

	keys_fit_128_bits = true;
	size_t keys_bytes = 0;
	key_sizes.resize(keys_size);

	if (keys_size == 0)
		return Type::CROSS;

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
		return Type::KEY_64;

	/// Если есть один строковый ключ, то используем хэш-таблицу с ним
	if (keys_size == 1
		&& (typeid_cast<const ColumnString *>(key_columns[0])
			|| typeid_cast<const ColumnConstString *>(key_columns[0])
			|| (typeid_cast<const ColumnFixedString *>(key_columns[0]) && !keys_fit_128_bits)))
		return Type::KEY_STRING;

	/// Если много ключей - будем строить множество хэшей от них
	return Type::HASHED;
}


template <typename Maps>
static void initImpl(Maps & maps, Join::Type type)
{
	switch (type)
	{
		case Join::Type::EMPTY:																	break;
		case Join::Type::KEY_64:		maps.key64		.reset(new typename Maps::MapUInt64); 	break;
		case Join::Type::KEY_STRING:	maps.key_string	.reset(new typename Maps::MapString); 	break;
		case Join::Type::HASHED:		maps.hashed		.reset(new typename Maps::MapHashed);	break;
		case Join::Type::CROSS:																	break;

		default:
			throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
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


/// Нужно ли использовать хэш-таблицы maps_*_full, в которых запоминается, была ли строчка присоединена.
static bool getFullness(ASTTableJoin::Kind kind)
{
	return kind == ASTTableJoin::Kind::Right || kind == ASTTableJoin::Kind::Full;
}


void Join::init(Type type_)
{
	type = type_;

	if (kind == ASTTableJoin::Kind::Cross)
		return;

	if (!getFullness(kind))
	{
		if (strictness == ASTTableJoin::Strictness::Any)
			initImpl(maps_any, type);
		else
			initImpl(maps_all, type);
	}
	else
	{
		if (strictness == ASTTableJoin::Strictness::Any)
			initImpl(maps_any_full, type);
		else
			initImpl(maps_all_full, type);
	}
}

size_t Join::getTotalRowCount() const
{
	size_t res = 0;

	if (type == Type::CROSS)
	{
		for (const auto & block : blocks)
			res += block.rowsInFirstColumn();
	}
	else
	{
		res += getTotalRowCountImpl(maps_any);
		res += getTotalRowCountImpl(maps_all);
		res += getTotalRowCountImpl(maps_any_full);
		res += getTotalRowCountImpl(maps_all_full);
	}

	return res;
}

size_t Join::getTotalByteCount() const
{
	size_t res = 0;

	if (type == Type::CROSS)
	{
		for (const auto & block : blocks)
			res += block.bytes();
	}
	else
	{
		res += getTotalByteCountImpl(maps_any);
		res += getTotalByteCountImpl(maps_all);
		res += getTotalByteCountImpl(maps_any_full);
		res += getTotalByteCountImpl(maps_all_full);
		res += pool.size();
	}

	return res;
}


bool Join::checkSizeLimits() const
{
	if (max_rows && getTotalRowCount() > max_rows)
		return false;
	if (max_bytes && getTotalByteCount() > max_bytes)
		return false;
	return true;
}


/// Вставка элемента в хэш-таблицу вида ключ -> ссылка на строку, которая затем будет использоваться при JOIN-е.
template <ASTTableJoin::Strictness STRICTNESS, typename Map>
struct Inserter
{
	static void insert(Map & map, const typename Map::key_type & key, Block * stored_block, size_t i, Arena & pool);
};

template <typename Map>
struct Inserter<ASTTableJoin::Strictness::Any, Map>
{
	static void insert(Map & map, const typename Map::key_type & key, Block * stored_block, size_t i, Arena & pool)
	{
		typename Map::iterator it;
		bool inserted;
		map.emplace(key, it, inserted);

		if (inserted)
			new (&it->second) typename Map::mapped_type(stored_block, i);
	}
};

/// Для строковых ключей отличается тем, что саму строчку надо разместить в пуле.
template <typename Map>
struct InserterAnyString
{
	static void insert(Map & map, const typename Map::key_type & key, Block * stored_block, size_t i, Arena & pool)
	{
		typename Map::iterator it;
		bool inserted;
		map.emplace(key, it, inserted);

		if (inserted)
		{
			it->first.data = pool.insert(key.data, key.size);
			new (&it->second) typename Map::mapped_type(stored_block, i);
		}
	}
};

template <> struct Inserter<ASTTableJoin::Strictness::Any, Join::MapsAny::MapString> : InserterAnyString<Join::MapsAny::MapString> {};
template <> struct Inserter<ASTTableJoin::Strictness::Any, Join::MapsAnyFull::MapString> : InserterAnyString<Join::MapsAnyFull::MapString> {};


template <typename Map>
struct Inserter<ASTTableJoin::Strictness::All, Map>
{
	static void insert(Map & map, const typename Map::key_type & key, Block * stored_block, size_t i, Arena & pool)
	{
		typename Map::iterator it;
		bool inserted;
		map.emplace(key, it, inserted);

		if (inserted)
		{
			new (&it->second) typename Map::mapped_type(stored_block, i);
		}
		else
		{
			/** Первый элемент списка хранится в значении хэш-таблицы, остальные - в pool-е.
			  * Мы будем вставлять каждый раз элемент на место второго.
			  * То есть, бывший второй элемент, если он был, станет третьим, и т. п.
			  */
			auto elem = reinterpret_cast<typename Map::mapped_type *>(pool.alloc(sizeof(typename Map::mapped_type)));

			elem->next = it->second.next;
			it->second.next = elem;
			elem->block = stored_block;
			elem->row_num = i;
		}
	}
};

template <typename Map>
struct InserterAllString
{
	static void insert(Map & map, const typename Map::key_type & key, Block * stored_block, size_t i, Arena & pool)
	{
		typename Map::iterator it;
		bool inserted;
		map.emplace(key, it, inserted);

		if (inserted)
		{
			it->first.data = pool.insert(key.data, key.size);
			new (&it->second) typename Map::mapped_type(stored_block, i);
		}
		else
		{
			auto elem = reinterpret_cast<typename Map::mapped_type *>(pool.alloc(sizeof(typename Map::mapped_type)));

			elem->next = it->second.next;
			it->second.next = elem;
			elem->block = stored_block;
			elem->row_num = i;
		}
	}
};

template <> struct Inserter<ASTTableJoin::Strictness::All, Join::MapsAll::MapString> : InserterAllString<Join::MapsAll::MapString> {};
template <> struct Inserter<ASTTableJoin::Strictness::All, Join::MapsAllFull::MapString> : InserterAllString<Join::MapsAllFull::MapString> {};


template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
void Join::insertFromBlockImpl(Maps & maps, size_t rows, const ConstColumnPlainPtrs & key_columns, size_t keys_size, Block * stored_block)
{
	if (type == Type::CROSS)
	{
		/// Ничего не делаем. Уже сохранили блок, и этого достаточно.
	}
	else if (type == Type::KEY_64)
	{
		using Map = typename Maps::MapUInt64;
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
	else if (type == Type::KEY_STRING)
	{
		using Map = typename Maps::MapString;
		Map & res = *maps.key_string;
		const IColumn & column = *key_columns[0];

		if (const ColumnString * column_string = typeid_cast<const ColumnString *>(&column))
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
		else if (const ColumnFixedString * column_string = typeid_cast<const ColumnFixedString *>(&column))
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
			throw Exception("Illegal type of column when creating join with string key: " + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
	}
	else if (type == Type::HASHED)
	{
		using Map = typename Maps::MapHashed;
		Map & res = *maps.hashed;

		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			UInt128 key = keys_fit_128_bits
				? packFixed<UInt128>(i, keys_size, key_columns, key_sizes)
				: hash128(i, keys_size, key_columns);

			Inserter<STRICTNESS, Map>::insert(res, key, stored_block, i, pool);
		}
	}
	else
		throw Exception("Unknown JOIN variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
}


void Join::setSampleBlock(const Block & block)
{
	Poco::ScopedWriteRWLock lock(rwlock);

	if (!empty())
		return;

	size_t keys_size = key_names_right.size();
	ConstColumnPlainPtrs key_columns(keys_size);

	for (size_t i = 0; i < keys_size; ++i)
		key_columns[i] = block.getByName(key_names_right[i]).column.get();

	/// Выберем, какую структуру данных для множества использовать.
	init(chooseMethod(key_columns, keys_fit_128_bits, key_sizes));

	sample_block_with_columns_to_add = block;

	/// Переносим из sample_block_with_columns_to_add ключевые столбцы в sample_block_with_keys, сохраняя порядок.
	size_t pos = 0;
	while (pos < sample_block_with_columns_to_add.columns())
	{
		const auto & name = sample_block_with_columns_to_add.unsafeGetByPosition(pos).name;
		if (key_names_right.end() != std::find(key_names_right.begin(), key_names_right.end(), name))
		{
			sample_block_with_keys.insert(sample_block_with_columns_to_add.unsafeGetByPosition(pos));
			sample_block_with_columns_to_add.erase(pos);
		}
		else
			++pos;
	}

	for (size_t i = 0, size = sample_block_with_columns_to_add.columns(); i < size; ++i)
	{
		auto & column = sample_block_with_columns_to_add.unsafeGetByPosition(i);
		if (!column.column)
			column.column = column.type->createColumn();
	}
}


bool Join::insertFromBlock(const Block & block)
{
	Poco::ScopedWriteRWLock lock(rwlock);

	/// Какую структуру данных для множества использовать?
	if (empty())
		throw Exception("Logical error: Join was not initialized", ErrorCodes::LOGICAL_ERROR);

	size_t keys_size = key_names_right.size();
	ConstColumnPlainPtrs key_columns(keys_size);

	/// Редкий случай, когда ключи являются константами. Чтобы не поддерживать отдельный код, материализуем их.
	Columns materialized_columns;

	/// Запоминаем столбцы ключей, с которыми будем работать
	for (size_t i = 0; i < keys_size; ++i)
	{
		key_columns[i] = block.getByName(key_names_right[i]).column.get();

		if (auto converted = key_columns[i]->convertToFullColumnIfConst())
		{
			materialized_columns.emplace_back(converted);
			key_columns[i] = materialized_columns.back().get();
		}
	}

	size_t rows = block.rows();

	blocks.push_back(block);
	Block * stored_block = &blocks.back();

	if (getFullness(kind))
	{
		/** Переносим ключевые столбцы в начало блока.
		  * Именно там их будет ожидать NonJoinedBlockInputStream.
		  */
		size_t key_num = 0;
		for (const auto & name : key_names_right)
		{
			size_t pos = stored_block->getPositionByName(name);
			ColumnWithTypeAndName col = stored_block->getByPosition(pos);
			stored_block->erase(pos);
			stored_block->insert(key_num, std::move(col));
			++key_num;
		}
	}
	else
	{
		/// Удаляем из stored_block ключевые столбцы, так как они не нужны.
		for (const auto & name : key_names_right)
			stored_block->erase(stored_block->getPositionByName(name));
	}

	/// Редкий случай, когда соединяемые столбцы являются константами. Чтобы не поддерживать отдельный код, материализуем их.
	for (size_t i = 0, size = stored_block->columns(); i < size; ++i)
	{
		ColumnPtr col = stored_block->getByPosition(i).column;
		if (auto converted = col->convertToFullColumnIfConst())
			stored_block->getByPosition(i).column = converted;
	}

	if (kind != ASTTableJoin::Kind::Cross)
	{
		/// Заполняем нужную хэш-таблицу.
		if (!getFullness(kind))
		{
			if (strictness == ASTTableJoin::Strictness::Any)
				insertFromBlockImpl<ASTTableJoin::Strictness::Any>(maps_any, rows, key_columns, keys_size, stored_block);
			else
				insertFromBlockImpl<ASTTableJoin::Strictness::All>(maps_all, rows, key_columns, keys_size, stored_block);
		}
		else
		{
			if (strictness == ASTTableJoin::Strictness::Any)
				insertFromBlockImpl<ASTTableJoin::Strictness::Any>(maps_any_full, rows, key_columns, keys_size, stored_block);
			else
				insertFromBlockImpl<ASTTableJoin::Strictness::All>(maps_all_full, rows, key_columns, keys_size, stored_block);
		}
	}

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


template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Map>
struct Adder;

template <typename Map>
struct Adder<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Any, Map>
{
	static void add(const Map & map, const typename Map::key_type & key, size_t num_columns_to_add, ColumnPlainPtrs & added_columns,
		size_t i, IColumn::Filter * filter, IColumn::Offset_t & current_offset, IColumn::Offsets_t * offsets,
		size_t num_columns_to_skip)
	{
		typename Map::const_iterator it = map.find(key);

		if (it != map.end())
		{
			it->second.setUsed();
			for (size_t j = 0; j < num_columns_to_add; ++j)
				added_columns[j]->insertFrom(*it->second.block->unsafeGetByPosition(num_columns_to_skip + j).column.get(), it->second.row_num);
		}
		else
		{
			for (size_t j = 0; j < num_columns_to_add; ++j)
				added_columns[j]->insertDefault();
		}
	}
};

template <typename Map>
struct Adder<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::Any, Map>
{
	static void add(const Map & map, const typename Map::key_type & key, size_t num_columns_to_add, ColumnPlainPtrs & added_columns,
		size_t i, IColumn::Filter * filter, IColumn::Offset_t & current_offset, IColumn::Offsets_t * offsets,
		size_t num_columns_to_skip)
	{
		typename Map::const_iterator it = map.find(key);

		if (it != map.end())
		{
			(*filter)[i] = 1;

			it->second.setUsed();
			for (size_t j = 0; j < num_columns_to_add; ++j)
				added_columns[j]->insertFrom(*it->second.block->unsafeGetByPosition(num_columns_to_skip + j).column.get(), it->second.row_num);
		}
		else
			(*filter)[i] = 0;
	}
};

template <ASTTableJoin::Kind KIND, typename Map>
struct Adder<KIND, ASTTableJoin::Strictness::All, Map>
{
	static void add(const Map & map, const typename Map::key_type & key, size_t num_columns_to_add, ColumnPlainPtrs & added_columns,
		size_t i, IColumn::Filter * filter, IColumn::Offset_t & current_offset, IColumn::Offsets_t * offsets,
		size_t num_columns_to_skip)
	{
		typename Map::const_iterator it = map.find(key);

		if (it != map.end())
		{
			size_t rows_joined = 0;
			it->second.setUsed();
			for (auto current = &static_cast<const typename Map::mapped_type::Base_t &>(it->second); current != nullptr; current = current->next)
			{
				for (size_t j = 0; j < num_columns_to_add; ++j)
					added_columns[j]->insertFrom(*current->block->unsafeGetByPosition(num_columns_to_skip + j).column.get(), current->row_num);

				++rows_joined;
			}

			current_offset += rows_joined;
			(*offsets)[i] = current_offset;
		}
		else
		{
			if (KIND == ASTTableJoin::Kind::Inner)
			{
				(*offsets)[i] = current_offset;
			}
			else
			{
				++current_offset;
				(*offsets)[i] = current_offset;

				for (size_t j = 0; j < num_columns_to_add; ++j)
					added_columns[j]->insertDefault();
			}
		}
	}
};


template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
void Join::joinBlockImpl(Block & block, const Maps & maps) const
{
	size_t keys_size = key_names_left.size();
	ConstColumnPlainPtrs key_columns(keys_size);

	/// Редкий случай, когда ключи являются константами. Чтобы не поддерживать отдельный код, материализуем их.
	Columns materialized_columns;

	/// Запоминаем столбцы ключей, с которыми будем работать
	for (size_t i = 0; i < keys_size; ++i)
	{
		key_columns[i] = block.getByName(key_names_left[i]).column.get();

		if (auto converted = key_columns[i]->convertToFullColumnIfConst())
		{
			materialized_columns.emplace_back(converted);
			key_columns[i] = materialized_columns.back().get();
		}
	}

	size_t existing_columns = block.columns();

	/** Если используется FULL или RIGHT JOIN, то столбцы из "левой" части надо материализовать.
	  * Потому что, если они константы, то в "неприсоединённых" строчках, у них могут быть другие значения
	  *  - значения по-умолчанию, которые могут отличаться от значений этих констант.
	  */
	if (getFullness(kind))
	{
		for (size_t i = 0; i < existing_columns; ++i)
		{
			auto & col = block.getByPosition(i).column;

			if (auto converted = col->convertToFullColumnIfConst())
				col = converted;
		}
	}

	/// Добавляем в блок новые столбцы.
	size_t num_columns_to_add = sample_block_with_columns_to_add.columns();
	ColumnPlainPtrs added_columns(num_columns_to_add);

	for (size_t i = 0; i < num_columns_to_add; ++i)
	{
		const ColumnWithTypeAndName & src_column = sample_block_with_columns_to_add.getByPosition(i);
		ColumnWithTypeAndName new_column = src_column.cloneEmpty();
		added_columns[i] = new_column.column.get();
		added_columns[i]->reserve(src_column.column->size());
		block.insert(std::move(new_column));
	}

	size_t rows = block.rowsInFirstColumn();

	/// Используется при ANY INNER JOIN
	std::unique_ptr<IColumn::Filter> filter;

	if ((kind == ASTTableJoin::Kind::Inner || kind == ASTTableJoin::Kind::Right) && strictness == ASTTableJoin::Strictness::Any)
		filter.reset(new IColumn::Filter(rows));

	/// Используется при ALL ... JOIN
	IColumn::Offset_t current_offset = 0;
	std::unique_ptr<IColumn::Offsets_t> offsets_to_replicate;

	if (strictness == ASTTableJoin::Strictness::All)
		offsets_to_replicate.reset(new IColumn::Offsets_t(rows));

	/** Для LEFT/INNER JOIN, сохранённые блоки не содержат ключи.
	  * Для FULL/RIGHT JOIN, сохранённые блоки содержат ключи;
	  *  но они не будут использоваться на этой стадии соединения (а будут в AdderNonJoined), и их нужно пропустить.
	  */
	size_t num_columns_to_skip = 0;
	if (getFullness(kind))
		num_columns_to_skip = keys_size;

//	std::cerr << num_columns_to_skip << "\n" << block.dumpStructure() << "\n" << blocks.front().dumpStructure() << "\n";

	if (type == Type::KEY_64)
	{
		using Map = typename Maps::MapUInt64;
		const Map & map = *maps.key64;
		const IColumn & column = *key_columns[0];

		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			/// Строим ключ
			UInt64 key = column.get64(i);
			Adder<KIND, STRICTNESS, Map>::add(
				map, key, num_columns_to_add, added_columns, i, filter.get(), current_offset, offsets_to_replicate.get(), num_columns_to_skip);
		}
	}
	else if (type == Type::KEY_STRING)
	{
		using Map = typename Maps::MapString;
		const Map & map = *maps.key_string;
		const IColumn & column = *key_columns[0];

		if (const ColumnString * column_string = typeid_cast<const ColumnString *>(&column))
		{
			const ColumnString::Offsets_t & offsets = column_string->getOffsets();
			const ColumnString::Chars_t & data = column_string->getChars();

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				StringRef key(&data[i == 0 ? 0 : offsets[i - 1]], (i == 0 ? offsets[i] : (offsets[i] - offsets[i - 1])) - 1);
				Adder<KIND, STRICTNESS, Map>::add(
					map, key, num_columns_to_add, added_columns, i, filter.get(), current_offset, offsets_to_replicate.get(), num_columns_to_skip);
			}
		}
		else if (const ColumnFixedString * column_string = typeid_cast<const ColumnFixedString *>(&column))
		{
			size_t n = column_string->getN();
			const ColumnFixedString::Chars_t & data = column_string->getChars();

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				StringRef key(&data[i * n], n);
				Adder<KIND, STRICTNESS, Map>::add(
					map, key, num_columns_to_add, added_columns, i, filter.get(), current_offset, offsets_to_replicate.get(), num_columns_to_skip);
			}
		}
		else
			throw Exception("Illegal type of column when creating set with string key: " + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
	}
	else if (type == Type::HASHED)
	{
		using Map = typename Maps::MapHashed;
		Map & map = *maps.hashed;

		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			UInt128 key = keys_fit_128_bits
				? packFixed<UInt128>(i, keys_size, key_columns, key_sizes)
				: hash128(i, keys_size, key_columns);

			Adder<KIND, STRICTNESS, Map>::add(
				map, key, num_columns_to_add, added_columns, i, filter.get(), current_offset, offsets_to_replicate.get(), num_columns_to_skip);
		}
	}
	else
		throw Exception("Unknown JOIN variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);

	/// Если ANY INNER|RIGHT JOIN - фильтруем все столбцы кроме новых.
	if (filter)
		for (size_t i = 0; i < existing_columns; ++i)
			block.getByPosition(i).column = block.getByPosition(i).column->filter(*filter, -1);

	/// Если ALL ... JOIN - размножаем все столбцы кроме новых.
	if (offsets_to_replicate)
		for (size_t i = 0; i < existing_columns; ++i)
			block.getByPosition(i).column = block.getByPosition(i).column->replicate(*offsets_to_replicate);
}


void Join::joinBlockImplCross(Block & block) const
{
	Block res = block.cloneEmpty();

	/// Добавляем в блок новые столбцы.
	size_t num_existing_columns = res.columns();
	size_t num_columns_to_add = sample_block_with_columns_to_add.columns();

	ColumnPlainPtrs src_left_columns(num_existing_columns);
	ColumnPlainPtrs dst_left_columns(num_existing_columns);
	ColumnPlainPtrs dst_right_columns(num_columns_to_add);

	for (size_t i = 0; i < num_existing_columns; ++i)
	{
		src_left_columns[i] = block.unsafeGetByPosition(i).column.get();
		dst_left_columns[i] = res.unsafeGetByPosition(i).column.get();
	}

	for (size_t i = 0; i < num_columns_to_add; ++i)
	{
		const ColumnWithTypeAndName & src_column = sample_block_with_columns_to_add.unsafeGetByPosition(i);
		ColumnWithTypeAndName new_column = src_column.cloneEmpty();
		dst_right_columns[i] = new_column.column.get();
		res.insert(std::move(new_column));
	}

	size_t rows_left = block.rowsInFirstColumn();

	/// NOTE Было бы оптимальнее использовать reserve, а также методы replicate для размножения значений левого блока.

	for (size_t i = 0; i < rows_left; ++i)
	{
		for (const Block & block_right : blocks)
		{
			size_t rows_right = block_right.rowsInFirstColumn();

			for (size_t col_num = 0; col_num < num_existing_columns; ++col_num)
				for (size_t j = 0; j < rows_right; ++j)
					dst_left_columns[col_num]->insertFrom(*src_left_columns[col_num], i);

			for (size_t col_num = 0; col_num < num_columns_to_add; ++col_num)
			{
				const IColumn * column_right = block_right.unsafeGetByPosition(col_num).column.get();

				for (size_t j = 0; j < rows_right; ++j)
					dst_right_columns[col_num]->insertFrom(*column_right, j);
			}
		}
	}

	block = res;
}


void Join::checkTypesOfKeys(const Block & block_left, const Block & block_right) const
{
	size_t keys_size = key_names_left.size();

	for (size_t i = 0; i < keys_size; ++i)
		if (block_left.getByName(key_names_left[i]).type->getName() != block_right.getByName(key_names_right[i]).type->getName())
			throw Exception("Type mismatch of columns to JOIN by: "
				+ key_names_left[i] + " " + block_left.getByName(key_names_left[i]).type->getName() + " at left, "
				+ key_names_right[i] + " " + block_right.getByName(key_names_right[i]).type->getName() + " at right",
				ErrorCodes::TYPE_MISMATCH);
}


void Join::joinBlock(Block & block) const
{
//	std::cerr << "joinBlock: " << block.dumpStructure() << "\n";

	Poco::ScopedReadRWLock lock(rwlock);

	checkTypesOfKeys(block, sample_block_with_keys);

	if (kind == ASTTableJoin::Kind::Left && strictness == ASTTableJoin::Strictness::Any)
		joinBlockImpl<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Any>(block, maps_any);
	else if (kind == ASTTableJoin::Kind::Inner && strictness == ASTTableJoin::Strictness::Any)
		joinBlockImpl<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::Any>(block, maps_any);
	else if (kind == ASTTableJoin::Kind::Left && strictness == ASTTableJoin::Strictness::All)
		joinBlockImpl<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::All>(block, maps_all);
	else if (kind == ASTTableJoin::Kind::Inner && strictness == ASTTableJoin::Strictness::All)
		joinBlockImpl<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::All>(block, maps_all);
	else if (kind == ASTTableJoin::Kind::Full && strictness == ASTTableJoin::Strictness::Any)
		joinBlockImpl<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Any>(block, maps_any_full);
	else if (kind == ASTTableJoin::Kind::Right && strictness == ASTTableJoin::Strictness::Any)
		joinBlockImpl<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::Any>(block, maps_any_full);
	else if (kind == ASTTableJoin::Kind::Full && strictness == ASTTableJoin::Strictness::All)
		joinBlockImpl<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::All>(block, maps_all_full);
	else if (kind == ASTTableJoin::Kind::Right && strictness == ASTTableJoin::Strictness::All)
		joinBlockImpl<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::All>(block, maps_all_full);
	else if (kind == ASTTableJoin::Kind::Cross)
		joinBlockImplCross(block);
	else
		throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);
}


void Join::joinTotals(Block & block) const
{
	Block totals_without_keys = totals;

	if (totals_without_keys)
	{
		for (const auto & name : key_names_right)
			totals_without_keys.erase(totals_without_keys.getPositionByName(name));

		for (size_t i = 0; i < totals_without_keys.columns(); ++i)
			block.insert(totals_without_keys.getByPosition(i));
	}
	else
	{
		/// Будем присоединять пустые totals - из одной строчки со значениями по-умолчанию.
		totals_without_keys = sample_block_with_columns_to_add.cloneEmpty();

		for (size_t i = 0; i < totals_without_keys.columns(); ++i)
		{
			totals_without_keys.getByPosition(i).column->insertDefault();
			block.insert(totals_without_keys.getByPosition(i));
		}
	}
}


template <ASTTableJoin::Strictness STRICTNESS, typename Mapped>
struct AdderNonJoined;

template <typename Mapped>
struct AdderNonJoined<ASTTableJoin::Strictness::Any, Mapped>
{
	static void add(const Mapped & mapped,
		size_t num_columns_left, ColumnPlainPtrs & columns_left,
		size_t num_columns_right, ColumnPlainPtrs & columns_right)
	{
		for (size_t j = 0; j < num_columns_left; ++j)
			columns_left[j]->insertDefault();

		for (size_t j = 0; j < num_columns_right; ++j)
			columns_right[j]->insertFrom(*mapped.block->unsafeGetByPosition(j).column.get(), mapped.row_num);
	}
};

template <typename Mapped>
struct AdderNonJoined<ASTTableJoin::Strictness::All, Mapped>
{
	static void add(const Mapped & mapped,
		size_t num_columns_left, ColumnPlainPtrs & columns_left,
		size_t num_columns_right, ColumnPlainPtrs & columns_right)
	{
		for (auto current = &static_cast<const typename Mapped::Base_t &>(mapped); current != nullptr; current = current->next)
		{
			for (size_t j = 0; j < num_columns_left; ++j)
				columns_left[j]->insertDefault();

			for (size_t j = 0; j < num_columns_right; ++j)
				columns_right[j]->insertFrom(*current->block->unsafeGetByPosition(j).column.get(), current->row_num);
		}
	}
};


/// Поток из неприсоединённых ранее строк правой таблицы.
class NonJoinedBlockInputStream : public IProfilingBlockInputStream
{
public:
	NonJoinedBlockInputStream(const Join & parent_, Block & left_sample_block, size_t max_block_size_)
		: parent(parent_), max_block_size(max_block_size_)
	{
		/** left_sample_block содержит ключи и "левые" столбцы.
		  * result_sample_block - ключи, "левые" столбцы и "правые" столбцы.
		  */

		size_t num_keys = parent.key_names_left.size();
		size_t num_columns_left = left_sample_block.columns() - num_keys;
		size_t num_columns_right = parent.sample_block_with_columns_to_add.columns();

		result_sample_block = left_sample_block;

//		std::cerr << result_sample_block.dumpStructure() << "\n";

		/// Добавляем в блок новые столбцы.
		for (size_t i = 0; i < num_columns_right; ++i)
		{
			const ColumnWithTypeAndName & src_column = parent.sample_block_with_columns_to_add.getByPosition(i);
			ColumnWithTypeAndName new_column = src_column.cloneEmpty();
			result_sample_block.insert(std::move(new_column));
		}

		column_numbers_left.reserve(num_columns_left);
		column_numbers_keys_and_right.reserve(num_keys + num_columns_right);

		for (size_t i = 0; i < num_keys + num_columns_left; ++i)
		{
			const String & name = left_sample_block.getByPosition(i).name;

			auto found_key_column = std::find(parent.key_names_left.begin(), parent.key_names_left.end(), name);
			if (parent.key_names_left.end() == found_key_column)
				column_numbers_left.push_back(i);
			else
				column_numbers_keys_and_right.push_back(found_key_column - parent.key_names_left.begin());
		}

		for (size_t i = 0; i < num_columns_right; ++i)
			column_numbers_keys_and_right.push_back(num_keys + num_columns_left + i);

		columns_left.resize(num_columns_left);
		columns_keys_and_right.resize(num_keys + num_columns_right);
	}

	String getName() const override { return "NonJoined"; }

	String getID() const override
	{
		std::stringstream res;
		res << "NonJoined(" << &parent << ")";
		return res.str();
	}


protected:
	Block readImpl() override
	{
		if (parent.blocks.empty())
			return Block();

		if (parent.strictness == ASTTableJoin::Strictness::Any)
			return createBlock<ASTTableJoin::Strictness::Any>(parent.maps_any_full);
		else if (parent.strictness == ASTTableJoin::Strictness::All)
			return createBlock<ASTTableJoin::Strictness::All>(parent.maps_all_full);
		else
			throw Exception("Logical error: unknown JOIN strictness (must be ANY or ALL)", ErrorCodes::LOGICAL_ERROR);
	}

private:
	const Join & parent;
	size_t max_block_size;

	Block result_sample_block;
	ColumnNumbers column_numbers_left;
	ColumnNumbers column_numbers_keys_and_right;
	ColumnPlainPtrs columns_left;
	ColumnPlainPtrs columns_keys_and_right;

	std::unique_ptr<void, std::function<void(void *)>> position;	/// type erasure


	template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
	Block createBlock(const Maps & maps)
	{
		Block block = result_sample_block.cloneEmpty();

		size_t num_columns_left = column_numbers_left.size();
		size_t num_columns_right = column_numbers_keys_and_right.size();

		for (size_t i = 0; i < num_columns_left; ++i)
		{
			auto & column_with_type_and_name = block.getByPosition(column_numbers_left[i]);
			column_with_type_and_name.column = column_with_type_and_name.type->createColumn();
			columns_left[i] = column_with_type_and_name.column.get();
		}

		for (size_t i = 0; i < num_columns_right; ++i)
		{
			auto & column_with_type_and_name = block.getByPosition(column_numbers_keys_and_right[i]);
			column_with_type_and_name.column = column_with_type_and_name.type->createColumn();
			columns_keys_and_right[i] = column_with_type_and_name.column.get();
			columns_keys_and_right[i]->reserve(column_with_type_and_name.column->size());
		}

		size_t rows_added = 0;
		if (parent.type == Join::Type::KEY_64)
			rows_added = fillColumns<STRICTNESS>(*maps.key64, num_columns_left, columns_left, num_columns_right, columns_keys_and_right);
		else if (parent.type == Join::Type::KEY_STRING)
			rows_added = fillColumns<STRICTNESS>(*maps.key_string, num_columns_left, columns_left, num_columns_right, columns_keys_and_right);
		else if (parent.type == Join::Type::HASHED)
			rows_added = fillColumns<STRICTNESS>(*maps.hashed, num_columns_left, columns_left, num_columns_right, columns_keys_and_right);
		else
			throw Exception("Unknown JOIN variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);

//		std::cerr << "rows added: " << rows_added << "\n";

		if (!rows_added)
			return Block();

/*		std::cerr << block.dumpStructure() << "\n";
		WriteBufferFromFileDescriptor wb(STDERR_FILENO);
		TabSeparatedBlockOutputStream out(wb);
		out.write(block);*/

		return block;
	}


	template <ASTTableJoin::Strictness STRICTNESS, typename Map>
	size_t fillColumns(const Map & map,
		size_t num_columns_left, ColumnPlainPtrs & columns_left,
		size_t num_columns_right, ColumnPlainPtrs & columns_right)
	{
		size_t rows_added = 0;

		if (!position)
			position = decltype(position)(
				static_cast<void *>(new typename Map::const_iterator(map.begin())),
				[](void * ptr) { delete reinterpret_cast<typename Map::const_iterator *>(ptr); });

		auto & it = *reinterpret_cast<typename Map::const_iterator *>(position.get());
		auto end = map.end();

		for (; it != end; ++it)
		{
			if (it->second.getUsed())
				continue;

			AdderNonJoined<STRICTNESS, typename Map::mapped_type>::add(it->second, num_columns_left, columns_left, num_columns_right, columns_right);

			++rows_added;
			if (rows_added == max_block_size)
				break;
		}

		return rows_added;
	}
};


BlockInputStreamPtr Join::createStreamWithNonJoinedRows(Block & left_sample_block, size_t max_block_size) const
{
	return std::make_shared<NonJoinedBlockInputStream>(*this, left_sample_block, max_block_size);
}


}
