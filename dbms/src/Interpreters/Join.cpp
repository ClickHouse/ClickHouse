#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>

#include <DB/Parsers/ASTJoin.h>
#include <DB/Interpreters/Join.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{


Join::Type Join::chooseMethod(const ConstColumnPlainPtrs & key_columns, bool & keys_fit_128_bits, Sizes & key_sizes)
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


/// Нужно ли использовать хэш-таблицы maps_*_full, в которых запоминается, была ли строчка присоединена.
static bool getFullness(ASTJoin::Kind kind)
{
	return kind == ASTJoin::Right || kind == ASTJoin::Full;
}


void Join::init(Type type_)
{
	type = type_;

	if (!getFullness(kind))
	{
		if (strictness == ASTJoin::Any)
			initImpl(maps_any, type);
		else
			initImpl(maps_all, type);
	}
	else
	{
		if (strictness == ASTJoin::Any)
			initImpl(maps_any_full, type);
		else
			initImpl(maps_all_full, type);
	}
}

size_t Join::getTotalRowCount() const
{
	size_t res = 0;
	res += getTotalRowCountImpl(maps_any);
	res += getTotalRowCountImpl(maps_all);
	res += getTotalRowCountImpl(maps_any_full);
	res += getTotalRowCountImpl(maps_all_full);
	return res;
}

size_t Join::getTotalByteCount() const
{
	size_t res = 0;
	res += getTotalByteCountImpl(maps_any);
	res += getTotalByteCountImpl(maps_all);
	res += getTotalByteCountImpl(maps_any_full);
	res += getTotalByteCountImpl(maps_all_full);
	res += pool.size();
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

template <> struct Inserter<ASTJoin::Any, Join::MapsAny::MapString> : InserterAnyString<Join::MapsAny::MapString> {};
template <> struct Inserter<ASTJoin::Any, Join::MapsAnyFull::MapString> : InserterAnyString<Join::MapsAnyFull::MapString> {};


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

template <> struct Inserter<ASTJoin::All, Join::MapsAll::MapString> : InserterAllString<Join::MapsAll::MapString> {};
template <> struct Inserter<ASTJoin::All, Join::MapsAllFull::MapString> : InserterAllString<Join::MapsAllFull::MapString> {};


template <ASTJoin::Strictness STRICTNESS, typename Maps>
void Join::insertFromBlockImpl(Maps & maps, size_t rows, const ConstColumnPlainPtrs & key_columns, size_t keys_size, Block * stored_block)
{
	if (type == Type::KEY_64)
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
	else if (type == Type::KEY_STRING)
	{
		typedef typename Maps::MapString Map;
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
		typedef typename Maps::MapHashed Map;
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


bool Join::insertFromBlock(const Block & block)
{
	Poco::ScopedWriteRWLock lock(rwlock);

	size_t keys_size = key_names_right.size();
	ConstColumnPlainPtrs key_columns(keys_size);

	/// Запоминаем столбцы ключей, с которыми будем работать
	for (size_t i = 0; i < keys_size; ++i)
		key_columns[i] = block.getByName(key_names_right[i]).column;

	size_t rows = block.rows();

	/// Какую структуру данных для множества использовать?
	if (empty())
		init(chooseMethod(key_columns, keys_fit_128_bits, key_sizes));

	blocks.push_back(block);
	Block * stored_block = &blocks.back();

	/// Удаляем из stored_block ключевые столбцы, так как они не нужны.
	for (const auto & name : key_names_right)
		stored_block->erase(stored_block->getPositionByName(name));

	if (!getFullness(kind))
	{
		if (strictness == ASTJoin::Any)
			insertFromBlockImpl<ASTJoin::Any>(maps_any, rows, key_columns, keys_size, stored_block);
		else
			insertFromBlockImpl<ASTJoin::All>(maps_all, rows, key_columns, keys_size, stored_block);
	}
	else
	{
		if (strictness == ASTJoin::Any)
			insertFromBlockImpl<ASTJoin::Any>(maps_any_full, rows, key_columns, keys_size, stored_block);
		else
			insertFromBlockImpl<ASTJoin::All>(maps_all_full, rows, key_columns, keys_size, stored_block);
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


template <ASTJoin::Kind KIND, ASTJoin::Strictness STRICTNESS, typename Map>
struct Adder;

template <typename Map>
struct Adder<ASTJoin::Left, ASTJoin::Any, Map>
{
	static void add(const Map & map, const typename Map::key_type & key, size_t num_columns_to_add, ColumnPlainPtrs & added_columns,
		size_t i, IColumn::Filter * filter, IColumn::Offset_t & current_offset, IColumn::Offsets_t * offsets)
	{
		typename Map::const_iterator it = map.find(key);

		if (it != map.end())
		{
			it->second.setUsed();
			for (size_t j = 0; j < num_columns_to_add; ++j)
				added_columns[j]->insertFrom(*it->second.block->unsafeGetByPosition(j).column.get(), it->second.row_num);
		}
		else
		{
			for (size_t j = 0; j < num_columns_to_add; ++j)
				added_columns[j]->insertDefault();
		}
	}
};

template <typename Map>
struct Adder<ASTJoin::Inner, ASTJoin::Any, Map>
{
	static void add(const Map & map, const typename Map::key_type & key, size_t num_columns_to_add, ColumnPlainPtrs & added_columns,
					size_t i, IColumn::Filter * filter, IColumn::Offset_t & current_offset, IColumn::Offsets_t * offsets)
	{
		typename Map::const_iterator it = map.find(key);

		if (it != map.end())
		{
			(*filter)[i] = 1;

			it->second.setUsed();
			for (size_t j = 0; j < num_columns_to_add; ++j)
				added_columns[j]->insertFrom(*it->second.block->unsafeGetByPosition(j).column.get(), it->second.row_num);
		}
		else
			(*filter)[i] = 0;
	}
};

template <ASTJoin::Kind KIND, typename Map>
struct Adder<KIND, ASTJoin::All, Map>
{
	static void add(const Map & map, const typename Map::key_type & key, size_t num_columns_to_add, ColumnPlainPtrs & added_columns,
					size_t i, IColumn::Filter * filter, IColumn::Offset_t & current_offset, IColumn::Offsets_t * offsets)
	{
		typename Map::const_iterator it = map.find(key);

		if (it != map.end())
		{
			size_t rows_joined = 0;
			it->second.setUsed();
			for (auto current = &static_cast<const typename Map::mapped_type::Base_t &>(it->second); current != nullptr; current = current->next)
			{
				for (size_t j = 0; j < num_columns_to_add; ++j)
					added_columns[j]->insertFrom(*current->block->unsafeGetByPosition(j).column.get(), current->row_num);

				++rows_joined;
			}

			current_offset += rows_joined;
			(*offsets)[i] = current_offset;
		}
		else
		{
			if (KIND == ASTJoin::Inner)
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


template <ASTJoin::Kind KIND, ASTJoin::Strictness STRICTNESS, typename Maps>
void Join::joinBlockImpl(Block & block, const Maps & maps) const
{
	if (blocks.empty())
		throw Exception("Attempt to JOIN with empty table", ErrorCodes::EMPTY_DATA_PASSED);

	size_t keys_size = key_names_left.size();
	ConstColumnPlainPtrs key_columns(keys_size);

	/// Запоминаем столбцы ключей, с которыми будем работать
	for (size_t i = 0; i < keys_size; ++i)
		key_columns[i] = block.getByName(key_names_left[i]).column;

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

	if ((kind == ASTJoin::Inner || kind == ASTJoin::Right) && strictness == ASTJoin::Any)
		filter.reset(new IColumn::Filter(rows));

	/// Используется при ALL ... JOIN
	IColumn::Offset_t current_offset = 0;
	std::unique_ptr<IColumn::Offsets_t> offsets_to_replicate;

	if (strictness == ASTJoin::All)
		offsets_to_replicate.reset(new IColumn::Offsets_t(rows));

	if (type == Type::KEY_64)
	{
		typedef typename Maps::MapUInt64 Map;
		const Map & map = *maps.key64;
		const IColumn & column = *key_columns[0];

		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			/// Строим ключ
			UInt64 key = column.get64(i);
			Adder<KIND, STRICTNESS, Map>::add(map, key, num_columns_to_add, added_columns, i, filter.get(), current_offset, offsets_to_replicate.get());
		}
	}
	else if (type == Type::KEY_STRING)
	{
		typedef typename Maps::MapString Map;
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
				Adder<KIND, STRICTNESS, Map>::add(map, key, num_columns_to_add, added_columns, i, filter.get(), current_offset, offsets_to_replicate.get());
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
				Adder<KIND, STRICTNESS, Map>::add(map, key, num_columns_to_add, added_columns, i, filter.get(), current_offset, offsets_to_replicate.get());
			}
		}
		else
			throw Exception("Illegal type of column when creating set with string key: " + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
	}
	else if (type == Type::HASHED)
	{
		typedef typename Maps::MapHashed Map;
		Map & map = *maps.hashed;

		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			UInt128 key = keys_fit_128_bits
				? packFixed<UInt128>(i, keys_size, key_columns, key_sizes)
				: hash128(i, keys_size, key_columns);

			Adder<KIND, STRICTNESS, Map>::add(map, key, num_columns_to_add, added_columns, i, filter.get(), current_offset, offsets_to_replicate.get());
		}
	}
	else
		throw Exception("Unknown JOIN variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);

	/// Если ANY INNER|RIGHT JOIN - фильтруем все столбцы кроме новых.
	if (filter)
		for (size_t i = 0; i < existing_columns; ++i)
			block.getByPosition(i).column = block.getByPosition(i).column->filter(*filter);

	/// Если ALL ... JOIN - размножаем все столбцы кроме новых.
	if (offsets_to_replicate)
		for (size_t i = 0; i < existing_columns; ++i)
			block.getByPosition(i).column = block.getByPosition(i).column->replicate(*offsets_to_replicate);
}


void Join::joinBlock(Block & block) const
{
	Poco::ScopedReadRWLock lock(rwlock);

	if (kind == ASTJoin::Left && strictness == ASTJoin::Any)
		joinBlockImpl<ASTJoin::Left, ASTJoin::Any>(block, maps_any);
	else if (kind == ASTJoin::Inner && strictness == ASTJoin::Any)
		joinBlockImpl<ASTJoin::Inner, ASTJoin::Any>(block, maps_any);
	else if (kind == ASTJoin::Left && strictness == ASTJoin::All)
		joinBlockImpl<ASTJoin::Left, ASTJoin::All>(block, maps_all);
	else if (kind == ASTJoin::Inner && strictness == ASTJoin::All)
		joinBlockImpl<ASTJoin::Inner, ASTJoin::All>(block, maps_all);
	else if (kind == ASTJoin::Full && strictness == ASTJoin::Any)
		joinBlockImpl<ASTJoin::Left, ASTJoin::Any>(block, maps_any_full);
	else if (kind == ASTJoin::Right && strictness == ASTJoin::Any)
		joinBlockImpl<ASTJoin::Inner, ASTJoin::Any>(block, maps_any_full);
	else if (kind == ASTJoin::Full && strictness == ASTJoin::All)
		joinBlockImpl<ASTJoin::Left, ASTJoin::All>(block, maps_all_full);
	else if (kind == ASTJoin::Right && strictness == ASTJoin::All)
		joinBlockImpl<ASTJoin::Inner, ASTJoin::All>(block, maps_all_full);
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
		if (blocks.empty())
			return;

		/// Будем присоединять пустые totals - из одной строчки со значениями по-умолчанию.
		totals_without_keys = blocks.front().cloneEmpty();

		for (size_t i = 0; i < totals_without_keys.columns(); ++i)
		{
			totals_without_keys.getByPosition(i).column->insertDefault();
			block.insert(totals_without_keys.getByPosition(i));
		}
	}
}


template <ASTJoin::Strictness STRICTNESS, typename Mapped>
struct AdderNonJoined;

template <typename Mapped>
struct AdderNonJoined<ASTJoin::Any, Mapped>
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
struct AdderNonJoined<ASTJoin::All, Mapped>
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
	NonJoinedBlockInputStream(const Join & parent_, Block & left_sample_block_, size_t max_block_size_)
		: parent(parent_), left_sample_block(left_sample_block_), max_block_size(max_block_size_)
	{
	}

	String getName() const override { return "NonJoinedBlockInputStream"; }

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

		if (parent.strictness == ASTJoin::Any)
			return createBlock<ASTJoin::Any>(parent.maps_any_full);
		else if (parent.strictness == ASTJoin::All)
			return createBlock<ASTJoin::All>(parent.maps_all_full);
		else
			throw Exception("Logical error: unknown JOIN strictness (must be ANY or ALL)", ErrorCodes::LOGICAL_ERROR);
	}

private:
	const Join & parent;
	Block left_sample_block;
	size_t max_block_size;

	std::unique_ptr<void, std::function<void(void *)>> position;	/// type erasure


	template <ASTJoin::Strictness STRICTNESS, typename Maps>
	Block createBlock(const Maps & maps)
	{
		Block block = left_sample_block.cloneEmpty();

		size_t num_columns_left = left_sample_block.columns();
		ColumnPlainPtrs columns_left(num_columns_left);

		for (size_t i = 0; i < num_columns_left; ++i)
		{
			auto & column_with_name_and_type = block.getByPosition(i);
			column_with_name_and_type.column = column_with_name_and_type.type->createColumn();
			columns_left[i] = column_with_name_and_type.column.get();
		}

		/// Добавляем в блок новые столбцы.
		const Block & first_mapped_block = parent.blocks.front();
		size_t num_columns_right = first_mapped_block.columns();
		ColumnPlainPtrs columns_right(num_columns_right);

		for (size_t i = 0; i < num_columns_right; ++i)
		{
			const ColumnWithNameAndType & src_column = first_mapped_block.getByPosition(i);
			ColumnWithNameAndType new_column = src_column.cloneEmpty();
			block.insert(new_column);
			columns_right[i] = new_column.column;
			columns_right[i]->reserve(src_column.column->size());
		}

		size_t rows_added = 0;
		if (parent.type == Join::Type::KEY_64)
			rows_added = fillColumns<STRICTNESS>(*maps.key64, num_columns_left, columns_left, num_columns_right, columns_right);
		else if (parent.type == Join::Type::KEY_STRING)
			rows_added = fillColumns<STRICTNESS>(*maps.key_string, num_columns_left, columns_left, num_columns_right, columns_right);
		else if (parent.type == Join::Type::HASHED)
			rows_added = fillColumns<STRICTNESS>(*maps.hashed, num_columns_left, columns_left, num_columns_right, columns_right);
		else
			throw Exception("Unknown JOIN variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);

		std::cerr << "rows added: " << rows_added << "\n";

		if (!rows_added)
			return Block();

		std::cerr << block.dumpStructure() << "\n";

		return block;
	}


	template <ASTJoin::Strictness STRICTNESS, typename Map>
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
			std::cerr << it->second.getUsed() << "\n";

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
	return new NonJoinedBlockInputStream(*this, left_sample_block, max_block_size);
}


}
