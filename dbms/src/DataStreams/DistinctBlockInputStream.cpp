#include <DB/DataStreams/DistinctBlockInputStream.h>


namespace DB
{

DistinctBlockInputStream::DistinctBlockInputStream(BlockInputStreamPtr input_, const Limits & limits, size_t limit_, Names columns_)
	: columns_names(columns_),
	limit(limit_),
	max_rows(limits.max_rows_in_distinct),
	max_bytes(limits.max_bytes_in_distinct),
	overflow_mode(limits.distinct_overflow_mode)
{
	children.push_back(input_);
}


Block DistinctBlockInputStream::readImpl()
{
	/// Пока не встретится блок, после фильтрации которого что-нибудь останется, или поток не закончится.
	while (1)
	{
		/// Если уже прочитали достаточно строк - то больше читать не будем.
		if (limit && set.size() >= limit)
			return Block();

		Block block = children[0]->read();

		if (!block)
			return Block();

		size_t rows = block.rows();
		size_t columns = columns_names.empty() ? block.columns() : columns_names.size();

		ConstColumnPlainPtrs column_ptrs;
		column_ptrs.reserve(columns);

		for (size_t i = 0; i < columns; ++i)
		{
			auto & column = columns_names.empty()
				? block.getByPosition(i).column
				: block.getByName(columns_names[i]).column;

			/// Игнорируем все константные столбцы.
			if (!column->isConst())
				column_ptrs.emplace_back(column.get());
		}

		columns = column_ptrs.size();

		/// Будем фильтровать блок, оставляя там только строки, которых мы ещё не видели.
		IColumn::Filter filter(rows);

		size_t old_set_size = set.size();

		for (size_t i = 0; i < rows; ++i)
		{
			/** Уникальность строк будем отслеживать с помощью множества значений SipHash128.
				* Делается несколько допущений.
				* 1. Допускается неточная работа в случае коллизий SipHash128.
				* 2. Допускается неточная работа, если строковые поля содержат нулевые байты.
				* 3. Не поддерживаются массивы.
				*
				* Для оптимизации, можно добавить другие методы из Set.h.
				*/

			UInt128 key;
			SipHash hash;

			for (size_t j = 0; j < columns; ++j)
			{
				StringRef data = column_ptrs[j]->getDataAtWithTerminatingZero(i);
				hash.update(data.data, data.size);
			}

			hash.get128(key.first, key.second);

			/// Если вставилось в множество - строчку оставляем, иначе - удаляем.
			filter[i] = set.insert(key).second;

			if (limit && set.size() == limit)
			{
				memset(&filter[i + 1], 0, (rows - (i + 1)) * sizeof(IColumn::Filter::value_type));
				break;
			}
		}

		/// Если ни одной новой строки не было в блоке - перейдём к следующему блоку.
		if (set.size() == old_set_size)
			continue;

		if (!checkLimits())
		{
			if (overflow_mode == OverflowMode::THROW)
				throw Exception("DISTINCT-Set size limit exceeded."
					" Rows: " + toString(set.size()) +
					", limit: " + toString(max_rows) +
					". Bytes: " + toString(set.getBufferSizeInBytes()) +
					", limit: " + toString(max_bytes) + ".",
					ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);

			if (overflow_mode == OverflowMode::BREAK)
				return Block();

			throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
		}

		size_t all_columns = block.columns();
		for (size_t i = 0; i < all_columns; ++i)
			block.getByPosition(i).column = block.getByPosition(i).column->filter(filter);

		return block;
	}
}

}
