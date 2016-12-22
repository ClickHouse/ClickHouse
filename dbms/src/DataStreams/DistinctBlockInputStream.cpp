#include <DB/DataStreams/DistinctBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int SET_SIZE_LIMIT_EXCEEDED;
}

DistinctBlockInputStream::DistinctBlockInputStream(BlockInputStreamPtr input_, const Limits & limits, size_t limit_, Names columns_)
	: columns_names(columns_),
	limit(limit_),
	max_rows(limits.max_rows_in_distinct),
	max_bytes(limits.max_bytes_in_distinct),
	overflow_mode(limits.distinct_overflow_mode)
{
	children.push_back(input_);
}

String DistinctBlockInputStream::getID() const
{
	std::stringstream res;
	res << "Distinct(" << children.back()->getID() << ")";
	return res.str();
}

Block DistinctBlockInputStream::readImpl()
{
	/// Пока не встретится блок, после фильтрации которого что-нибудь останется, или поток не закончится.
	while (1)
	{
		/// Если уже прочитали достаточно строк - то больше читать не будем.
		if (limit && data.getTotalRowCount() >= limit)
			return Block();

		Block block = children[0]->read();
		if (!block)
			return Block();

		const ConstColumnPlainPtrs column_ptrs(getKeyColumns(block));
		if (column_ptrs.empty())
			return block;

		if (data.empty())
			data.init(SetVariants::chooseMethod(column_ptrs, key_sizes));

		const size_t old_set_size = data.getTotalRowCount();
		const size_t rows = block.rows();
		/// Будем фильтровать блок, оставляя там только строки, которых мы ещё не видели.
		IColumn::Filter filter(rows);

		switch (data.type)
		{
			case SetVariants::Type::EMPTY:
				break;
	#define M(NAME) \
			case SetVariants::Type::NAME: \
				buildFilter(*data.NAME, column_ptrs, filter, rows); \
				break;
		APPLY_FOR_SET_VARIANTS(M)
	#undef M
		}

		/// Если ни одной новой строки не было в блоке - перейдём к следующему блоку.
		if (data.getTotalRowCount() == old_set_size)
			continue;

		if (!checkLimits())
		{
			if (overflow_mode == OverflowMode::THROW)
				throw Exception("DISTINCT-Set size limit exceeded."
					" Rows: " + toString(data.getTotalRowCount()) +
					", limit: " + toString(max_rows) +
					". Bytes: " + toString(data.getTotalByteCount()) +
					", limit: " + toString(max_bytes) + ".",
					ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);

			if (overflow_mode == OverflowMode::BREAK)
				return Block();

			throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
		}

		size_t all_columns = block.columns();
		for (size_t i = 0; i < all_columns; ++i)
			block.getByPosition(i).column = block.getByPosition(i).column->filter(filter, -1);

		return block;
	}
}

bool DistinctBlockInputStream::checkLimits() const
{
	if (max_rows && data.getTotalRowCount() > max_rows)
		return false;
	if (max_bytes && data.getTotalByteCount() > max_bytes)
		return false;
	return true;
}

template <typename Method>
void DistinctBlockInputStream::buildFilter(
	Method & method,
	const ConstColumnPlainPtrs & columns,
	IColumn::Filter & filter,
	size_t rows) const
{
	typename Method::State state;
	state.init(columns);

	/// Для всех строчек
	for (size_t i = 0; i < rows; ++i)
	{
		/// Строим ключ
		typename Method::Key key = state.getKey(columns, columns.size(), i, key_sizes);

		/// Если вставилось в множество - строчку оставляем, иначе - удаляем.
		filter[i] = method.data.insert(key).second;

		if (limit && data.getTotalRowCount() == limit)
		{
			memset(&filter[i + 1], 0, (rows - (i + 1)) * sizeof(IColumn::Filter::value_type));
			break;
		}
	}
}

ConstColumnPlainPtrs DistinctBlockInputStream::getKeyColumns(const Block & block) const
{
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

	return column_ptrs;
}

}
