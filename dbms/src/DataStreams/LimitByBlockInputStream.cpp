#include <DB/DataStreams/LimitByBlockInputStream.h>

namespace DB
{

LimitByBlockInputStream::LimitByBlockInputStream(BlockInputStreamPtr input_, size_t group_size_, Names columns_)
	: columns_names(columns_)
	, group_size(group_size_)
{
	children.push_back(input_);
}

String LimitByBlockInputStream::getID() const
{
	std::stringstream res;
	res << "LimitBy(" << this << ")";
	return res.str();
}

Block LimitByBlockInputStream::readImpl()
{
	while (true)
	{
		Block block = children[0]->read();
		if (!block)
			return Block();

		const ConstColumnPlainPtrs column_ptrs(getKeyColumns(block));
		const size_t rows = block.rows();
		IColumn::Filter filter(rows);
		bool inserted = false;

		for (size_t i = 0; i < rows; ++i)
		{
			UInt128 key;
			SipHash hash;

			for (auto & column : column_ptrs)
				column->updateHashWithValue(i, hash);

			hash.get128(key.first, key.second);

			const bool valid = (keys_counts[key]++ < group_size);
			filter[i] = valid;
			inserted |= valid;
		}

		if (!inserted)
			continue;

		size_t all_columns = block.columns();
		for (size_t i = 0; i < all_columns; ++i)
			block.getByPosition(i).column = block.getByPosition(i).column->filter(filter, -1);

		return block;
	}
}

ConstColumnPlainPtrs LimitByBlockInputStream::getKeyColumns(Block & block) const
{
	ConstColumnPlainPtrs column_ptrs;
	column_ptrs.reserve(columns_names.size());

	for (const auto & name : columns_names)
	{
		auto & column = block.getByName(name).column;

		/// Ignore all constant columns.
		if (!column->isConst())
			column_ptrs.emplace_back(column.get());
	}

	return column_ptrs;
}

}
