#pragma once

#include <DB/Core/Block.h>


namespace DB
{


/** Столбец, который всего лишь группирует вместе несколько других столбцов.
  */
class ColumnTuple final : public IColumn
{
private:
	Block data;
	Columns columns;

public:
	ColumnTuple() {}

	ColumnTuple(Block data_) : data(data_)
	{
		size_t size = data.columns();
		columns.resize(size);
		for (size_t i = 0; i < size; ++i)
			columns[i] = data.getByPosition(i).column;
	}

	std::string getName() const override { return "Tuple"; }

	SharedPtr<IColumn> cloneEmpty() const override
	{
		return new ColumnTuple(data.cloneEmpty());
	}

	size_t size() const override
	{
		return data.rows();
	}

	Field operator[](size_t n) const override
	{
		Array res;

		for (Columns::const_iterator it = columns.begin(); it != columns.end(); ++it)
			res.push_back((**it)[n]);

		return res;
	}

	void get(size_t n, Field & res) const override
	{
		size_t size = columns.size();
		res = Array(size);
		Array & res_arr = DB::get<Array &>(res);
		for (size_t i = 0; i < size; ++i)
			columns[i]->get(n, res_arr[i]);
	}

	StringRef getDataAt(size_t n) const override
	{
		throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	void insertData(const char * pos, size_t length) override
	{
		throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	void insert(const Field & x) override
	{
		const Array & arr = DB::get<const Array &>(x);

		size_t size = columns.size();
		if (arr.size() != size)
			throw Exception("Cannot insert value of different size into tuple", ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE);

		for (size_t i = 0; i < size; ++i)
			columns[i]->insert(arr[i]);
	}

	void insertFrom(const IColumn & src_, size_t n) override
	{
		const ColumnTuple & src = static_cast<const ColumnTuple &>(src_);

		size_t size = columns.size();
		if (src.columns.size() != size)
			throw Exception("Cannot insert value of different size into tuple", ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE);

		for (size_t i = 0; i < size; ++i)
			columns[i]->insertFrom(*src.columns[i], n);
	}

	void insertDefault() override
	{
		for (Columns::iterator it = columns.begin(); it != columns.end(); ++it)
			(*it)->insertDefault();
	}


	ColumnPtr cut(size_t start, size_t length) const override
	{
		Block res_block = data.cloneEmpty();

		for (size_t i = 0; i < columns.size(); ++i)
			res_block.getByPosition(i).column = data.getByPosition(i).column->cut(start, length);

		return new ColumnTuple(res_block);
	}

	ColumnPtr filter(const Filter & filt) const override
	{
		Block res_block = data.cloneEmpty();

		for (size_t i = 0; i < columns.size(); ++i)
			res_block.getByPosition(i).column = data.getByPosition(i).column->filter(filt);

		return new ColumnTuple(res_block);
	}

	ColumnPtr permute(const Permutation & perm, size_t limit) const override
	{
		Block res_block = data.cloneEmpty();

		for (size_t i = 0; i < columns.size(); ++i)
			res_block.getByPosition(i).column = data.getByPosition(i).column->permute(perm, limit);

		return new ColumnTuple(res_block);
	}

	ColumnPtr replicate(const Offsets_t & offsets) const override
	{
		Block res_block = data.cloneEmpty();

		for (size_t i = 0; i < columns.size(); ++i)
			res_block.getByPosition(i).column = data.getByPosition(i).column->replicate(offsets);

		return new ColumnTuple(res_block);
	}

	int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override
	{
		size_t size = columns.size();
		for (size_t i = 0; i < size; ++i)
			if (int res = columns[i]->compareAt(n, m, *static_cast<const ColumnTuple &>(rhs).columns[i], nan_direction_hint))
				return res;

		return 0;
	}

	template <bool positive>
	struct Less
	{
		ConstColumnPlainPtrs plain_columns;

		Less(const Columns & columns)
		{
			for (Columns::const_iterator it = columns.begin(); it != columns.end(); ++it)
				plain_columns.push_back(&**it);
		}

		bool operator() (size_t a, size_t b) const
		{
			for (ConstColumnPlainPtrs::const_iterator it = plain_columns.begin(); it != plain_columns.end(); ++it)
			{
				int res = (*it)->compareAt(a, b, **it, positive ? 1 : -1);
				if (res < 0)
					return positive;
				else if (res > 0)
					return !positive;
			}
			return false;
		}
	};

	void getPermutation(bool reverse, size_t limit, Permutation & res) const override
	{
		size_t rows = size();
		res.resize(rows);
		for (size_t i = 0; i < rows; ++i)
			res[i] = i;

		if (limit >= rows)
			limit = 0;

		if (limit)
		{
			if (reverse)
				std::partial_sort(res.begin(), res.begin() + limit, res.end(), Less<false>(columns));
			else
				std::partial_sort(res.begin(), res.begin() + limit, res.end(), Less<true>(columns));
		}
		else
		{
			if (reverse)
				std::sort(res.begin(), res.end(), Less<false>(columns));
			else
				std::sort(res.begin(), res.end(), Less<true>(columns));
		}
	}

	void reserve(size_t n) override
	{
		for (Columns::iterator it = columns.begin(); it != columns.end(); ++it)
			(*it)->reserve(n);
	}

	size_t byteSize() const override
	{
		size_t res = 0;
		for (Columns::const_iterator it = columns.begin(); it != columns.end(); ++it)
			res += (*it)->byteSize();
		return res;
	}

	void getExtremes(Field & min, Field & max) const override
	{
		throw Exception("Method getExtremes is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}


	const Block & getData() const { return data; }
	Block & getData() { return data; }
};


}
