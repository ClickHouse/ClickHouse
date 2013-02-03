#pragma once

#include <DB/Core/Block.h>


namespace DB
{


/** Столбец, который всего лишь группирует вместе несколько других столбцов.
  */
class ColumnTuple : public IColumn
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
	
	std::string getName() const { return "Tuple"; }
	
	SharedPtr<IColumn> cloneEmpty() const
	{
		return new ColumnTuple(data.cloneEmpty());
	}

	size_t size() const
	{
		return data.rows();
	}

	bool empty() const { return size() == 0; }

	Field operator[](size_t n) const
	{
		Array res;

		for (Columns::const_iterator it = columns.begin(); it != columns.end(); ++it)
			res.push_back((**it)[n]);

		return res;
	}

	void get(size_t n, Field & res) const
	{
		size_t size = columns.size();
		res = Array(size);
		Array & res_arr = DB::get<Array &>(res);
		for (size_t i = 0; i < size; ++i)
			columns[i]->get(n, res_arr[i]);
	}

	StringRef getDataAt(size_t n) const
	{
		throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	void insert(const Field & x)
	{
		const Array & arr = DB::get<const Array &>(x);

		size_t size = columns.size();
		if (arr.size() != size)
			throw Exception("Cannot insert value of different size into tuple", ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE);

		for (size_t i = 0; i < size; ++i)
			columns[i]->insert(arr[i]);
	}

	void insertFrom(const IColumn & src_, size_t n)
	{
		const ColumnTuple & src = static_cast<const ColumnTuple &>(src_);
		
		size_t size = columns.size();
		if (src.columns.size() != size)
			throw Exception("Cannot insert value of different size into tuple", ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE);
			
		for (size_t i = 0; i < size; ++i)
			columns[i]->insertFrom(*src.columns[i], n);
	}

	void insertDefault()
	{
		for (Columns::iterator it = columns.begin(); it != columns.end(); ++it)
			(*it)->insertDefault();
	}


	/** В следующих функциях ничего не делаем, так как столбцы - элементы tuple обычно содержатся в блоке вместе с tuple,
	  *  и соответствующие операции применяются к ним также. То есть, операции будут применены к tuple автоматически.
	  */
	void cut(size_t start, size_t length) {}
	void filter(const Filter & filt) {}
	void permute(const Permutation & perm) {}
	void replicate(const Offsets_t & offsets) {}

	int compareAt(size_t n, size_t m, const IColumn & rhs) const
	{
		size_t size = columns.size();
		for (size_t i = 0; i < size; ++i)
			if (int res = columns[i]->compareAt(n, m, *static_cast<const ColumnTuple &>(rhs).columns[i]))
				return res;
		
		return 0;
	}

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
				int res = (*it)->compareAt(a, b, **it);
				if (res < 0)
					return true;
				else if (res > 0)
					return false;
			}
			return false;
		}
	};

	Permutation getPermutation() const
	{
		size_t rows = size();
		Permutation perm(rows);
		for (size_t i = 0; i < rows; ++i)
			perm[i] = i;

		Less less(columns);
		std::sort(perm.begin(), perm.end(), less);
		return perm;
	}

	void reserve(size_t n)
	{
		for (Columns::iterator it = columns.begin(); it != columns.end(); ++it)
			(*it)->reserve(n);
	}

	void clear()
	{
		for (Columns::iterator it = columns.begin(); it != columns.end(); ++it)
			(*it)->clear();
	}

	size_t byteSize() const
	{
		size_t res = 0;
		for (Columns::const_iterator it = columns.begin(); it != columns.end(); ++it)
			res += (*it)->byteSize();
		return res;
	}


	const Block & getData() const { return data; }
};


}
