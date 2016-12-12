#pragma once

#include <DB/Core/Block.h>
#include <ext/map.hpp>
#include <ext/range.hpp>


namespace DB
{

namespace ErrorCodes
{
	extern const int NOT_IMPLEMENTED;
	extern const int CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE;
}


/** Column, that is just group of few another columns.
  *
  * For constant Tuples, see ColumnConstTuple.
  * Mixed constant/non-constant columns is prohibited in tuple
  *  for implementation simplicity.
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
			columns[i] = data.unsafeGetByPosition(i).column;
	}

	std::string getName() const override { return "Tuple"; }

	ColumnPtr cloneEmpty() const override
	{
		return std::make_shared<ColumnTuple>(data.cloneEmpty());
	}

	size_t size() const override
	{
		return data.rows();
	}

	Field operator[](size_t n) const override
	{
		return Tuple{ext::map<TupleBackend>(columns, [n] (const auto & column) { return (*column)[n]; })};
	}

	void get(size_t n, Field & res) const override
	{
		const size_t size = columns.size();
		res = Tuple(TupleBackend(size));
		TupleBackend & res_arr = DB::get<Tuple &>(res).t;
		for (const auto i : ext::range(0, size))
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
		const TupleBackend & tuple = DB::get<const Tuple &>(x).t;

		const size_t size = columns.size();
		if (tuple.size() != size)
			throw Exception("Cannot insert value of different size into tuple", ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE);

		for (size_t i = 0; i < size; ++i)
			columns[i]->insert(tuple[i]);
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
		for (auto & column : columns)
			column->insertDefault();
	}

	void popBack(size_t n) override
	{
		for (auto & column : columns)
			column->popBack(n);
	}


	StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override
	{
		size_t values_size = 0;
		for (auto & column : columns)
			values_size += column->serializeValueIntoArena(n, arena, begin).size;

		return StringRef(begin, values_size);
	}

	const char * deserializeAndInsertFromArena(const char * pos) override
	{
		for (auto & column : columns)
			pos = column->deserializeAndInsertFromArena(pos);

		return pos;
	}

	void updateHashWithValue(size_t n, SipHash & hash) const override
	{
		for (auto & column : columns)
			column->updateHashWithValue(n, hash);
	}

	void insertRangeFrom(const IColumn & src, size_t start, size_t length) override
	{
		for (size_t i = 0; i < columns.size(); ++i)
			data.unsafeGetByPosition(i).column->insertRangeFrom(
				*static_cast<const ColumnTuple &>(src).data.unsafeGetByPosition(i).column.get(),
				start, length);
	}

	ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override
	{
		Block res_block = data.cloneEmpty();

		for (size_t i = 0; i < columns.size(); ++i)
			res_block.unsafeGetByPosition(i).column = data.unsafeGetByPosition(i).column->filter(filt, result_size_hint);

		return std::make_shared<ColumnTuple>(res_block);
	}

	ColumnPtr permute(const Permutation & perm, size_t limit) const override
	{
		Block res_block = data.cloneEmpty();

		for (size_t i = 0; i < columns.size(); ++i)
			res_block.unsafeGetByPosition(i).column = data.unsafeGetByPosition(i).column->permute(perm, limit);

		return std::make_shared<ColumnTuple>(res_block);
	}

	ColumnPtr replicate(const Offsets_t & offsets) const override
	{
		Block res_block = data.cloneEmpty();

		for (size_t i = 0; i < columns.size(); ++i)
			res_block.unsafeGetByPosition(i).column = data.unsafeGetByPosition(i).column->replicate(offsets);

		return std::make_shared<ColumnTuple>(res_block);
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
			for (const auto & column : columns)
				plain_columns.push_back(column.get());
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
		for (auto & column : columns)
			column->reserve(n);
	}

	size_t byteSize() const override
	{
		size_t res = 0;
		for (const auto & column : columns)
			res += column->byteSize();
		return res;
	}

	ColumnPtr convertToFullColumnIfConst() const override
	{
		Block materialized = data;
		for (size_t i = 0, size = materialized.columns(); i < size; ++i)
			if (auto converted = materialized.unsafeGetByPosition(i).column->convertToFullColumnIfConst())
				materialized.unsafeGetByPosition(i).column = converted;

		return std::make_shared<ColumnTuple>(materialized);
	}


	const Block & getData() const { return data; }
	Block & getData() { return data; }

	const Columns & getColumns() const { return columns; }
	Columns & getColumns() { return columns; }

	void getExtremes(Field & min, Field & max) const override
	{
		const size_t tuple_size = columns.size();

		min = Tuple(TupleBackend(tuple_size));
		max = Tuple(TupleBackend(tuple_size));

		auto & min_backend = min.get<Tuple &>().t;
		auto & max_backend = max.get<Tuple &>().t;

		for (const auto i : ext::range(0, tuple_size))
			columns[i]->getExtremes(min_backend[i], max_backend[i]);
	}
};


}
