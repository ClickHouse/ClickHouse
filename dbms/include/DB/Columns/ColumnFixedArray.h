#ifndef DBMS_CORE_COLUMN_FIXED_ARRAY_H
#define DBMS_CORE_COLUMN_FIXED_ARRAY_H

#include <Poco/SharedPtr.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/Columns/IColumn.h>


namespace DB
{

using Poco::SharedPtr;

/** Cтолбeц значений типа "массив фиксированного размера".
  */
class ColumnFixedArray : public IColumn
{
public:
	/** Создать пустой столбец массивов фиксированного размера n, со типом значений, как в столбце nested_column */
	ColumnFixedArray(ColumnPtr nested_column, size_t n_)
		: data(nested_column), n(n_)
	{
		clear();
	}

	std::string getName() const { return "ColumnFixedArray(" + data->getName() + ")"; }

	ColumnPtr cloneEmpty() const
	{
		return new ColumnFixedArray(data->cloneEmpty(), n);
	}
	
	size_t size() const
	{
		return data->size() / n;
	}
	
	Field operator[](size_t index) const
	{
		Array res;
		for (size_t i = n * index; i < n * (index + 1); ++i)
			res[i] = (*data)[n * index + i];
		return res;
	}

	void get(size_t index, Field & res) const
	{
		res = Array(n);
		Array & res_arr = DB::get<Array &>(res);
		for (size_t i = n * index; i < n * (index + 1); ++i)
			data->get(n * index + i, res_arr[i]);
	}

	StringRef getDataAt(size_t n) const
	{
		throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	void insertData(const char * pos, size_t length)
	{
		throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	void cut(size_t start, size_t length)
	{
		data->cut(n * start, n * length);
	}

	void insert(const Field & x)
	{
		const Array & array = DB::get<const Array &>(x);
		if (n != array.size())
			throw Exception("Size of array doesn't match size of FixedArray column",
				ErrorCodes::SIZE_OF_ARRAY_DOESNT_MATCH_SIZE_OF_FIXEDARRAY_COLUMN);

		for (size_t i = 0; i < n; ++i)
			data->insert(array[i]);
	}

	void insertFrom(const IColumn & src_, size_t index)
	{
		const ColumnFixedArray & src = static_cast<const ColumnFixedArray &>(src_);

		if (n != src.getN())
			throw Exception("Size of array doesn't match size of FixedArray column",
				ErrorCodes::SIZE_OF_ARRAY_DOESNT_MATCH_SIZE_OF_FIXEDARRAY_COLUMN);

		for (size_t i = 0; i < n; ++i)
			data->insertFrom(src.getData(), n * index + i);
	}

	void insertDefault()
	{
		for (size_t i = 0; i < n; ++i)
			data->insertDefault();
	}

	void clear()
	{
		data->clear();
	}

	/** Более эффективные методы манипуляции */
	IColumn & getData()
	{
		return *data;
	}

	void filter(const Filter & filt)
	{
		size_t size = this->size();
		if (size != filt.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		if (size == 0)
			return;

		/// Не слишком оптимально. Можно сделать специализацию для массивов известных типов.
		Filter nested_filt(size * n);
		for (size_t i = 0; i < size; ++i)
			if (filt[i])
				memset(&nested_filt[i * n], 1, n);
		data->filter(nested_filt);
	}

	void replicate(const Offsets_t & offsets)
	{
		throw Exception("Replication of column FixedArray is not implemented.", ErrorCodes::NOT_IMPLEMENTED);
	}

	void permute(const Permutation & perm)
	{
		size_t size = this->size();
		if (size != perm.size())
			throw Exception("Size of permutation doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		if (size == 0)
			return;

		Permutation nested_perm(size * n);
		for (size_t i = 0; i < size; ++i)
			for (size_t j = 0; j < n; ++j)
				nested_perm[i * n + j] = perm[i] * n + j;
		data->permute(nested_perm);
	}

	int compareAt(size_t p1, size_t p2, const IColumn & rhs_) const
	{
		const ColumnFixedArray & rhs = static_cast<const ColumnFixedArray &>(rhs_);

		/// Не оптимально
		for (size_t i = 0; i < n; ++i)
			if (int res = data->compareAt(p1 * n + i, p2 * n + i, *rhs.data))
				return res;

		return 0;
	}

	struct less
	{
		const ColumnFixedArray & parent;
		const Permutation & nested_perm;
		
		less(const ColumnFixedArray & parent_, const Permutation & nested_perm_) : parent(parent_), nested_perm(nested_perm_) {}
		bool operator()(size_t lhs, size_t rhs) const
		{
			for (size_t i = 0; i < parent.n; ++i)
			{
				if (nested_perm[lhs * parent.n + i] < nested_perm[rhs * parent.n + i])
					return true;
				else if (nested_perm[lhs * parent.n + i] > nested_perm[rhs * parent.n + i])
					return false;
			}
			return false;
		}
	};

	Permutation getPermutation() const
	{
		Permutation nested_perm = data->getPermutation();
		size_t s = data->size() / n;
		Permutation res(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;

		std::sort(res.begin(), res.end(), less(*this, nested_perm));
		return res;
	}

	void reserve(size_t elems)
	{
		data->reserve(n * elems);
	}

	size_t byteSize() const
	{
		return data->byteSize() + sizeof(n);
	}

	const IColumn & getData() const
	{
		return *data;
	}

	size_t getN() const
	{
		return n;
	}

protected:
	ColumnPtr data;
	const size_t n;
};


}

#endif
