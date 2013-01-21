#pragma once

#include <DB/Columns/ColumnConst.h>
#include <DB/Interpreters/Set.h>


namespace DB
{

/** Столбец, содержащий множество значений в секции IN.
  * Ведёт себя как столбец-константа (так как множество одно, а не своё на каждую строку).
  * Значение у этого столбца нестандартное, поэтому его невозможно получить через обычный интерфейс.
  */
class ColumnSet : public IColumn
{
public:
	ColumnSet(size_t s_, SetPtr data_) : s(s_), data(data_) {}

	std::string getName() const { return "ColumnSet"; }
	ColumnPtr cloneEmpty() const { return new ColumnSet(0, NULL); }
	size_t size() const { return s; }
	Field operator[](size_t n) const { throw Exception("Cannot get value from ColumnSet", ErrorCodes::NOT_IMPLEMENTED); }
	void get(size_t n, Field & res) const { throw Exception("Cannot get value from ColumnSet", ErrorCodes::NOT_IMPLEMENTED); };
	void cut(size_t start, size_t length) { s = length; }
	void clear() { s = 0; data = NULL; }
	void insert(const Field & x) { throw Exception("Cannot insert element into ColumnSet", ErrorCodes::NOT_IMPLEMENTED); }
	void insertDefault() { ++s; }
	size_t byteSize() const { return 0; }
	int compareAt(size_t n, size_t m, const IColumn & rhs_) const { return 0; }
	StringRef getDataAt(size_t n) const { throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED); }

	void filter(const Filter & filt)
	{
		size_t new_size = 0;
		for (Filter::const_iterator it = filt.begin(); it != filt.end(); ++it)
			if (*it)
				++new_size;
		s = new_size;
	}

	void permute(const Permutation & perm)
	{
		if (s != perm.size())
			throw Exception("Size of permutation doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
	}

	Permutation getPermutation() const
	{
		Permutation res(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;
		return res;
	}

	void replicate(const Offsets_t & offsets)
	{
		if (s != offsets.size())
			throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
	}


	SetPtr & getData() { return data; }
	const SetPtr & getData() const { return data; }

private:
	size_t s;
	SetPtr data;
};

}
