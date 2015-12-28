#pragma once

#include <DB/Columns/IColumn.h>
#include <DB/Columns/ColumnsCommon.h>


namespace DB
{

/** Базовый класс для столбцов-констант, содержащих значение, не входящее в Field.
  * Не является полноценым столбцом и используется особым образом.
  */
class IColumnDummy : public IColumn
{
public:
	IColumnDummy(size_t s_) : s(s_) {}

	virtual ColumnPtr cloneDummy(size_t s_) const = 0;

	ColumnPtr cloneResized(size_t s_) const override { return cloneDummy(s_); }
	bool isConst() const override { return true; }
	size_t size() const override { return s; }
	void insertDefault() override { ++s; }
	size_t byteSize() const override { return 0; }
	int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override { return 0; }

	Field operator[](size_t n) const override { throw Exception("Cannot get value from " + getName(), ErrorCodes::NOT_IMPLEMENTED); }
	void get(size_t n, Field & res) const override { throw Exception("Cannot get value from " + getName(), ErrorCodes::NOT_IMPLEMENTED); };
	void insert(const Field & x) override { throw Exception("Cannot insert element into " + getName(), ErrorCodes::NOT_IMPLEMENTED); }
	StringRef getDataAt(size_t n) const override { throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED); }
	void insertData(const char * pos, size_t length) override { throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED); }

	StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override
	{
		throw Exception("Method serializeValueIntoArena is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	const char * deserializeAndInsertFromArena(const char * pos) override
	{
		throw Exception("Method deserializeAndInsertFromArena is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	void getExtremes(Field & min, Field & max) const override
	{
		throw Exception("Method getExtremes is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	void insertRangeFrom(const IColumn & src, size_t start, size_t length) override
	{
		s += length;
	}

	ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override
	{
		return cloneDummy(countBytesInFilter(filt));
	}

	ColumnPtr permute(const Permutation & perm, size_t limit) const override
	{
		if (s != perm.size())
			throw Exception("Size of permutation doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		return cloneDummy(limit ? std::min(s, limit) : s);
	}

	void getPermutation(bool reverse, size_t limit, Permutation & res) const override
	{
		res.resize(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;
	}

	ColumnPtr replicate(const Offsets_t & offsets) const override
	{
		if (s != offsets.size())
			throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		return cloneDummy(s == 0 ? 0 : offsets.back());
	}

private:
	size_t s;
};

}
