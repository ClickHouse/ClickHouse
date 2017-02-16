#pragma once
#include <DB/Columns/ColumnConst.h>
#include <DB/DataTypes/DataTypeAggregateFunction.h>


namespace DB
{

class ColumnConstAggregateFunction : public IColumnConst
{
public:

	ColumnConstAggregateFunction(size_t size, const Field & value_, const DataTypePtr & data_type_)
	: data_type(data_type_), value(value_), s(size)
	{
	}

	String getName() const override
	{
		return "ColumnConst<ColumnAggregateFunction>";
	}

	bool isConst() const override
	{
		return true;
	}

	ColumnPtr convertToFullColumnIfConst() const override
	{
		return convertToFullColumn();
	}

	ColumnPtr convertToFullColumn() const override
	{
		auto res = std::make_shared<ColumnAggregateFunction>(getAggregateFunction());

		for (size_t i = 0; i < s; ++i)
			res->insert(value);

		return res;
	}

	ColumnPtr cloneResized(size_t new_size) const override
	{
		return std::make_shared<ColumnConstAggregateFunction>(new_size, value, data_type);
	}

	size_t size() const override
	{
		return s;
	}

	Field operator[](size_t n) const override
	{
		/// NOTE: there are no out of bounds check (like in ColumnConstBase)
		return value;
	}

	void get(size_t n, Field & res) const override
	{
		res = value;
	}

	StringRef getDataAt(size_t n) const override
	{
		return value.get<const String &>();
	}

	void insert(const Field & x) override
	{
		/// NOTE: Cannot check source function of x
		if (value != x)
			throw Exception("Cannot insert different element into constant column " + getName(),
				ErrorCodes::CANNOT_INSERT_ELEMENT_INTO_CONSTANT_COLUMN);
		++s;
	}

	void insertRangeFrom(const IColumn & src, size_t start, size_t length) override
	{
		if (!equalsFuncAndValue(src))
			throw Exception("Cannot insert different element into constant column " + getName(),
				ErrorCodes::CANNOT_INSERT_ELEMENT_INTO_CONSTANT_COLUMN);

		s += length;
	}

	void insertData(const char * pos, size_t length) override
	{
		throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	void insertDefault() override
	{
		++s;
	}

	void popBack(size_t n) override
	{
		s -= n;
	}

	StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override
	{
		throw Exception("Method serializeValueIntoArena is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	const char * deserializeAndInsertFromArena(const char * pos) override
	{
		throw Exception("Method deserializeAndInsertFromArena is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	void updateHashWithValue(size_t n, SipHash & hash) const override
	{
		throw Exception("Method updateHashWithValue is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override
	{
		if (s != filt.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		return std::make_shared<ColumnConstAggregateFunction>(countBytesInFilter(filt), value, data_type);
	}

	ColumnPtr permute(const Permutation & perm, size_t limit) const override
	{
		if (limit == 0)
			limit = s;
		else
			limit = std::min(s, limit);

		if (perm.size() < limit)
			throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		return std::make_shared<ColumnConstAggregateFunction>(limit, value, data_type);
	}

	int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override
	{
		return 0;
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

		size_t replicated_size = 0 == s ? 0 : offsets.back();
		return std::make_shared<ColumnConstAggregateFunction>(replicated_size, value, data_type);
	}

	void getExtremes(Field & min, Field & max) const override
	{
		min = value;
		max = value;
	}

	size_t byteSize() const override
	{
		return sizeof(value) + sizeof(s);
	}

	size_t allocatedSize() const override
	{
		return byteSize();
	}

private:

	DataTypePtr data_type;
	Field value;
	size_t s;

	AggregateFunctionPtr getAggregateFunction() const
	{
		return typeid_cast<const DataTypeAggregateFunction &>(*data_type).getFunction();
	}

	bool equalsFuncAndValue(const IColumn & rhs) const
	{
		auto rhs_const = dynamic_cast<const ColumnConstAggregateFunction *>(&rhs);
		return rhs_const && value == rhs_const->value && data_type->equals(*rhs_const->data_type);
	}
};


}
