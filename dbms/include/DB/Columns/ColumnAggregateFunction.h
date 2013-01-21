#pragma once

#include <DB/AggregateFunctions/IAggregateFunction.h>

#include <DB/Columns/ColumnVector.h>


namespace DB
{

/** Столбец, хранящий состояния агрегатных функций.
  */
class ColumnAggregateFunction : public ColumnVector<AggregateFunctionPtr, std::vector<AggregateFunctionPtr> >
{
public:
 	std::string getName() const { return "ColumnAggregateFunction"; }

 	ColumnPtr cloneEmpty() const { return new ColumnAggregateFunction; };

	bool isNumeric() const { return false; }

	Field operator[](size_t n) const
	{
		return data[n];
	}

	void get(size_t n, Field & res) const
	{
		res = data[n];
	}

	StringRef getDataAt(size_t n) const
	{
		throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}
	
	void cut(size_t start, size_t length)
	{
		if (start + length > data.size())
			throw Exception("Parameters start = "
				+ Poco::NumberFormatter::format(start) + ", length = "
				+ Poco::NumberFormatter::format(length) + " are out of bound in IColumnVector<T>::cut() method"
				" (data.size() = " + Poco::NumberFormatter::format(data.size()) + ").",
				ErrorCodes::PARAMETER_OUT_OF_BOUND);

		if (start == 0)
			data.resize(length);
		else
		{
			Container_t tmp(data.begin() + start, data.begin() + start + length);
			tmp.swap(data);
		}
	}

	void insert(const Field & x)
	{
		data.push_back(DB::get<const AggregateFunctionPtr &>(x));
	}

	int compareAt(size_t n, size_t m, const IColumn & rhs_) const
	{
		return 0;
	}

	void getPermutation(Permutation & res) const
	{
		size_t s = data.size();
		res.resize(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;
	}
};


}
