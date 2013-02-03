#pragma once

#include <DB/AggregateFunctions/IAggregateFunction.h>

#include <DB/Columns/ColumnVector.h>


namespace DB
{

/** Столбец, хранящий состояния агрегатных функций.
  * Для оптимизации, агрегатные функции хранятся по обычным указателям, а не shared_ptr-ам.
  * Столбец захватывает владение всеми агрегатными функциями, которые в него переданы
  *  (уничтожает их в дестркуторе с помощью delete).
  * Это значит, что вставляемые агрегатные функции должны быть выделены с помощью new,
  *  и не могут быть захвачены каком-либо smart-ptr-ом.
  */
class ColumnAggregateFunction : public ColumnVector<AggregateFunctionPlainPtr>
{
public:
    ~ColumnAggregateFunction()
	{
		for (size_t i = 0, s = data.size(); i < s; ++i)
			delete data[i];
	}
	
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
		data.push_back(DB::get<AggregateFunctionPlainPtr>(x));
	}

	int compareAt(size_t n, size_t m, const IColumn & rhs_) const
	{
		return 0;
	}

	Permutation getPermutation() const
	{
		size_t s = data.size();
		Permutation res(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;
		return res;
	}
};


}
