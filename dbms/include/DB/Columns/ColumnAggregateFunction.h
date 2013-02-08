#pragma once

#include <DB/Common/Arena.h>

#include <DB/AggregateFunctions/IAggregateFunction.h>

#include <DB/Columns/ColumnVector.h>


namespace DB
{

/** Столбец, хранящий состояния агрегатных функций.
  * Состояния агрегатных функций хранятся в пуле (arena),
  *  (возможно, в нескольких)
  *  а в массиве (ColumnVector) хранятся указатели на них.
  * Столбец захватывает владение пулом и всеми агрегатными функциями,
  *  которые в него переданы (уничтожает их в дестркуторе).
  */
class ColumnAggregateFunction : public ColumnVector<AggregateDataPtr>
{
private:
	typedef SharedPtr<Arena> ArenaPtr;
	typedef std::vector<ArenaPtr> Arenas;
	
	const AggregateFunctionPtr func;
	Arenas arenas;
public:
	ColumnAggregateFunction(AggregateFunctionPtr & func_)
	{
		set(func_);
	}

	void set(AggregateFunctionPtr & func_)
	{
		func = func_;
	}

	/// Захватить владение ареной.
	void addArena(ArenaPtr & arena_)
	{
		arenas.push_back(arena_);
	}
	
    ~ColumnAggregateFunction()
	{
		for (size_t i = 0, s = data.size(); i < s; ++i)
			func->destroy(data[i]);
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
		throw Exception("Method insert is not supported for ColumnAggregateFunction. You must access underlying vector directly.",
			ErrorCodes::NOT_IMPLEMENTED);
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
