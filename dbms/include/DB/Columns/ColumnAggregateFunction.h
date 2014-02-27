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
class ColumnAggregateFunction : public ColumnVectorBase<AggregateDataPtr>
{
private:
	AggregateFunctionPtr func;	/// Используется для уничтожения состояний и для финализации значений.
	Arenas arenas;
public:
	ColumnAggregateFunction(const AggregateFunctionPtr & func_)
		: func(func_)
	{
	}

	ColumnAggregateFunction(const AggregateFunctionPtr & func_, const Arenas & arenas_)
		: func(func_), arenas(arenas_)
	{
	}

	void set(const AggregateFunctionPtr & func_)
	{
		func = func_;
	}

	AggregateFunctionPtr getAggregateFunction()
	{
		return func;
	}

	/// Захватить владение ареной.
	void addArena(ArenaPtr arena_)
	{
		arenas.push_back(arena_);
	}

	ColumnPtr convertToValues()
	{
		IAggregateFunction * function = func;
		ColumnPtr res = function->getReturnType()->createColumn();
		IColumn & column = *res;
		res->reserve(data.size());

		for (size_t i = 0; i < data.size(); ++i)
		{
			function->insertResultInto(data[i], column);
		}

		return res;
	}

	~ColumnAggregateFunction()
	{
		if (!func->hasTrivialDestructor())
			for (size_t i = 0, s = data.size(); i < s; ++i)
				func->destroy(data[i]);
	}
	
 	std::string getName() const { return "ColumnAggregateFunction"; }

 	ColumnPtr cloneEmpty() const { return new ColumnAggregateFunction(func, arenas); };

	bool isNumeric() const { return false; }

	Field operator[](size_t n) const
	{
		throw Exception("Method operator[] is not supported for ColumnAggregateFunction. You must access underlying vector directly.", ErrorCodes::NOT_IMPLEMENTED);;
	}

	void get(size_t n, Field & res) const
	{
		throw Exception("Method get is not supported for ColumnAggregateFunction. You must access underlying vector directly.", ErrorCodes::NOT_IMPLEMENTED);;
	}

	StringRef getDataAt(size_t n) const
	{
		throw Exception("Method getDataAt is not supported for ColumnAggregateFunction. You must access underlying vector directly.", ErrorCodes::NOT_IMPLEMENTED);
	}

	void insertData(const char * pos, size_t length)
	{
		throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}
	
	ColumnPtr cut(size_t start, size_t length) const
	{
		throw Exception("Method cut is not supported for ColumnAggregateFunction.", ErrorCodes::NOT_IMPLEMENTED);
	}

	ColumnPtr filter(const Filter & filter) const
	{
		throw Exception("Method filter is not supported for ColumnAggregateFunction.", ErrorCodes::NOT_IMPLEMENTED);
	}

	ColumnPtr permute(const Permutation & perm, size_t limit) const
	{
		throw Exception("Method permute is not supported for ColumnAggregateFunction.", ErrorCodes::NOT_IMPLEMENTED);
	}

	ColumnPtr replicate(const Offsets_t & offsets) const
	{
		throw Exception("Method replicate is not supported for ColumnAggregateFunction.", ErrorCodes::NOT_IMPLEMENTED);
	}

	void insert(const Field & x)
	{
		throw Exception("Method insert is not supported for ColumnAggregateFunction. You must access underlying vector directly.",
			ErrorCodes::NOT_IMPLEMENTED);
	}

	void getExtremes(Field & min, Field & max) const
	{
		throw Exception("Method getExtremes is not supported for ColumnAggregateFunction.", ErrorCodes::NOT_IMPLEMENTED);
	}

	int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const
	{
		return 0;
	}

	void getPermutation(bool reverse, size_t limit, Permutation & res) const
	{
		size_t s = data.size();
		res.resize(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;
	}
};


}
