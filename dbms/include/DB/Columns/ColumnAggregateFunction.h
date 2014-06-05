#pragma once

#include <DB/Common/Arena.h>

#include <DB/AggregateFunctions/IAggregateFunction.h>

#include <DB/Columns/ColumnVector.h>

#include <DB/Core/Field.h>

#include <DB/IO/ReadBufferFromString.h>

namespace DB
{


/** Отвечает за владение и уничтожение состояний агрегатных функций.
  * В ColumnAggregateFunction хранится SharedPtr на него,
  *  чтобы уничтожить все состояния агрегатных функций только после того,
  *  как они перестали быть нужными во всех столбцах, использующих общие состояния.
  *
  * Состояния агрегатных функций выделены в пуле (arena) (возможно, в нескольких).
  *  а в массиве (data) хранятся указатели на них.
  */
class AggregateStatesHolder
{
public:
	AggregateFunctionPtr func;	/// Используется для уничтожения состояний и для финализации значений.
	Arenas arenas;
	PODArray<AggregateDataPtr> data;

	~AggregateStatesHolder()
	{
		IAggregateFunction * function = func;

		if (!function->hasTrivialDestructor())
			for (size_t i = 0, s = data.size(); i < s; ++i)
				function->destroy(data[i]);
	}
};


/** Столбец состояний агрегатных функций.
  */
class ColumnAggregateFunction final : public ColumnVectorBase<AggregateDataPtr>
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

	AggregateFunctionPtr getAggregateFunction() { return func; }
	AggregateFunctionPtr getAggregateFunction() const { return func; }

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
			function->insertResultInto(data[i], column);

		return res;
	}

	~ColumnAggregateFunction()
	{
		std::cerr << __PRETTY_FUNCTION__ << " " << this << std::endl;

		IAggregateFunction * function = func;

		if (!function->hasTrivialDestructor())
			for (size_t i = 0, s = data.size(); i < s; ++i)
				function->destroy(data[i]);
	}
	
 	std::string getName() const { return "ColumnAggregateFunction"; }

 	ColumnPtr cloneEmpty() const { return new ColumnAggregateFunction(func, arenas); };

	bool isNumeric() const { return false; }

	Field operator[](size_t n) const
	{
		Field field = String();
		{
			WriteBufferFromString buffer(field.get<String &>());
			func->serialize(data[n], buffer);
		}
		return field;
	}

	void get(size_t n, Field & res) const
	{
		res.assignString("", 0);
		{
			WriteBufferFromString buffer(res.get<String &>());
			func->serialize(data[n], buffer);
		}
	}

	StringRef getDataAt(size_t n) const
	{
		return StringRef(data[n], sizeof(data[n]));
	}

	/// Объединить состояние в последней строке с заданным
	void insertMerge(StringRef input)
	{
		func->merge(data.back(), input.data);
	}

	void insert(const Field & x)
	{
		if (arenas.empty())
			arenas.push_back(new Arena());
		data.push_back(arenas.back()->alloc(func->sizeOfData()*10));
		func->create(data.back());
		ReadBufferFromString read_buffer(x.safeGet<const String &>());
		func->deserializeMerge(data.back(), read_buffer);
	}

	void insertData(const char * pos, size_t length)
	{
		data.push_back(*reinterpret_cast<const AggregateDataPtr *>(pos));
	}

	ColumnPtr cut(size_t start, size_t length) const
	{
		if (start + length > this->data.size())
			throw Exception("Parameters start = "
				+ toString(start) + ", length = "
				+ toString(length) + " are out of bound in ColumnAggregateFunction::cut() method"
				" (data.size() = " + toString(this->data.size()) + ").",
				ErrorCodes::PARAMETER_OUT_OF_BOUND);

		ColumnAggregateFunction * res_ = new ColumnAggregateFunction(func, arenas);
		ColumnPtr res = res_;
		res_->data.resize(length);
		for (size_t i = 0; i < length; ++i)
			res_->data[i] = this->data[start+i];
		return res;
	}

	ColumnPtr filter(const Filter & filter) const
	{
		size_t size = data.size();
		if (size != filter.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		ColumnAggregateFunction * res_ = new ColumnAggregateFunction(func, arenas);
		ColumnPtr res = res_;

		if (size == 0)
			return res;

		res_->data.reserve(size);
		for (size_t i = 0; i < size; ++i)
			if (filter[i])
				res_->data.push_back(this->data[i]);

		return res;
	}

	ColumnPtr permute(const Permutation & perm, size_t limit) const
	{
		size_t size = this->data.size();

		if (limit == 0)
			limit = size;
		else
			limit = std::min(size, limit);

		if (perm.size() < limit)
			throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		ColumnAggregateFunction * res_ = new ColumnAggregateFunction(func, arenas);
		ColumnPtr res = res_;
		res_->data.resize(limit);
		for (size_t i = 0; i < limit; ++i)
			res_->data[i] = this->data[perm[i]];
		return res;
	}

	ColumnPtr replicate(const Offsets_t & offsets) const
	{
		throw Exception("Method replicate is not supported for ColumnAggregateFunction.", ErrorCodes::NOT_IMPLEMENTED);
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
