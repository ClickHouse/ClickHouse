#pragma once

#include <DB/Common/Arena.h>

#include <DB/AggregateFunctions/IAggregateFunction.h>

#include <DB/Columns/IColumn.h>

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

	AggregateStatesHolder(const AggregateFunctionPtr & func_) : func(func_) {}
	AggregateStatesHolder(const AggregateFunctionPtr & func_, const Arenas & arenas_) : func(func_), arenas(arenas_) {}

	~AggregateStatesHolder()
	{
		IAggregateFunction * function = func;

		if (!function->hasTrivialDestructor())
			for (size_t i = 0, s = data.size(); i < s; ++i)
				function->destroy(data[i]);
	}

	/// Захватить владение ареной.
	void addArena(ArenaPtr arena_)
	{
		arenas.push_back(arena_);
	}
};

typedef SharedPtr<AggregateStatesHolder> AggregateStatesHolderPtr;


/** Столбец состояний агрегатных функций.
  */
class ColumnAggregateFunction final : public IColumn
{
public:
	typedef PODArray<AggregateDataPtr> Container_t;

private:
	AggregateStatesHolderPtr holder;

	/** Массив (указателей на состояния агрегатных функций) может
	  *  либо лежать в holder-е (пример - только что созданный столбец со всеми значениями),
	  *  либо быть своим (пример - столбец отфильтрованных значений - тогда свой массив содержит только то, что отфильтровано).
	  */
	Container_t own_data;
	Container_t * data;		/// Указывает на AggregateStatesHolder::data или на own_data.

public:
	ColumnAggregateFunction(AggregateStatesHolderPtr holder_)
		: holder(holder_), data(&holder->data)
	{
	}

	void set(const AggregateFunctionPtr & func_)
	{
		holder->func = func_;
	}

	AggregateFunctionPtr getAggregateFunction() { return holder->func; }
	AggregateFunctionPtr getAggregateFunction() const { return holder->func; }

	/// Захватить владение ареной.
	void addArena(ArenaPtr arena_)
	{
		holder->addArena(arena_);
	}

	ColumnPtr convertToValues()
	{
		IAggregateFunction * function = holder->func;
		ColumnPtr res = function->getReturnType()->createColumn();
		IColumn & column = *res;
		res->reserve(data->size());

		for (size_t i = 0; i < data->size(); ++i)
			function->insertResultInto((*data)[i], column);

		return res;
	}

 	std::string getName() const { return "ColumnAggregateFunction"; }

	size_t sizeOfField() const { return sizeof(own_data[0]); }

	size_t size() const
	{
		return data->size();
	}

 	ColumnPtr cloneEmpty() const
 	{
		auto res = new ColumnAggregateFunction(const_cast<AggregateStatesHolderPtr &>(holder));
		res->data = &res->own_data;
		return res;
	};

	Field operator[](size_t n) const
	{
		Field field = String();
		{
			WriteBufferFromString buffer(field.get<String &>());
			holder->func->serialize((*data)[n], buffer);
		}
		return field;
	}

	void get(size_t n, Field & res) const
	{
		res = String();
		{
			WriteBufferFromString buffer(res.get<String &>());
			holder->func->serialize((*data)[n], buffer);
		}
	}

	StringRef getDataAt(size_t n) const
	{
		return StringRef((*data)[n], sizeof((*data)[n]));
	}

	/// Объединить состояние в последней строке с заданным
	void insertMerge(StringRef input)
	{
		holder.get()->func.get()->merge(data->back(), input.data);
	}

	void insert(const Field & x)
	{
		if (holder.get()->arenas.empty())
			holder.get()->addArena(new Arena());

		IAggregateFunction * function = holder->func;

		data->push_back(holder.get()->arenas.back()->alloc(function->sizeOfData()));
		function->create(data->back());
		ReadBufferFromString read_buffer(x.get<const String &>());
		function->deserializeMerge(data->back(), read_buffer);
	}

	void insertFrom(const IColumn & src, size_t n)
	{
		/// Должны совпадать holder-ы.
		if (unlikely(holder.get() != static_cast<const ColumnAggregateFunction &>(src).holder.get()))
			throw Exception("Columns with aggregates must hold same data", ErrorCodes::LOGICAL_ERROR);

		data->push_back(static_cast<const ColumnAggregateFunction &>(src).getData()[n]);
	}

	void insertDefault()
	{
		throw Exception("Method insertDefault is not supported for ColumnAggregateFunction.", ErrorCodes::NOT_IMPLEMENTED);
	}

	size_t byteSize() const
	{
		return data->size() * sizeof((*data)[0]);
	}

	void insertData(const char * pos, size_t length)
	{
		data->push_back(*reinterpret_cast<const AggregateDataPtr *>(pos));
	}

	ColumnPtr cut(size_t start, size_t length) const
	{
		if (start + length > data->size())
			throw Exception("Parameters start = "
				+ toString(start) + ", length = "
				+ toString(length) + " are out of bound in ColumnAggregateFunction::cut() method"
				" (data.size() = " + toString(data->size()) + ").",
				ErrorCodes::PARAMETER_OUT_OF_BOUND);

		ColumnPtr res = cloneEmpty();
		ColumnAggregateFunction * res_ = static_cast<ColumnAggregateFunction *>(res.get());

		res_->data->resize(length);
		for (size_t i = 0; i < length; ++i)
			(*res_->data)[i] = (*data)[start + i];
		return res;
	}

	ColumnPtr filter(const Filter & filter) const
	{
		size_t size = data->size();
		if (size != filter.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		ColumnPtr res = cloneEmpty();
		ColumnAggregateFunction * res_ = static_cast<ColumnAggregateFunction *>(res.get());

		if (size == 0)
			return res;

		res_->data->reserve(size);
		for (size_t i = 0; i < size; ++i)
			if (filter[i])
				res_->data->push_back((*data)[i]);

		return res;
	}

	ColumnPtr permute(const Permutation & perm, size_t limit) const
	{
		size_t size = data->size();

		if (limit == 0)
			limit = size;
		else
			limit = std::min(size, limit);

		if (perm.size() < limit)
			throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		ColumnPtr res = cloneEmpty();
		ColumnAggregateFunction * res_ = static_cast<ColumnAggregateFunction *>(res.get());

		res_->data->resize(limit);
		for (size_t i = 0; i < limit; ++i)
			(*res_->data)[i] = (*data)[perm[i]];

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
		size_t s = data->size();
		res.resize(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;
	}

	/** Более эффективные методы манипуляции */
	Container_t & getData()
	{
		return *data;
	}

	const Container_t & getData() const
	{
		return *data;
	}
};


}
