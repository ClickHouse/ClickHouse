#pragma once

#include <DB/Common/Arena.h>

#include <DB/AggregateFunctions/IAggregateFunction.h>

#include <DB/Columns/IColumn.h>

#include <DB/Core/Field.h>

#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int PARAMETER_OUT_OF_BOUND;
	extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}


/** Столбец состояний агрегатных функций.
  * Представлен в виде массива указателей на состояния агрегатных функций (data).
  * Сами состояния хранятся в одном из пулов (arenas).
  *
  * Может быть в двух вариантах:
  *
  * 1. Владеть своими значениями - то есть, отвечать за их уничтожение.
  * Столбец состоит из значений, "отданных ему на попечение" после выполнения агрегации (см. Aggregator, функция convertToBlocks),
  *  или из значений, созданных им самим (см. метод insert).
  * В этом случае, src будет равно nullptr, и столбец будет сам уничтожать (вызывать IAggregateFunction::destroy)
  *  состояния агрегатных функций в деструкторе.
  *
  * 2. Не владеть своими значениями, а использовать значения, взятые из другого столбца ColumnAggregateFunction.
  * Например, это столбец, полученный перестановкой/фильтрацией или другими преобразованиями из другого столбца.
  * В этом случае, в src будет shared ptr-ом на столбец-источник. Уничтожением значений будет заниматься этот столбец-источник.
  *
  * Это решение несколько ограничено:
  * - не поддерживается вариант, в котором столбец содержит часть "своих" и часть "чужих" значений;
  * - не поддерживается вариант наличия нескольких столбцов-источников, что может понадобиться для более оптимального слияния двух столбцов.
  *
  * Эти ограничения можно снять, если добавить массив флагов или даже refcount-ов,
  *  определяющий, какие отдельные значения надо уничтожать, а какие - нет.
  * Ясно, что этот метод имел бы существенно ненулевую цену.
  */
class ColumnAggregateFunction final : public IColumn, public std::enable_shared_from_this<ColumnAggregateFunction>
{
public:
	using Container_t = PaddedPODArray<AggregateDataPtr>;

private:
	/// Memory pools. Aggregate states are allocated from them.
	Arenas arenas;

	/// Used for destroying states and for finalization of values.
	AggregateFunctionPtr func;

	/// Source column. Used (holds source from destruction),
	///  if this column has been constructed from another and uses all or part of its values.
	std::shared_ptr<const ColumnAggregateFunction> src;

	/// Array of pointers to aggregation states, that are placed in arenas.
	Container_t data;

public:
	/// Create a new column that has another column as a source.
	ColumnAggregateFunction(const ColumnAggregateFunction & other)
		: arenas(other.arenas), func(other.func), src(other.shared_from_this())
	{
	}

	ColumnAggregateFunction(const AggregateFunctionPtr & func_)
		: func(func_)
	{
	}

	ColumnAggregateFunction(const AggregateFunctionPtr & func_, const Arenas & arenas_)
		: arenas(arenas_), func(func_)
	{
	}

    ~ColumnAggregateFunction()
	{
		if (!func->hasTrivialDestructor() && !src)
			for (auto val : data)
				func->destroy(val);
	}

    void set(const AggregateFunctionPtr & func_)
	{
		func = func_;
	}

	AggregateFunctionPtr getAggregateFunction() { return func; }
	AggregateFunctionPtr getAggregateFunction() const { return func; }

	/// Take shared ownership of Arena, that holds memory for states of aggregate functions.
	void addArena(ArenaPtr arena_)
	{
		arenas.push_back(arena_);
	}

	/** Transform column with states of aggregate functions to column with final result values.
	  */
	ColumnPtr convertToValues() const;

	std::string getName() const override { return "ColumnAggregateFunction"; }

	size_t sizeOfField() const override { return sizeof(getData()[0]); }

	size_t size() const override
	{
		return getData().size();
	}

	ColumnPtr cloneEmpty() const override
	{
		return std::make_shared<ColumnAggregateFunction>(func, Arenas(1, std::make_shared<Arena>()));
	};

	Field operator[](size_t n) const override
	{
		Field field = String();
		{
			WriteBufferFromString buffer(field.get<String &>());
			func->serialize(getData()[n], buffer);
		}
		return field;
	}

	void get(size_t n, Field & res) const override
	{
		res = String();
		{
			WriteBufferFromString buffer(res.get<String &>());
			func->serialize(getData()[n], buffer);
		}
	}

	StringRef getDataAt(size_t n) const override
	{
		return StringRef(reinterpret_cast<const char *>(&getData()[n]), sizeof(getData()[n]));
	}

	void insertData(const char * pos, size_t length) override
	{
		getData().push_back(*reinterpret_cast<const AggregateDataPtr *>(pos));
	}

	void insertFrom(const IColumn & src, size_t n) override
	{
		/// Must create new state of aggregate function and take ownership of it,
		///  because ownership of states of aggregate function cannot be shared for individual rows,
		///  (only as a whole, see comment above).
		insertDefault();
		insertMergeFrom(src, n);
	}

	/// Merge state at last row with specified state in another column.
	void insertMergeFrom(const IColumn & src, size_t n)
	{
		Arena & arena = createOrGetArena();
		func->merge(getData().back(), static_cast<const ColumnAggregateFunction &>(src).getData()[n], &arena);
	}

	Arena & createOrGetArena()
	{
		if (unlikely(arenas.empty()))
			arenas.emplace_back(std::make_shared<Arena>());
		return *arenas.back().get();
	}

	void insert(const Field & x) override
	{
		IAggregateFunction * function = func.get();

		Arena & arena = createOrGetArena();

		getData().push_back(arena.alloc(function->sizeOfData()));
		function->create(getData().back());
		ReadBufferFromString read_buffer(x.get<const String &>());
		function->deserialize(getData().back(), read_buffer, &arena);
	}

	void insertDefault() override
	{
		IAggregateFunction * function = func.get();

		Arena & arena = createOrGetArena();

		getData().push_back(arena.alloc(function->sizeOfData()));
		function->create(getData().back());
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

	size_t byteSize() const override;

	size_t allocatedSize() const override;

	void insertRangeFrom(const IColumn & from, size_t start, size_t length) override;

	void popBack(size_t n) override
	{
		size_t size = data.size();
		size_t new_size = size - n;

		if (!src)
			for (size_t i = new_size; i < size; ++i)
				func->destroy(data[i]);

		data.resize_assume_reserved(new_size);
	}

	ColumnPtr filter(const Filter & filter, ssize_t result_size_hint) const override;

	ColumnPtr permute(const Permutation & perm, size_t limit) const override;

	ColumnPtr replicate(const Offsets_t & offsets) const override
	{
		throw Exception("Method replicate is not supported for ColumnAggregateFunction.", ErrorCodes::NOT_IMPLEMENTED);
	}

	Columns scatter(ColumnIndex num_columns, const Selector & selector) const override
	{
		/// Columns with scattered values will point to this column as the owner of values.
		Columns columns(num_columns);
		for (auto & column : columns)
			column = std::make_shared<ColumnAggregateFunction>(*this);

		size_t num_rows = size();

		{
			size_t reserve_size = num_rows / num_columns * 1.1;	/// 1.1 is just a guess. Better to use n-sigma rule.

			if (reserve_size > 1)
				for (auto & column : columns)
					column->reserve(reserve_size);
		}

		for (size_t i = 0; i < num_rows; ++i)
			static_cast<ColumnAggregateFunction &>(*columns[selector[i]]).data.push_back(data[i]);

		return columns;
	}

	int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override
	{
		return 0;
	}

	void getPermutation(bool reverse, size_t limit, Permutation & res) const override
	{
		size_t s = getData().size();
		res.resize(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;
	}

	/** Более эффективные методы манипуляции */
	Container_t & getData()
	{
		return data;
	}

	const Container_t & getData() const
	{
		return data;
	}

	void getExtremes(Field & min, Field & max) const override
	{
		throw Exception("Method getExtremes is not supported for ColumnAggregateFunction.", ErrorCodes::NOT_IMPLEMENTED);
	}
};


}
