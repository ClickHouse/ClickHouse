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
class ColumnAggregateFunction final : public IColumn
{
public:
	typedef PODArray<AggregateDataPtr> Container_t;

private:
	Arenas arenas;			/// Пулы, в которых выделены состояния агрегатных функций.

	struct Holder
	{
		typedef SharedPtr<Holder> Ptr;

		AggregateFunctionPtr func;	/// Используется для уничтожения состояний и для финализации значений.
		const Ptr src;		/// Источник. Используется, если данный столбец создан из другого и использует все или часть его значений.
		Container_t data;	/// Массив указателей на состояния агрегатных функций, расположенных в пулах.

		Holder(const AggregateFunctionPtr & func_) : func(func_) {}
		Holder(const Ptr & src_) : func(src_->func), src(src_) {}

		~Holder()
		{
			IAggregateFunction * function = func;

			if (!function->hasTrivialDestructor() && src.isNull())
				for (auto val : data)
					function->destroy(val);
		}
	};

	Holder::Ptr holder;		/// NOTE Вместо этого можно было бы унаследовать IColumn от enable_shared_from_this.

	/// Создать столбец на основе другого.
	ColumnAggregateFunction(const ColumnAggregateFunction & src)
		: arenas(src.arenas), holder(new Holder(src.holder))
	{
	}

public:
	ColumnAggregateFunction(const AggregateFunctionPtr & func_)
		: holder(new Holder(func_))
	{
	}

	ColumnAggregateFunction(const AggregateFunctionPtr & func_, const Arenas & arenas_)
		: arenas(arenas_), holder(new Holder(func_))
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
		arenas.push_back(arena_);
	}

	/** Преобразовать столбец состояний агрегатной функции в столбец с готовыми значениями результатов.
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
		return new ColumnAggregateFunction(holder->func, Arenas(1, new Arena));
	};

	Field operator[](size_t n) const override
	{
		Field field = String();
		{
			WriteBufferFromString buffer(field.get<String &>());
			holder.get()->func->serialize(getData()[n], buffer);
		}
		return field;
	}

	void get(size_t n, Field & res) const override
	{
		res = String();
		{
			WriteBufferFromString buffer(res.get<String &>());
			holder.get()->func->serialize(getData()[n], buffer);
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
		getData().push_back(static_cast<const ColumnAggregateFunction &>(src).getData()[n]);
	}

	/// Объединить состояние в последней строке с заданным
	void insertMergeFrom(const IColumn & src, size_t n)
	{
		holder.get()->func.get()->merge(getData().back(), static_cast<const ColumnAggregateFunction &>(src).getData()[n]);
	}

	void insert(const Field & x) override
	{
		IAggregateFunction * function = holder.get()->func;

		if (unlikely(arenas.empty()))
			arenas.emplace_back(new Arena);

		getData().push_back(arenas.back().get()->alloc(function->sizeOfData()));
		function->create(getData().back());
		ReadBufferFromString read_buffer(x.get<const String &>());
		function->deserializeMerge(getData().back(), read_buffer);
	}

	void insertDefault() override
	{
		throw Exception("Method insertDefault is not supported for ColumnAggregateFunction.", ErrorCodes::NOT_IMPLEMENTED);
	}

	StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override
	{
		throw Exception("Method serializeValueIntoArena is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	const char * deserializeAndInsertFromArena(const char * pos) override
	{
		throw Exception("Method deserializeAndInsertFromArena is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	size_t byteSize() const override
	{
		return getData().size() * sizeof(getData()[0]);
	}

	void insertRangeFrom(const IColumn & src, size_t start, size_t length) override
	{
		const ColumnAggregateFunction & src_concrete = static_cast<const ColumnAggregateFunction &>(src);

		if (start + length > src_concrete.getData().size())
			throw Exception("Parameters start = "
				+ toString(start) + ", length = "
				+ toString(length) + " are out of bound in ColumnAggregateFunction::insertRangeFrom method"
				" (data.size() = " + toString(src_concrete.getData().size()) + ").",
				ErrorCodes::PARAMETER_OUT_OF_BOUND);

		auto & data = getData();
		size_t old_size = data.size();
		data.resize(old_size + length);
		memcpy(&data[old_size], &src_concrete.getData()[start], length * sizeof(data[0]));
	}

	ColumnPtr filter(const Filter & filter, ssize_t result_size_hint) const override
	{
		size_t size = getData().size();
		if (size != filter.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		ColumnAggregateFunction * res_ = new ColumnAggregateFunction(*this);
		ColumnPtr res = res_;

		if (size == 0)
			return res;

		auto & res_data = res_->getData();

		if (result_size_hint)
			res_data.reserve(result_size_hint > 0 ? result_size_hint : size);

		for (size_t i = 0; i < size; ++i)
			if (filter[i])
				res_data.push_back(getData()[i]);

		/// Для экономии оперативки в случае слишком сильной фильтрации.
		if (res_data.size() * 2 < res_data.capacity())
			res_data = Container_t(res_data.cbegin(), res_data.cend());

		return res;
	}

	ColumnPtr permute(const Permutation & perm, size_t limit) const override
	{
		size_t size = getData().size();

		if (limit == 0)
			limit = size;
		else
			limit = std::min(size, limit);

		if (perm.size() < limit)
			throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		ColumnAggregateFunction * res_ = new ColumnAggregateFunction(*this);
		ColumnPtr res = res_;

		res_->getData().resize(limit);
		for (size_t i = 0; i < limit; ++i)
			res_->getData()[i] = getData()[perm[i]];

		return res;
	}

	ColumnPtr replicate(const Offsets_t & offsets) const override
	{
		throw Exception("Method replicate is not supported for ColumnAggregateFunction.", ErrorCodes::NOT_IMPLEMENTED);
	}

	void getExtremes(Field & min, Field & max) const override
	{
		throw Exception("Method getExtremes is not supported for ColumnAggregateFunction.", ErrorCodes::NOT_IMPLEMENTED);
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
		return holder.get()->data;
	}

	const Container_t & getData() const
	{
		return holder.get()->data;
	}
};


}
