#pragma once

#include <DB/Common/PODArray.h>

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeArray.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>
#include <DB/AggregateFunctions/QuantilesCommon.h>

#include <DB/Columns/ColumnArray.h>


namespace DB
{


/** В качестве состояния используется массив, в который складываются все значения.
  * NOTE Если различных значений мало, то это не оптимально.
  * Для 8 и 16-битных значений возможно, было бы лучше использовать lookup-таблицу.
  */
template <typename T>
struct AggregateFunctionQuantileExactData
{
	/// Сразу будет выделена память на несколько элементов так, чтобы состояние занимало 64 байта.
	static constexpr size_t bytes_in_arena = 64 - sizeof(PODArray<T>);

	using Array = PODArray<T, bytes_in_arena, AllocatorWithStackMemory<Allocator<false>, bytes_in_arena>>;
	Array array;
};


/** Точно вычисляет квантиль.
  * В качестве типа аргумента может быть только числовой тип (в том числе, дата и дата-с-временем).
  * Тип результата совпадает с типом аргумента.
  */
template <typename T>
class AggregateFunctionQuantileExact final
	: public IUnaryAggregateFunction<AggregateFunctionQuantileExactData<T>, AggregateFunctionQuantileExact<T>>
{
private:
	double level;
	DataTypePtr type;

public:
	AggregateFunctionQuantileExact(double level_ = 0.5) : level(level_) {}

	String getName() const override { return "quantileExact"; }

	DataTypePtr getReturnType() const override
	{
		return type;
	}

	void setArgument(const DataTypePtr & argument)
	{
		type = argument;
	}

	void setParameters(const Array & params) override
	{
		if (params.size() != 1)
			throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		level = apply_visitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
	}

	void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		this->data(place).array.push_back(static_cast<const ColumnVector<T> &>(column).getData()[row_num]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		this->data(place).array.insert(this->data(rhs).array.begin(), this->data(rhs).array.end());
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		const auto & array = this->data(place).array;

		size_t size = array.size();
		writeVarUInt(size, buf);
		buf.write(reinterpret_cast<const char *>(&array[0]), size * sizeof(array[0]));
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		auto & array = this->data(place).array;

		size_t size = 0;
		readVarUInt(size, buf);
		array.resize(size);
		buf.read(reinterpret_cast<char *>(&array[0]), size * sizeof(array[0]));
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		/// Сортировка массива не будет считаться нарушением константности.
		auto & array = const_cast<typename AggregateFunctionQuantileExactData<T>::Array &>(this->data(place).array);

		T quantile = T();

		if (!array.empty())
		{
			size_t n = level < 1
				? level * array.size()
				: (array.size() - 1);

			std::nth_element(array.begin(), array.begin() + n, array.end());	/// NOTE Можно придумать алгоритм radix-select.

			quantile = array[n];
		}

		static_cast<ColumnVector<T> &>(to).getData().push_back(quantile);
	}
};


/** То же самое, но позволяет вычислить сразу несколько квантилей.
  * Для этого, принимает в качестве параметров несколько уровней. Пример: quantilesExact(0.5, 0.8, 0.9, 0.95)(ConnectTiming).
  * Возвращает массив результатов.
  */
template <typename T>
class AggregateFunctionQuantilesExact final
	: public IUnaryAggregateFunction<AggregateFunctionQuantileExactData<T>, AggregateFunctionQuantilesExact<T>>
{
private:
	QuantileLevels<double> levels;
	DataTypePtr type;

public:
	String getName() const override { return "quantilesExact"; }

	DataTypePtr getReturnType() const override
	{
		return new DataTypeArray(type);
	}

	void setArgument(const DataTypePtr & argument)
	{
		type = argument;
	}

	void setParameters(const Array & params) override
	{
		levels.set(params);
	}

	void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		this->data(place).array.push_back(static_cast<const ColumnVector<T> &>(column).getData()[row_num]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		this->data(place).array.insert(this->data(rhs).array.begin(), this->data(rhs).array.end());
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		const auto & array = this->data(place).array;

		size_t size = array.size();
		writeVarUInt(size, buf);
		buf.write(reinterpret_cast<const char *>(&array[0]), size * sizeof(array[0]));
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		auto & array = this->data(place).array;

		size_t size = 0;
		readVarUInt(size, buf);
		array.resize(size);
		buf.read(reinterpret_cast<char *>(&array[0]), size * sizeof(array[0]));
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		/// Сортировка массива не будет считаться нарушением константности.
		auto & array = const_cast<typename AggregateFunctionQuantileExactData<T>::Array &>(this->data(place).array);

		ColumnArray & arr_to = static_cast<ColumnArray &>(to);
		ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();

		size_t num_levels = levels.size();
		offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + num_levels);

		typename ColumnVector<T>::Container_t & data_to = static_cast<ColumnVector<T> &>(arr_to.getData()).getData();
		size_t old_size = data_to.size();
		data_to.resize(old_size + num_levels);

		if (!array.empty())
		{
			size_t prev_n = 0;
			for (auto level_index : levels.permutation)
			{
				auto level = levels.levels[level_index];

				size_t n = level < 1
					? level * array.size()
					: (array.size() - 1);

				std::nth_element(array.begin() + prev_n, array.begin() + n, array.end());

				data_to[old_size + level_index] = array[n];
				prev_n = n;
			}
		}
		else
		{
			for (size_t i = 0; i < num_levels; ++i)
				data_to[old_size + i] = T();
		}
	}
};

}
