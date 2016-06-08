#pragma once

#include <DB/Common/HashTable/HashMap.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeArray.h>

#include <DB/AggregateFunctions/IBinaryAggregateFunction.h>
#include <DB/AggregateFunctions/QuantilesCommon.h>

#include <DB/Columns/ColumnArray.h>


namespace DB
{


/** В качестве состояния используется хэш-таблица вида: значение -> сколько раз встретилось.
  */
template <typename T>
struct AggregateFunctionQuantileExactWeightedData
{
	using Key = T;
	using Weight = UInt64;

	/// При создании, хэш-таблица должна быть небольшой.
	using Map = HashMap<
		Key, Weight,
		HashCRC32<Key>,
		HashTableGrower<4>,
		HashTableAllocatorWithStackMemory<sizeof(std::pair<Key, Weight>) * (1 << 3)>
	>;

	Map map;
};


/** Точно вычисляет квантиль по множеству значений, для каждого из которых задан вес - сколько раз значение встречалось.
  * Можно рассматривать набор пар value, weight - как набор гистограмм,
  *  в которых value - значение, округлённое до середины столбика, а weight - высота столбика.
  * В качестве типа аргумента может быть только числовой тип (в том числе, дата и дата-с-временем).
  * Тип результата совпадает с типом аргумента.
  */
template <typename ValueType, typename WeightType>
class AggregateFunctionQuantileExactWeighted final
	: public IBinaryAggregateFunction<
		AggregateFunctionQuantileExactWeightedData<ValueType>,
		AggregateFunctionQuantileExactWeighted<ValueType, WeightType>>
{
private:
	double level;
	DataTypePtr type;

public:
	AggregateFunctionQuantileExactWeighted(double level_ = 0.5) : level(level_) {}

	String getName() const override { return "quantileExactWeighted"; }

	DataTypePtr getReturnType() const override
	{
		return type;
	}

	void setArgumentsImpl(const DataTypes & arguments)
	{
		type = arguments[0];
	}

	void setParameters(const Array & params) override
	{
		if (params.size() != 1)
			throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		level = apply_visitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
	}

	void addImpl(AggregateDataPtr place, const IColumn & column_value, const IColumn & column_weight, size_t row_num) const
	{
		this->data(place)
			.map[static_cast<const ColumnVector<ValueType> &>(column_value).getData()[row_num]]
			+= static_cast<const ColumnVector<WeightType> &>(column_weight).getData()[row_num];
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		auto & map = this->data(place).map;
		const auto & rhs_map = this->data(rhs).map;

		for (const auto & pair : rhs_map)
			map[pair.first] += pair.second;
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		this->data(place).map.write(buf);
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		typename AggregateFunctionQuantileExactWeightedData<ValueType>::Map::Reader reader(buf);

		auto & map = this->data(place).map;
		while (reader.next())
		{
			const auto & pair = reader.get();
			map[pair.first] = pair.second;
		}
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		auto & map = this->data(place).map;
		size_t size = map.size();

		if (0 == size)
		{
			static_cast<ColumnVector<ValueType> &>(to).getData().push_back(ValueType());
			return;
		}

		/// Копируем данные во временный массив, чтобы получить нужный по порядку элемент.
		using Pair = typename AggregateFunctionQuantileExactWeightedData<ValueType>::Map::value_type;
		std::unique_ptr<Pair[]> array_holder(new Pair[size]);
		Pair * array = array_holder.get();

		size_t i = 0;
		UInt64 sum_weight = 0;
		for (const auto & pair : map)
		{
			sum_weight += pair.second;
			array[i] = pair;
			++i;
		}

		std::sort(array, array + size, [](const Pair & a, const Pair & b) { return a.first < b.first; });

		UInt64 threshold = std::ceil(sum_weight * level);
		UInt64 accumulated = 0;

		const Pair * it = array;
		const Pair * end = array + size;
		while (it < end)
		{
			accumulated += it->second;

			if (accumulated >= threshold)
				break;

			++it;
		}

		if (it == end)
			--it;

		static_cast<ColumnVector<ValueType> &>(to).getData().push_back(it->first);
	}
};


/** То же самое, но позволяет вычислить сразу несколько квантилей.
  * Для этого, принимает в качестве параметров несколько уровней. Пример: quantilesExactWeighted(0.5, 0.8, 0.9, 0.95)(ConnectTiming, Weight).
  * Возвращает массив результатов.
  */
template <typename ValueType, typename WeightType>
class AggregateFunctionQuantilesExactWeighted final
	: public IBinaryAggregateFunction<
		AggregateFunctionQuantileExactWeightedData<ValueType>,
		AggregateFunctionQuantilesExactWeighted<ValueType, WeightType>>
{
private:
	QuantileLevels<double> levels;
	DataTypePtr type;

public:
	String getName() const override { return "quantilesExactWeighted"; }

	DataTypePtr getReturnType() const override
	{
		return std::make_shared<DataTypeArray>(type);
	}

	void setArgumentsImpl(const DataTypes & arguments)
	{
		type = arguments[0];
	}

	void setParameters(const Array & params) override
	{
		levels.set(params);
	}

	void addImpl(AggregateDataPtr place, const IColumn & column_value, const IColumn & column_weight, size_t row_num) const
	{
		this->data(place)
			.map[static_cast<const ColumnVector<ValueType> &>(column_value).getData()[row_num]]
			+= static_cast<const ColumnVector<WeightType> &>(column_weight).getData()[row_num];
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		auto & map = this->data(place).map;
		const auto & rhs_map = this->data(rhs).map;

		for (const auto & pair : rhs_map)
			map[pair.first] += pair.second;
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		this->data(place).map.write(buf);
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		typename AggregateFunctionQuantileExactWeightedData<ValueType>::Map::Reader reader(buf);

		auto & map = this->data(place).map;
		while (reader.next())
		{
			const auto & pair = reader.get();
			map[pair.first] = pair.second;
		}
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		auto & map = this->data(place).map;
		size_t size = map.size();

		ColumnArray & arr_to = static_cast<ColumnArray &>(to);
		ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();

		size_t num_levels = levels.size();
		offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + num_levels);

		typename ColumnVector<ValueType>::Container_t & data_to = static_cast<ColumnVector<ValueType> &>(arr_to.getData()).getData();

		size_t old_size = data_to.size();
		data_to.resize(old_size + num_levels);

		if (0 == size)
		{
			for (size_t i = 0; i < num_levels; ++i)
				data_to[old_size + i] = ValueType();
			return;
		}

		/// Копируем данные во временный массив, чтобы получить нужный по порядку элемент.
		using Pair = typename AggregateFunctionQuantileExactWeightedData<ValueType>::Map::value_type;
		std::unique_ptr<Pair[]> array_holder(new Pair[size]);
		Pair * array = array_holder.get();

		size_t i = 0;
		UInt64 sum_weight = 0;
		for (const auto & pair : map)
		{
			sum_weight += pair.second;
			array[i] = pair;
			++i;
		}

		std::sort(array, array + size, [](const Pair & a, const Pair & b) { return a.first < b.first; });

		UInt64 accumulated = 0;

		const Pair * it = array;
		const Pair * end = array + size;

		size_t level_index = 0;
		UInt64 threshold = std::ceil(sum_weight * levels.levels[levels.permutation[level_index]]);

		while (it < end)
		{
			accumulated += it->second;

			while (accumulated >= threshold)
			{
				data_to[old_size + levels.permutation[level_index]] = it->first;
				++level_index;

				if (level_index == num_levels)
					return;

				threshold = std::ceil(sum_weight * levels.levels[levels.permutation[level_index]]);
			}

			++it;
		}

		while (level_index < num_levels)
		{
			data_to[old_size + levels.permutation[level_index]] = array[size - 1].first;
			++level_index;
		}
	}
};

}
