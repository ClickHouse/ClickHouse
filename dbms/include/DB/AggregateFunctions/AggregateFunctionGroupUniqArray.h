#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Columns/ColumnArray.h>

#include <DB/Interpreters/HashSet.h>

#include <DB/AggregateFunctions/AggregateFunctionGroupArray.h>

#define AGGREGATE_FUNCTION_GROUP_ARRAY_UNIQ_MAX_SIZE 0xFFFFFF


namespace DB
{


template <typename T>
struct AggregateFunctionGroupUniqArrayData
{
	struct hash
	{
		size_t operator() (T key) const
		{
			return intHash32<0>(key);
		}
	};

	struct GrowthTraits : public default_growth_traits
	{
		/// При создании, хэш-таблица должна быть небольшой.
		static const int INITIAL_SIZE_DEGREE = 4;
	};

	/// NOTE: Можно сделать отдельную хэш-таблицу с оптимизацией для маленьких размеров.
	typedef HashSet<T, hash, default_zero_traits<T>, GrowthTraits> Set;
	Set value;
};

template <> size_t AggregateFunctionGroupUniqArrayData<Float32>::hash::operator() (Float32 key) const
{
	UInt64 res = 0;
	memcpy(reinterpret_cast<char *>(&res), reinterpret_cast<char *>(&key), sizeof(key));
	return intHash32<0>(res);
}

template <> size_t AggregateFunctionGroupUniqArrayData<Float64>::hash::operator() (Float64 key) const
{
	UInt64 res = 0;
	memcpy(reinterpret_cast<char *>(&res), reinterpret_cast<char *>(&key), sizeof(key));
	return intHash32<0>(res);
}


/// Складывает все значения в хэш-множество. Возвращает массив уникальных значений. Реализована для числовых типов.
template <typename T>
class AggregateFunctionGroupUniqArray : public IUnaryAggregateFunction<AggregateFunctionGroupUniqArrayData<T> >
{
private:
	typedef AggregateFunctionGroupUniqArrayData<T> State;
	
public:
	String getName() const { return "groupUniqArray"; }

	DataTypePtr getReturnType() const
	{
		return new DataTypeArray(new typename DataTypeFromFieldType<T>::Type);
	}

	void setArgument(const DataTypePtr & argument)
	{
	}


	void addOne(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		this->data(place).value.insert(static_cast<const ColumnVector<T> &>(column).getData()[row_num]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		typename State::Set & set = this->data(place).value;
		const typename State::Set & rhs_set = this->data(rhs).value;
		for (typename State::Set::const_iterator it = rhs_set.begin(); it != rhs_set.end(); ++it)
			set.insert(*it);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		const typename State::Set & set = this->data(place).value;
		size_t size = set.size();
		writeVarUInt(size, buf);
		for (typename State::Set::const_iterator it = set.begin(); it != set.end(); ++it)
			writeIntBinary(*it, buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		size_t size = 0;
		readVarUInt(size, buf);

		if (size > AGGREGATE_FUNCTION_GROUP_ARRAY_UNIQ_MAX_SIZE)
			throw Exception("Too large hash set size for aggregate function groupUniqArray", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

		typename State::Set & set = this->data(place).value;

		for (size_t i = 0; i < size; ++i)
		{
			T tmp;
			readIntBinary(tmp, buf);
			set.insert(tmp);
		}
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		ColumnArray & arr_to = static_cast<ColumnArray &>(to);
		ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();

		const typename State::Set & set = this->data(place).value;
		size_t size = set.size();

		offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

		typename ColumnVector<T>::Container_t & data_to = static_cast<ColumnVector<T> &>(arr_to.getData()).getData();
		size_t old_size = data_to.size();
		data_to.resize(old_size + size);

		size_t i = 0;
		for (typename State::Set::const_iterator it = set.begin(); it != set.end(); ++it, ++i)
			data_to[old_size + i] = *it;
	}
};


/** То же самое, но в качестве аргумента - числовые массивы. Применяется ко всем элементам массивов.
  * То есть, выдаёт массив, содержащий уникальные значения из внутренностей массивов-аргументов.
  */
template <typename T>
class AggregateFunctionGroupUniqArrays : public AggregateFunctionGroupUniqArray<T>
{
public:
	void addOne(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		const ColumnArray & arr = static_cast<const ColumnArray &>(column);
		const ColumnArray::Offsets_t & offsets = arr.getOffsets();
		const typename ColumnVector<T>::Container_t & data = static_cast<const ColumnVector<T> &>(arr.getData()).getData();

		IColumn::Offset_t begin = row_num ? offsets[row_num - 1] : 0;
		IColumn::Offset_t end = offsets[row_num];

		typename AggregateFunctionGroupUniqArrayData<T>::Set & set = this->data(place).value;

		for (IColumn::Offset_t i = begin; i != end; ++i)
			set.insert(data[i]);
	}
};


#undef AGGREGATE_FUNCTION_GROUP_ARRAY_UNIQ_MAX_SIZE

}
