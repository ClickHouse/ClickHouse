#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Columns/ColumnArray.h>

#include <DB/Common/HashTable/HashSet.h>

#include <DB/AggregateFunctions/AggregateFunctionGroupArray.h>

#define AGGREGATE_FUNCTION_GROUP_ARRAY_UNIQ_MAX_SIZE 0xFFFFFF


namespace DB
{


template <typename T>
struct AggregateFunctionGroupUniqArrayData
{
	/// При создании, хэш-таблица должна быть небольшой.
	typedef HashSet<
		T,
		DefaultHash<T>,
		HashTableGrower<4>,
		HashTableAllocatorWithStackMemory<sizeof(T) * (1 << 4)>
	> Set;

	Set value;
};


/// Складывает все значения в хэш-множество. Возвращает массив уникальных значений. Реализована для числовых типов.
template <typename T>
class AggregateFunctionGroupUniqArray : public IUnaryAggregateFunction<AggregateFunctionGroupUniqArrayData<T>, AggregateFunctionGroupUniqArray<T> >
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
		this->data(place).value.merge(this->data(rhs).value);
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
		this->data(place).value.readAndMerge(buf);
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
class AggregateFunctionGroupUniqArrays final : public AggregateFunctionGroupUniqArray<T>
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
