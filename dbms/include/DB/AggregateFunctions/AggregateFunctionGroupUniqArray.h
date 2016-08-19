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
class AggregateFunctionGroupUniqArray
	: public IUnaryAggregateFunction<AggregateFunctionGroupUniqArrayData<T>, AggregateFunctionGroupUniqArray<T>>
{
private:
	using State = AggregateFunctionGroupUniqArrayData<T>;

public:
	String getName() const override { return "groupUniqArray"; }

	DataTypePtr getReturnType() const override
	{
		return std::make_shared<DataTypeArray>(std::make_shared<typename DataTypeFromFieldType<T>::Type>());
	}

	void setArgument(const DataTypePtr & argument)
	{
	}


	void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		this->data(place).value.insert(static_cast<const ColumnVector<T> &>(column).getData()[row_num]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		this->data(place).value.merge(this->data(rhs).value);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		const typename State::Set & set = this->data(place).value;
		size_t size = set.size();
		writeVarUInt(size, buf);
		for (typename State::Set::const_iterator it = set.begin(); it != set.end(); ++it)
			writeIntBinary(*it, buf);
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		this->data(place).value.read(buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
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

#undef AGGREGATE_FUNCTION_GROUP_ARRAY_UNIQ_MAX_SIZE

}
