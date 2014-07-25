#pragma once

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>


namespace DB
{


/** Считает количество уникальных значений до не более чем указанного в параметре.
  *
  * Пример: uniqUpTo(3)(UserID)
  * - посчитает количество уникальных посетителей, вернёт 1, 2, 3 или 4, если их >= 4.
  *
  * Для строк используется некриптографическая хэш-функция, за счёт чего рассчёт может быть немного неточным.
  */

template <typename T>
struct AggregateFunctionUniqUpToData
{
	UInt32 count = 0;
	T data[0];	/// Данные идут после конца структуры. При вставке, делается линейный поиск.


	size_t size() const
	{
		return count;
	}

	/// threshold - для скольки элементов есть место в data.
	void insert(T x, UInt32 threshold)
	{
		if (count > threshold)
			return;

		for (size_t i = 0; i < count; ++i)
			if (data[i] == x)
				return;

		if (count < threshold)
			data[count] = x;

		++count;
	}

	void merge(const AggregateFunctionUniqUpToData<T> & rhs, UInt32 threshold)
	{
		if (count > threshold)
			return;

		if (rhs.count > threshold)
		{
			count = rhs.count;
			return;
		}

		for (size_t i = 0; i < rhs.count; ++i)
			insert(rhs.data[i], threshold);
	}

	void write(WriteBuffer & wb) const
	{
		writeVarUInt(count, wb);
		for (size_t i = 0; i < count; ++i)
			writeBinary(data[i], wb);
	}

	void readAndMerge(ReadBuffer & rb, UInt32 threshold)
	{
		UInt32 rhs_count;
		readVarUInt(rhs_count, rb);

		if (rhs_count > threshold + 1)
			throw Poco::Exception("Cannot read AggregateFunctionUniqUpToData: too large count.");

		for (size_t i = 0; i < rhs_count; ++i)
		{
			T x;
			readBinary(x, rb);
			insert(x, threshold);
		}
	}


	void addOne(const IColumn & column, size_t row_num, UInt32 threshold)
	{
		insert(static_cast<const ColumnVector<T> &>(column).getData()[row_num], threshold);
	}
};


/// Для строк, запоминаются их хэши.
template <>
struct AggregateFunctionUniqUpToData<String> : AggregateFunctionUniqUpToData<UInt64>
{
	void addOne(const IColumn & column, size_t row_num, UInt32 threshold)
	{
		/// Имейте ввиду, что вычисление приближённое.
		StringRef value = column.getDataAt(row_num);
		insert(CityHash64(value.data, value.size), threshold);
	}
};


constexpr UInt32 uniq_upto_max_threshold = 100;

template <typename T>
class AggregateFunctionUniqUpTo final : public IUnaryAggregateFunction<AggregateFunctionUniqUpToData<T>, AggregateFunctionUniqUpTo<T> >
{
private:
	UInt32 threshold = 5;	/// Значение по-умолчанию, если параметр не указан.

public:
	size_t sizeOfData() const
	{
		return sizeof(AggregateFunctionUniqUpToData<T>) + sizeof(T) * threshold;
	}

	String getName() const { return "uniqUpTo"; }

	DataTypePtr getReturnType() const
	{
		return new DataTypeUInt64;
	}

	void setArgument(const DataTypePtr & argument)
	{
	}

	void setParameters(const Array & params)
	{
		if (params.size() != 1)
			throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		threshold = apply_visitor(FieldVisitorConvertToNumber<UInt32>(), params[0]);

		if (threshold > uniq_upto_max_threshold)
			throw Exception("Too large parameter for aggregate function " + getName() + ". Maximum: " + toString(uniq_upto_max_threshold),
				ErrorCodes::ARGUMENT_OUT_OF_BOUND);
	}

	void addOne(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		this->data(place).addOne(column, row_num, threshold);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		this->data(place).merge(this->data(rhs), threshold);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		this->data(place).write(buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		this->data(place).readAndMerge(buf, threshold);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		static_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).size());
	}
};


}
