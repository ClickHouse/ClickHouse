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
struct __attribute__((__packed__)) AggregateFunctionUniqUpToData
{
	/** Если count == threshold + 1 - это значит, что "переполнилось" (значений больше threshold).
	  * В этом случае (например, после вызова функции merge), массив data не обязательно содержит инициализированные значения
	  * - пример: объединяем состояние, в котором мало значений, с другим состоянием, которое переполнилось;
	  *   тогда выставляем count в threshold + 1, а значения из другого состояния не копируем.
	  */
	UInt8 count = 0;

	/// Данные идут после конца структуры. При вставке, делается линейный поиск.
	T data[0];


	size_t size() const
	{
		return count;
	}

	/// threshold - для скольки элементов есть место в data.
	void insert(T x, UInt8 threshold)
	{
		/// Состояние уже переполнено - ничего делать не нужно.
		if (count > threshold)
			return;

		/// Линейный поиск совпадающего элемента.
		for (size_t i = 0; i < count; ++i)
			if (data[i] == x)
				return;

		/// Не нашли совпадающий элемент. Если есть место ещё для одного элемента - вставляем его.
		if (count < threshold)
			data[count] = x;

		/// После увеличения count, состояние может оказаться переполненным.
		++count;
	}

	void merge(const AggregateFunctionUniqUpToData<T> & rhs, UInt8 threshold)
	{
		if (count > threshold)
			return;

		if (rhs.count > threshold)
		{
			/// Если rhs переполнено, то выставляем у текущего состояния count тоже переполненным.
			count = rhs.count;
			return;
		}

		for (size_t i = 0; i < rhs.count; ++i)
			insert(rhs.data[i], threshold);
	}

	void write(WriteBuffer & wb, UInt8 threshold) const
	{
		writeBinary(count, wb);

		/// Пишем значения, только если состояние не переполнено. Иначе они не нужны, а важен только факт того, что состояние переполнено.
		if (count <= threshold)
			wb.write(reinterpret_cast<const char *>(&data[0]), count * sizeof(data[0]));
	}

	void readAndMerge(ReadBuffer & rb, UInt8 threshold)
	{
		UInt8 rhs_count;
		readBinary(rhs_count, rb);

		if (rhs_count > threshold)
		{
			/// Если rhs переполнено, то выставляем у текущего состояния count тоже переполненным.
			count = rhs_count;
			return;
		}

		for (size_t i = 0; i < rhs_count; ++i)
		{
			T x;
			readBinary(x, rb);
			insert(x, threshold);
		}
	}


	void addOne(const IColumn & column, size_t row_num, UInt8 threshold)
	{
		insert(static_cast<const ColumnVector<T> &>(column).getData()[row_num], threshold);
	}
};


/// Для строк, запоминаются их хэши.
template <>
struct AggregateFunctionUniqUpToData<String> : AggregateFunctionUniqUpToData<UInt64>
{
	void addOne(const IColumn & column, size_t row_num, UInt8 threshold)
	{
		/// Имейте ввиду, что вычисление приближённое.
		StringRef value = column.getDataAt(row_num);
		insert(CityHash64(value.data, value.size), threshold);
	}
};


constexpr UInt8 uniq_upto_max_threshold = 100;

template <typename T>
class AggregateFunctionUniqUpTo final : public IUnaryAggregateFunction<AggregateFunctionUniqUpToData<T>, AggregateFunctionUniqUpTo<T> >
{
private:
	UInt8 threshold = 5;	/// Значение по-умолчанию, если параметр не указан.

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

		UInt64 threshold_param = apply_visitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

		if (threshold_param > uniq_upto_max_threshold)
			throw Exception("Too large parameter for aggregate function " + getName() + ". Maximum: " + toString(uniq_upto_max_threshold),
				ErrorCodes::ARGUMENT_OUT_OF_BOUND);

		threshold = threshold_param;
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
		this->data(place).write(buf, threshold);
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
