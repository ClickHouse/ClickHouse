#pragma once

#include <DB/Core/FieldVisitors.h>
#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>
#include <DB/AggregateFunctions/UniqVariadicHash.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeTuple.h>


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

	void read(ReadBuffer & rb, UInt8 threshold)
	{
		readBinary(count, rb);

		if (count <= threshold)
			rb.read(reinterpret_cast<char *>(&data[0]), count * sizeof(data[0]));
	}


	void addImpl(const IColumn & column, size_t row_num, UInt8 threshold)
	{
		insert(static_cast<const ColumnVector<T> &>(column).getData()[row_num], threshold);
	}
};


/// Для строк, запоминаются их хэши.
template <>
struct AggregateFunctionUniqUpToData<String> : AggregateFunctionUniqUpToData<UInt64>
{
	void addImpl(const IColumn & column, size_t row_num, UInt8 threshold)
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
	size_t sizeOfData() const override
	{
		return sizeof(AggregateFunctionUniqUpToData<T>) + sizeof(T) * threshold;
	}

	String getName() const override { return "uniqUpTo"; }

	DataTypePtr getReturnType() const override
	{
		return std::make_shared<DataTypeUInt64>();
	}

	void setArgument(const DataTypePtr & argument)
	{
	}

	void setParameters(const Array & params) override
	{
		if (params.size() != 1)
			throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		UInt64 threshold_param = apply_visitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

		if (threshold_param > uniq_upto_max_threshold)
			throw Exception("Too large parameter for aggregate function " + getName() + ". Maximum: " + toString(uniq_upto_max_threshold),
				ErrorCodes::ARGUMENT_OUT_OF_BOUND);

		threshold = threshold_param;
	}

	void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena *) const
	{
		this->data(place).addImpl(column, row_num, threshold);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
	{
		this->data(place).merge(this->data(rhs), threshold);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		this->data(place).write(buf, threshold);
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
	{
		this->data(place).read(buf, threshold);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		static_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).size());
	}
};


/** Для нескольких аргументов. Для вычисления, хэширует их.
  * Можно передать несколько аргументов как есть; также можно передать один аргумент - кортеж.
  * Но (для возможности эффективной реализации), нельзя передать несколько аргументов, среди которых есть кортежи.
  */
template <bool argument_is_tuple>
class AggregateFunctionUniqUpToVariadic final : public IAggregateFunctionHelper<AggregateFunctionUniqUpToData<UInt64>>
{
private:
	size_t num_args = 0;
	UInt8 threshold = 5;	/// Значение по-умолчанию, если параметр не указан.

public:
	size_t sizeOfData() const override
	{
		return sizeof(AggregateFunctionUniqUpToData<UInt64>) + sizeof(UInt64) * threshold;
	}

	String getName() const override { return "uniqUpTo"; }

	DataTypePtr getReturnType() const override
	{
		return std::make_shared<DataTypeUInt64>();
	}

	void setArguments(const DataTypes & arguments) override
	{
		if (argument_is_tuple)
			num_args = typeid_cast<const DataTypeTuple &>(*arguments[0]).getElements().size();
		else
			num_args = arguments.size();
	}

	void setParameters(const Array & params) override
	{
		if (params.size() != 1)
			throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		UInt64 threshold_param = apply_visitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

		if (threshold_param > uniq_upto_max_threshold)
			throw Exception("Too large parameter for aggregate function " + getName() + ". Maximum: " + toString(uniq_upto_max_threshold),
				ErrorCodes::ARGUMENT_OUT_OF_BOUND);

		threshold = threshold_param;
	}

	void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
	{
		this->data(place).insert(UniqVariadicHash<false, argument_is_tuple>::apply(num_args, columns, row_num), threshold);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
	{
		this->data(place).merge(this->data(rhs), threshold);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		this->data(place).write(buf, threshold);
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
	{
		this->data(place).read(buf, threshold);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		static_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).size());
	}

	static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *arena)
	{
		static_cast<const AggregateFunctionUniqUpToVariadic &>(*that).add(place, columns, row_num, arena);
	}

	IAggregateFunction::AddFunc getAddressOfAddFunction() const override final { return &addFree; }
};


}
