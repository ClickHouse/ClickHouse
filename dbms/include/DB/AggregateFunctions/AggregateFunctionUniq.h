#pragma once

#include <city.h>

#include <stats/UniquesHashSet.h>
#include <statdaemons/HyperLogLogCounter.h>

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteBufferFromString.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>

#include <DB/Columns/ColumnString.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{


template <typename T> struct AggregateFunctionUniqTraits
{
	static UInt64 hash(T x) { return x; }
};

template <> struct AggregateFunctionUniqTraits<Float32>
{
	static UInt64 hash(Float32 x)
	{
		UInt64 res = 0;
		memcpy(reinterpret_cast<char *>(&res), reinterpret_cast<char *>(&x), sizeof(x));
		return res;
	}
};

template <> struct AggregateFunctionUniqTraits<Float64>
{
	static UInt64 hash(Float64 x)
	{
		UInt64 res = 0;
		memcpy(reinterpret_cast<char *>(&res), reinterpret_cast<char *>(&x), sizeof(x));
		return res;
	}
};


struct AggregateFunctionUniqUniquesHashSetData
{
	typedef UniquesHashSet Set;
	Set set;
	
	static String getName() { return "uniq"; }
};

struct AggregateFunctionUniqHLL12Data
{
	typedef HyperLogLogCounter<12> Set;
	Set set;
	
	static String getName() { return "uniqHLL12"; }
};

/// Структура для делегации работы по добавлению одного элемента
/// в аггрегатную функцию uniq. Используется для частичной специализации
/// для добавления строк.
template<typename T, typename Data> struct OneAdder;

/// Приближённо вычисляет количество различных значений.
template <typename T, typename Data>
class AggregateFunctionUniq : public IUnaryAggregateFunction<Data>
{
public:
	AggregateFunctionUniq() {}

	String getName() const { return Data::getName(); }

	DataTypePtr getReturnType() const
	{
		return new DataTypeUInt64;
	}

	void setArgument(const DataTypePtr & argument)
	{
	}

	void addOne(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		OneAdder<T, Data>::addOne(*this, place, column, row_num);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		this->data(place).set.merge(this->data(rhs).set);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		this->data(place).set.write(buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		typename Data::Set tmp_set;
		tmp_set.read(buf);
		this->data(place).set.merge(tmp_set);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		static_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).set.size());
	}
	
private:
	template<typename T0, typename Data0> friend struct OneAdder;
};

template<typename T, typename Data>
struct OneAdder
{
	static void addOne(const AggregateFunctionUniq<T, Data> & aggregate_function, AggregateDataPtr place, const IColumn & column, size_t row_num)
	{
		aggregate_function.data(place).set.insert(
			AggregateFunctionUniqTraits<T>::hash(static_cast<const ColumnVector<T> &>(column).getData()[row_num]));
	}
};

template<typename Data>
struct OneAdder<String, Data>
{
	static void addOne(const AggregateFunctionUniq<String, Data> & aggregate_function, AggregateDataPtr place, const IColumn & column, size_t row_num)
	{
		/// Имейте ввиду, что вычисление приближённое.
		StringRef value = column.getDataAt(row_num);
		aggregate_function.data(place).set.insert(CityHash64(value.data, value.size));
	}
};


/** То же самое, но выводит состояние вычислений в строке в текстовом виде.
  * Используется, если какой-то внешней программе (сейчас это ███████████)
  *  надо получить это состояние и потом использовать по-своему.
  */
template <typename T, typename Data>
class AggregateFunctionUniqState : public AggregateFunctionUniq<T, Data>
{
public:
	String getName() const { return Data::getName() + "State"; }

	DataTypePtr getReturnType() const
	{
		return new DataTypeString;
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		String res;
		{
			WriteBufferFromString wb(res);
			this->data(place).set.writeText(wb);
		}

		static_cast<ColumnString &>(to).insertDataWithTerminatingZero(res.data(), res.size() + 1);
	}
};


/** Принимает два аргумента - значение и условие.
  * Приближённо считает количество различных значений, когда выполнено это условие.
  */
template <typename T>
class AggregateFunctionUniqIf : public IAggregateFunctionHelper<AggregateFunctionUniqUniquesHashSetData>
{
public:
	AggregateFunctionUniqIf() {}

	String getName() const { return "uniqIf"; }

	DataTypePtr getReturnType() const
	{
		return new DataTypeUInt64;
	}

	void setArguments(const DataTypes & arguments)
	{
		if (!dynamic_cast<const DataTypeUInt8 *>(&*arguments[1]))
			throw Exception("Incorrect type " + arguments[1]->getName() + " of second argument for aggregate function " + getName() + ". Must be UInt8.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num) const
	{
		if (static_cast<const ColumnUInt8 &>(*columns[1]).getData()[row_num])
			data(place).set.insert(AggregateFunctionUniqTraits<T>::hash(static_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]));
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		data(place).set.merge(data(rhs).set);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		data(place).set.write(buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		UniquesHashSet tmp_set;
		tmp_set.read(buf);
		data(place).set.merge(tmp_set);
	}

 	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		static_cast<ColumnUInt64 &>(to).getData().push_back(data(place).set.size());
	}
};

template <>
inline void AggregateFunctionUniqIf<String>::add(AggregateDataPtr place, const IColumn ** columns, size_t row_num) const
{
	if (static_cast<const ColumnUInt8 &>(*columns[1]).getData()[row_num])
	{
		/// Имейте ввиду, что вычисление приближённое.
		StringRef value = columns[0]->getDataAt(row_num);
		data(place).set.insert(CityHash64(value.data, value.size));
	}
}

}
