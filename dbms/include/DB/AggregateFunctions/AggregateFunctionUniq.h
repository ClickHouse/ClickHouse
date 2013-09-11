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
	typedef HLL12 Set;
	Set set;
	
	static String getName() { return "uniqHLL12"; }
};


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
	/// Структура для делегации работы по добавлению одного элемента
	/// в аггрегатную функцию uniq. Используется для частичной специализации
	/// для добавления строк.
	template<typename T0, typename Data0>
	struct OneAdder
	{
		static void addOne(const AggregateFunctionUniq<T0, Data0> & aggregate_function,
						   AggregateDataPtr place, const IColumn & column, size_t row_num)
		{
			aggregate_function.data(place).set.insert(
				AggregateFunctionUniqTraits<T0>::hash(static_cast<const ColumnVector<T0> &>(column).getData()[row_num]));
		}
	};
	
	template<typename Data0>
	struct OneAdder<String, Data0>
	{
		static void addOne(const AggregateFunctionUniq<String, Data0> & aggregate_function,
						   AggregateDataPtr place, const IColumn & column, size_t row_num)
		{
			/// Имейте ввиду, что вычисление приближённое.
			StringRef value = column.getDataAt(row_num);
			aggregate_function.data(place).set.insert(CityHash64(value.data, value.size));
		}
	};
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


}
