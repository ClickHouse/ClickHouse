#pragma once

#include <city.h>
#include <type_traits>

#include <stats/UniquesHashSet.h>
#include <statdaemons/HyperLogLogCounter.h>

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteBufferFromString.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>

#include <DB/Interpreters/AggregationCommon.h>
#include <DB/Common/HashTable/HashSet.h>

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


template <typename T>
struct AggregateFunctionUniqExactData
{
	typedef T Key;

	/// При создании, хэш-таблица должна быть небольшой.
	struct Grower : public HashTableGrower
	{
		static const size_t initial_size_degree = 64 / sizeof(Key);
		Grower() { size_degree = initial_size_degree; }
	};

	typedef HashSet<
		Key,
		DefaultHash<Key>,
		Grower,
		HashTableAllocatorWithStackMemory<64>
	> Set;

	Set set;

	static String getName() { return "uniqExact"; }
};

/// Для строк будем класть в хэш-таблицу значения SipHash-а (128 бит).
template <>
struct AggregateFunctionUniqExactData<String>
{
	typedef UInt128 Key;

	/// При создании, хэш-таблица должна быть небольшой.
	struct Grower : public HashTableGrower
	{
		static const size_t initial_size_degree = 64 / sizeof(Key);
		Grower() { size_degree = initial_size_degree; }
	};

	typedef HashSet<
		Key,
		UInt128TrivialHash,
		Grower,
		HashTableAllocatorWithStackMemory<64>
	> Set;

	Set set;

	static String getName() { return "uniqExact"; }
};


namespace detail
{
	/** Структура для делегации работы по добавлению одного элемента в аггрегатные функции uniq.
	  * Используется для частичной специализации для добавления строк.
	  */
	template<typename T, typename Data>
	struct OneAdder
	{
		static void addOne(Data & data, const IColumn & column, size_t row_num)
		{
			data.set.insert(AggregateFunctionUniqTraits<T>::hash(static_cast<const ColumnVector<T> &>(column).getData()[row_num]));
		}
	};

	template<typename Data>
	struct OneAdder<String, Data>
	{
		static void addOne(Data & data, const IColumn & column, size_t row_num)
		{
			/// Имейте ввиду, что вычисление приближённое.
			StringRef value = column.getDataAt(row_num);
			data.set.insert(CityHash64(value.data, value.size));
		}
	};

	template<typename T>
	struct OneAdder<T, AggregateFunctionUniqExactData<T> >
	{
		static void addOne(AggregateFunctionUniqExactData<T> & data, const IColumn & column, size_t row_num)
		{
			data.set.insert(static_cast<const ColumnVector<T> &>(column).getData()[row_num]);
		}
	};

	template<>
	struct OneAdder<String, AggregateFunctionUniqExactData<String> >
	{
		static void addOne(AggregateFunctionUniqExactData<String> & data, const IColumn & column, size_t row_num)
		{
			StringRef value = column.getDataAt(row_num);

			UInt128 key;
			SipHash hash;
			hash.update(value.data, value.size);
			hash.get128(key.first, key.second);

			data.set.insert(key);
		}
	};
}


/// Приближённо вычисляет количество различных значений.
template <typename T, typename Data>
class AggregateFunctionUniq : public IUnaryAggregateFunction<Data, AggregateFunctionUniq<T, Data> >
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
		detail::OneAdder<T, Data>::addOne(this->data(place), column, row_num);
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
		this->data(place).set.readAndMerge(buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		static_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).set.size());
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


}
