#pragma once

#include <city.h>
#include <type_traits>

#include <stats/UniquesHashSet.h>

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteBufferFromString.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>

#include <DB/Interpreters/AggregationCommon.h>
#include <DB/Common/HashTable/HashSet.h>
#include <DB/Common/HyperLogLogWithSmallSetOptimization.h>
#include <DB/Common/CombinedCardinalityEstimator.h>

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
	typedef UniquesHashSet<DefaultHash<UInt64>> Set;
	Set set;

	static String getName() { return "uniq"; }
};


template <typename T>
struct AggregateFunctionUniqHLL12Data
{
	typedef HyperLogLogWithSmallSetOptimization<T, 16, 12> Set;
	Set set;

	static String getName() { return "uniqHLL12"; }
};

template <>
struct AggregateFunctionUniqHLL12Data<String>
{
	typedef HyperLogLogWithSmallSetOptimization<UInt64, 16, 12> Set;
	Set set;

	static String getName() { return "uniqHLL12"; }
};


template <typename T>
struct AggregateFunctionUniqExactData
{
	typedef T Key;

	/// При создании, хэш-таблица должна быть небольшой.
	typedef HashSet<
		Key,
		DefaultHash<Key>,
		HashTableGrower<4>,
		HashTableAllocatorWithStackMemory<sizeof(Key) * (1 << 4)>
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
	typedef HashSet<
		Key,
		UInt128TrivialHash,
		HashTableGrower<3>,
		HashTableAllocatorWithStackMemory<sizeof(Key) * (1 << 3)>
	> Set;

	Set set;

	static String getName() { return "uniqExact"; }
};


template <typename T>
struct AggregateFunctionUniqCombinedData
{
	using Key = T;
	using Set = CombinedCardinalityEstimator<Key, HashSet<Key, TrivialHash, HashTableGrower<> >, 16, 16, 19, TrivialHash>;
	Set set;

	static String getName() { return "uniqCombined"; }
};

template <>
struct AggregateFunctionUniqCombinedData<String>
{
	using Key = UInt64;
	using Set = CombinedCardinalityEstimator<Key, HashSet<Key, TrivialHash, HashTableGrower<> >, 16, 16, 19, TrivialHash>;
	Set set;

	static String getName() { return "uniqCombined"; }
};

namespace detail
{
	template<typename T, typename Enable = void>
	struct Hash64To32;

	template<typename T>
	struct Hash64To32<T, typename std::enable_if<std::is_same<T, Int64>::value || std::is_same<T, UInt64>::value>::type>
	{
		/// https://gist.github.com/badboy/6267743
		static UInt32 compute(T key)
		{
			using U = typename std::make_unsigned<T>::type;
			auto x = static_cast<U>(key);

			x = (~x) + (x << 18);
			x = x ^ (x >> 31);
			x = x * 21;
			x = x ^ (x >> 11);
			x = x + (x << 6);
			x = x ^ (x >> 22);
			return static_cast<UInt32>(x);
		}
	};

	template<typename T, typename Enable = void>
	struct CombinedCardinalityTraits
	{
		static UInt32 hash(T key)
		{
			return key;
		}
	};

	template<typename T>
	struct CombinedCardinalityTraits<T, typename std::enable_if<std::is_same<T, Int64>::value || std::is_same<T, UInt64>::value>::type>
	{
		using Op = Hash64To32<T>;

		static UInt32 hash(T key)
		{
			return Op::compute(key);
		};
	};

	template<typename T>
	struct CombinedCardinalityTraits<T, typename std::enable_if<std::is_same<T, Float64>::value>::type>
	{
		using Op = Hash64To32<UInt64>;

		static UInt32 hash(T key)
		{
			UInt64 res = 0;
			memcpy(reinterpret_cast<char *>(&res), reinterpret_cast<char *>(&key), sizeof(key));
			return Op::compute(res);
		}
	};

	template<typename T>
	struct CombinedCardinalityTraits<T, typename std::enable_if<std::is_same<T, Float32>::value>::type>
	{
		static UInt32 hash(T key)
		{
			UInt32 res = 0;
			memcpy(reinterpret_cast<char *>(&res), reinterpret_cast<char *>(&key), sizeof(key));
			return res;
		}
	};

	/** Структура для делегации работы по добавлению одного элемента в агрегатные функции uniq.
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

	template<typename T>
	struct OneAdder<T, AggregateFunctionUniqCombinedData<T> >
	{
		static void addOne(AggregateFunctionUniqCombinedData<T> & data, const IColumn & column, size_t row_num)
		{
			const auto & value = static_cast<const ColumnVector<T> &>(column).getData()[row_num];
			data.set.insert(CombinedCardinalityTraits<T>::hash(value));
		}
	};

	template<>
	struct OneAdder<String, AggregateFunctionUniqCombinedData<String> >
	{
		static void addOne(AggregateFunctionUniqCombinedData<String> & data, const IColumn & column, size_t row_num)
		{
			StringRef value = column.getDataAt(row_num);
			data.set.insert(CityHash64(value.data, value.size));
		}
	};
}


/// Приближённо вычисляет количество различных значений.
template <typename T, typename Data>
class AggregateFunctionUniq final : public IUnaryAggregateFunction<Data, AggregateFunctionUniq<T, Data> >
{
public:
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


}
