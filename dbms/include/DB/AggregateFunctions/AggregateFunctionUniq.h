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
#include <DB/AggregateFunctions/UniqCombinedBiasData.h>


namespace DB
{

/// uniq

struct AggregateFunctionUniqUniquesHashSetData
{
	typedef UniquesHashSet<DefaultHash<UInt64> > Set;
	Set set;

	static String getName() { return "uniq"; }
};

/// uniqHLL12

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

/// uniqExact

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

template <typename T, HyperLogLogMode mode>
struct BaseUniqCombinedData
{
	using Key = UInt64;
	using Set = CombinedCardinalityEstimator<
		Key,
		HashSet<Key, DefaultHash<Key>, HashTableGrower<> >,
		16,
		14,
		17,
		DefaultHash<Key>,
		UInt64,
		HyperLogLogBiasEstimator<UniqCombinedBiasData>,
		mode
	>;

	Set set;
};

template <HyperLogLogMode mode>
struct BaseUniqCombinedData<String, mode>
{
	using Key = UInt64;
	using Set = CombinedCardinalityEstimator<
		Key,
		HashSet<Key, TrivialHash, HashTableGrower<> >,
		16,
		14,
		17,
		TrivialHash,
		UInt64,
		HyperLogLogBiasEstimator<UniqCombinedBiasData>,
		mode
	>;

	Set set;
};

/// Агрегатные функции uniqCombinedRaw, uniqCombinedLinearCounting, и uniqCombinedBiasCorrected
/// предназначены для разработки новых версий функции uniqCombined.
/// Пользователи должны использовать только uniqCombined.

template <typename T>
struct AggregateFunctionUniqCombinedRawData
	: public BaseUniqCombinedData<T, HyperLogLogMode::Raw>
{
	static String getName() { return "uniqCombinedRaw"; }
};

template <typename T>
struct AggregateFunctionUniqCombinedLinearCountingData
	: public BaseUniqCombinedData<T, HyperLogLogMode::LinearCounting>
{
	static String getName() { return "uniqCombinedLinearCounting"; }
};

template <typename T>
struct AggregateFunctionUniqCombinedBiasCorrectedData
	: public BaseUniqCombinedData<T, HyperLogLogMode::BiasCorrected>
{
	static String getName() { return "uniqCombinedBiasCorrected"; }
};

template <typename T>
struct AggregateFunctionUniqCombinedData
	: public BaseUniqCombinedData<T, HyperLogLogMode::FullFeatured>
{
	static String getName() { return "uniqCombined"; }
};

namespace detail
{

/** Хэш-функция для uniq.
  */
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

/** Структура для делегации работы по добавлению одного элемента в агрегатные функции uniq.
  * Используется для частичной специализации для добавления строк.
  */
template <typename T, typename Data, typename Enable = void>
struct OneAdder;

template <typename T, typename Data>
struct OneAdder<T, Data, typename std::enable_if<
	std::is_same<Data, AggregateFunctionUniqUniquesHashSetData>::value ||
	std::is_same<Data, AggregateFunctionUniqHLL12Data<T> >::value ||
	std::is_same<Data, AggregateFunctionUniqCombinedRawData<T> >::value ||
	std::is_same<Data, AggregateFunctionUniqCombinedLinearCountingData<T> >::value ||
	std::is_same<Data, AggregateFunctionUniqCombinedBiasCorrectedData<T> >::value ||
	std::is_same<Data, AggregateFunctionUniqCombinedData<T> >::value>::type>
{
	template <typename T2 = T>
	static void addOne(Data & data, const IColumn & column, size_t row_num,
		typename std::enable_if<!std::is_same<T2, String>::value>::type * = nullptr)
	{
		const auto & value = static_cast<const ColumnVector<T2> &>(column).getData()[row_num];
		data.set.insert(AggregateFunctionUniqTraits<T2>::hash(value));
	}

	template <typename T2 = T>
	static void addOne(Data & data, const IColumn & column,	size_t row_num,
		typename std::enable_if<std::is_same<T2, String>::value>::type * = nullptr)
	{
		StringRef value = column.getDataAt(row_num);
		data.set.insert(CityHash64(value.data, value.size));
	}
};

template <typename T, typename Data>
struct OneAdder<T, Data, typename std::enable_if<
	std::is_same<Data, AggregateFunctionUniqExactData<T>>::value>::type>
{
	template <typename T2 = T>
	static void addOne(Data & data, const IColumn & column, size_t row_num,
		typename std::enable_if<!std::is_same<T2, String>::value>::type * = nullptr)
	{
		data.set.insert(static_cast<const ColumnVector<T2> &>(column).getData()[row_num]);
	}

	template <typename T2 = T>
	static void addOne(Data & data, const IColumn & column, size_t row_num,
		typename std::enable_if<std::is_same<T2, String>::value>::type * = nullptr)
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
