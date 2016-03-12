#pragma once

#include <city.h>
#include <type_traits>

#include <DB/AggregateFunctions/UniquesHashSet.h>

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteBufferFromString.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeTuple.h>

#include <DB/Interpreters/AggregationCommon.h>
#include <DB/Common/HashTable/HashSet.h>
#include <DB/Common/HyperLogLogWithSmallSetOptimization.h>
#include <DB/Common/CombinedCardinalityEstimator.h>

#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnTuple.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>
#include <DB/AggregateFunctions/UniqCombinedBiasData.h>
#include <DB/AggregateFunctions/UniqVariadicHash.h>


namespace DB
{

/// uniq

struct AggregateFunctionUniqUniquesHashSetData
{
	typedef UniquesHashSet<DefaultHash<UInt64>> Set;
	Set set;

	static String getName() { return "uniq"; }
};

/// Для функции, принимающей несколько аргументов. Такая функция сама заранее их хэширует, поэтому здесь используется TrivialHash.
struct AggregateFunctionUniqUniquesHashSetDataForVariadic
{
	typedef UniquesHashSet<TrivialHash> Set;
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

struct AggregateFunctionUniqHLL12DataForVariadic
{
	typedef HyperLogLogWithSmallSetOptimization<UInt64, 16, 12, TrivialHash> Set;
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
		HashCRC32<Key>,
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
	using Key = UInt32;
	using Set = CombinedCardinalityEstimator<
		Key,
		HashSet<Key, TrivialHash, HashTableGrower<> >,
		16,
		14,
		17,
		TrivialHash,
		UInt32,
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

/** Хэш-функция для uniqCombined.
  */
template <typename T> struct AggregateFunctionUniqCombinedTraits
{
	static UInt32 hash(T x) { return static_cast<UInt32>(intHash64(x)); }
};

template <> struct AggregateFunctionUniqCombinedTraits<Float32>
{
	static UInt32 hash(Float32 x)
	{
		UInt64 res = 0;
		memcpy(reinterpret_cast<char *>(&res), reinterpret_cast<char *>(&x), sizeof(x));
		return static_cast<UInt32>(intHash64(res));
	}
};

template <> struct AggregateFunctionUniqCombinedTraits<Float64>
{
	static UInt32 hash(Float64 x)
	{
		UInt64 res = 0;
		memcpy(reinterpret_cast<char *>(&res), reinterpret_cast<char *>(&x), sizeof(x));
		return static_cast<UInt32>(intHash64(res));
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
	std::is_same<Data, AggregateFunctionUniqHLL12Data<T> >::value>::type>
{
	template <typename T2 = T>
	static void addImpl(Data & data, const IColumn & column, size_t row_num,
		typename std::enable_if<!std::is_same<T2, String>::value>::type * = nullptr)
	{
		const auto & value = static_cast<const ColumnVector<T2> &>(column).getData()[row_num];
		data.set.insert(AggregateFunctionUniqTraits<T2>::hash(value));
	}

	template <typename T2 = T>
	static void addImpl(Data & data, const IColumn & column,	size_t row_num,
		typename std::enable_if<std::is_same<T2, String>::value>::type * = nullptr)
	{
		StringRef value = column.getDataAt(row_num);
		data.set.insert(CityHash64(value.data, value.size));
	}
};

template <typename T, typename Data>
struct OneAdder<T, Data, typename std::enable_if<
	std::is_same<Data, AggregateFunctionUniqCombinedRawData<T> >::value ||
	std::is_same<Data, AggregateFunctionUniqCombinedLinearCountingData<T> >::value ||
	std::is_same<Data, AggregateFunctionUniqCombinedBiasCorrectedData<T> >::value ||
	std::is_same<Data, AggregateFunctionUniqCombinedData<T> >::value>::type>
{
	template <typename T2 = T>
	static void addImpl(Data & data, const IColumn & column, size_t row_num,
		typename std::enable_if<!std::is_same<T2, String>::value>::type * = nullptr)
	{
		const auto & value = static_cast<const ColumnVector<T2> &>(column).getData()[row_num];
		data.set.insert(AggregateFunctionUniqCombinedTraits<T2>::hash(value));
	}

	template <typename T2 = T>
	static void addImpl(Data & data, const IColumn & column,	size_t row_num,
		typename std::enable_if<std::is_same<T2, String>::value>::type * = nullptr)
	{
		StringRef value = column.getDataAt(row_num);
		data.set.insert(CityHash64(value.data, value.size));
	}
};

template <typename T, typename Data>
struct OneAdder<T, Data, typename std::enable_if<
	std::is_same<Data, AggregateFunctionUniqExactData<T> >::value>::type>
{
	template <typename T2 = T>
	static void addImpl(Data & data, const IColumn & column, size_t row_num,
		typename std::enable_if<!std::is_same<T2, String>::value>::type * = nullptr)
	{
		data.set.insert(static_cast<const ColumnVector<T2> &>(column).getData()[row_num]);
	}

	template <typename T2 = T>
	static void addImpl(Data & data, const IColumn & column, size_t row_num,
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


/// Приближённо или точно вычисляет количество различных значений.
template <typename T, typename Data>
class AggregateFunctionUniq final : public IUnaryAggregateFunction<Data, AggregateFunctionUniq<T, Data> >
{
public:
	String getName() const override { return Data::getName(); }

	DataTypePtr getReturnType() const override
	{
		return new DataTypeUInt64;
	}

	void setArgument(const DataTypePtr & argument)
	{
	}

	void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		detail::OneAdder<T, Data>::addImpl(this->data(place), column, row_num);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		this->data(place).set.merge(this->data(rhs).set);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		this->data(place).set.write(buf);
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		this->data(place).set.read(buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		static_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).set.size());
	}
};


/** Для нескольких аргументов. Для вычисления, хэширует их.
  * Можно передать несколько аргументов как есть; также можно передать один аргумент - кортеж.
  * Но (для возможности эффективной реализации), нельзя передать несколько аргументов, среди которых есть кортежи.
  */
template <typename Data, bool argument_is_tuple>
class AggregateFunctionUniqVariadic final : public IAggregateFunctionHelper<Data>
{
private:
	static constexpr bool is_exact = std::is_same<Data, AggregateFunctionUniqExactData<String>>::value;

	size_t num_args = 0;

public:
	String getName() const override { return Data::getName(); }

	DataTypePtr getReturnType() const override
	{
		return new DataTypeUInt64;
	}

	void setArguments(const DataTypes & arguments) override
	{
		if (argument_is_tuple)
			num_args = typeid_cast<const DataTypeTuple &>(*arguments[0]).getElements().size();
		else
			num_args = arguments.size();
	}

	void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num) const override
	{
		this->data(place).set.insert(UniqVariadicHash<is_exact, argument_is_tuple>::apply(num_args, columns, row_num));
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		this->data(place).set.merge(this->data(rhs).set);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		this->data(place).set.write(buf);
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		this->data(place).set.read(buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		static_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).set.size());
	}

	static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num)
	{
		return static_cast<const AggregateFunctionUniqVariadic &>(*that).add(place, columns, row_num);
	}

	IAggregateFunction::AddFunc getAddressOfAddFunction() const override final { return &addFree; }
};


}
