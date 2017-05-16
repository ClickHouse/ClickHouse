#pragma once

#include <city.h>
#include <type_traits>

#include <AggregateFunctions/UniquesHashSet.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>

#include <Interpreters/AggregationCommon.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HyperLogLogWithSmallSetOptimization.h>
#include <Common/CombinedCardinalityEstimator.h>
#include <Common/MemoryTracker.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>

#include <AggregateFunctions/IUnaryAggregateFunction.h>
#include <AggregateFunctions/UniqCombinedBiasData.h>
#include <AggregateFunctions/UniqVariadicHash.h>


namespace DB
{

/// uniq

struct AggregateFunctionUniqUniquesHashSetData
{
    using Set = UniquesHashSet<DefaultHash<UInt64>>;
    Set set;

    static String getName() { return "uniq"; }
};

/// For a function that takes multiple arguments. Such a function pre-hashes them in advance, so TrivialHash is used here.
struct AggregateFunctionUniqUniquesHashSetDataForVariadic
{
    using Set = UniquesHashSet<TrivialHash>;
    Set set;

    static String getName() { return "uniq"; }
};


/// uniqHLL12

template <typename T>
struct AggregateFunctionUniqHLL12Data
{
    using Set = HyperLogLogWithSmallSetOptimization<T, 16, 12>;
    Set set;

    static String getName() { return "uniqHLL12"; }
};

template <>
struct AggregateFunctionUniqHLL12Data<String>
{
    using Set = HyperLogLogWithSmallSetOptimization<UInt64, 16, 12>;
    Set set;

    static String getName() { return "uniqHLL12"; }
};

struct AggregateFunctionUniqHLL12DataForVariadic
{
    using Set = HyperLogLogWithSmallSetOptimization<UInt64, 16, 12, TrivialHash>;
    Set set;

    static String getName() { return "uniqHLL12"; }
};


/// uniqExact

template <typename T>
struct AggregateFunctionUniqExactData
{
    using Key = T;

    /// When creating, the hash table must be small.
    using Set = HashSet<
        Key,
        HashCRC32<Key>,
        HashTableGrower<4>,
        HashTableAllocatorWithStackMemory<sizeof(Key) * (1 << 4)>>;

    Set set;

    static String getName() { return "uniqExact"; }
};

/// For rows, we put the SipHash values (128 bits) into the hash table.
template <>
struct AggregateFunctionUniqExactData<String>
{
    using Key = UInt128;

    /// When creating, the hash table must be small.
    using Set = HashSet<
        Key,
        UInt128TrivialHash,
        HashTableGrower<3>,
        HashTableAllocatorWithStackMemory<sizeof(Key) * (1 << 3)>>;

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

/// Aggregate functions uniqCombinedRaw, uniqCombinedLinearCounting, and uniqCombinedBiasCorrected
///  are intended for development of new versions of the uniqCombined function.
/// Users should only use uniqCombined.

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

/** Hash function for uniq.
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

/** Hash function for uniqCombined.
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

/** The structure for the delegation work to add one element to the `uniq` aggregate functions.
  * Used for partial specialization to add strings.
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
    static void addImpl(Data & data, const IColumn & column,    size_t row_num,
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
    static void addImpl(Data & data, const IColumn & column,    size_t row_num,
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


/// Calculates the number of different values approximately or exactly.
template <typename T, typename Data>
class AggregateFunctionUniq final : public IUnaryAggregateFunction<Data, AggregateFunctionUniq<T, Data> >
{
public:
    String getName() const override { return Data::getName(); }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void setArgument(const DataTypePtr & argument)
    {
    }

    void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena *) const
    {
        detail::OneAdder<T, Data>::addImpl(this->data(place), column, row_num);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).set.merge(this->data(rhs).set);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).set.write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).set.read(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        static_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).set.size());
    }
};


/** For multiple arguments. To compute, hashes them.
  * You can pass multiple arguments as is; You can also pass one argument - a tuple.
  * But (for the possibility of effective implementation), you can not pass several arguments, among which there are tuples.
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
        return std::make_shared<DataTypeUInt64>();
    }

    void setArguments(const DataTypes & arguments) override
    {
        if (argument_is_tuple)
            num_args = typeid_cast<const DataTypeTuple &>(*arguments[0]).getElements().size();
        else
            num_args = arguments.size();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).set.insert(UniqVariadicHash<is_exact, argument_is_tuple>::apply(num_args, columns, row_num));
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).set.merge(this->data(rhs).set);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).set.write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).set.read(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        static_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).set.size());
    }

    static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena)
    {
        static_cast<const AggregateFunctionUniqVariadic &>(*that).add(place, columns, row_num, arena);
    }

    IAggregateFunction::AddFunc getAddressOfAddFunction() const override final { return &addFree; }
};


}
