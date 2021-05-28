#pragma once

#include <Common/CombinedCardinalityEstimator.h>
#include <Common/FieldVisitors.h>
#include <Common/SipHash.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/UniqCombinedBiasData.h>
#include <AggregateFunctions/UniqVariadicHash.h>

#include <ext/bit_cast.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{
namespace detail
{
    /** Hash function for uniqCombined/uniqCombined64 (based on Ret).
     */
    template <typename T, typename Ret>
    struct AggregateFunctionUniqCombinedTraits
    {
        static Ret hash(T x)
        {
            if constexpr (sizeof(T) > sizeof(UInt64))
                return static_cast<Ret>(DefaultHash64<T>(x));
            else
                return static_cast<Ret>(intHash64(x));
        }
    };

    template <typename Ret>
    struct AggregateFunctionUniqCombinedTraits<UInt128, Ret>
    {
        static Ret hash(UInt128 x)
        {
            return sipHash64(x);
        }
    };

    template <typename Ret>
    struct AggregateFunctionUniqCombinedTraits<Float32, Ret>
    {
        static Ret hash(Float32 x)
        {
            UInt64 res = ext::bit_cast<UInt64>(x);
            return static_cast<Ret>(intHash64(res));
        }
    };

    template <typename Ret>
    struct AggregateFunctionUniqCombinedTraits<Float64, Ret>
    {
        static Ret hash(Float64 x)
        {
            UInt64 res = ext::bit_cast<UInt64>(x);
            return static_cast<Ret>(intHash64(res));
        }
    };

}

// Unlike HashTableGrower always grows to power of 2.
struct UniqCombinedHashTableGrower : public HashTableGrower<>
{
    void increaseSize() { ++size_degree; }
};

template <typename Key, UInt8 K>
struct AggregateFunctionUniqCombinedDataWithKey
{
    // TODO(ilezhankin): pre-generate values for |UniqCombinedBiasData|,
    //                   at the moment gen-bias-data.py script doesn't work.

    // We want to migrate from |HashSet| to |HyperLogLogCounter| when the sizes in memory become almost equal.
    // The size per element in |HashSet| is sizeof(Key)*2 bytes, and the overall size of |HyperLogLogCounter| is 2^K * 6 bits.
    // For Key=UInt32 we can calculate: 2^X * 4 * 2 ≤ 2^(K-3) * 6 ⇒ X ≤ K-4.
    using Set = CombinedCardinalityEstimator<Key, HashSet<Key, TrivialHash, UniqCombinedHashTableGrower>, 16, K - 5 + (sizeof(Key) == sizeof(UInt32)), K, TrivialHash, Key>;

    Set set;
};

template <typename Key>
struct AggregateFunctionUniqCombinedDataWithKey<Key, 17>
{
    using Set = CombinedCardinalityEstimator<Key,
        HashSet<Key, TrivialHash, UniqCombinedHashTableGrower>,
        16,
        12 + (sizeof(Key) == sizeof(UInt32)),
        17,
        TrivialHash,
        Key,
        HyperLogLogBiasEstimator<UniqCombinedBiasData>,
        HyperLogLogMode::FullFeatured>;

    Set set;
};


template <typename T, UInt8 K, typename HashValueType>
struct AggregateFunctionUniqCombinedData : public AggregateFunctionUniqCombinedDataWithKey<HashValueType, K>
{
};


/// For String keys, 64 bit hash is always used (both for uniqCombined and uniqCombined64),
///  because of backwards compatibility (64 bit hash was already used for uniqCombined).
template <UInt8 K, typename HashValueType>
struct AggregateFunctionUniqCombinedData<String, K, HashValueType> : public AggregateFunctionUniqCombinedDataWithKey<UInt64 /*always*/, K>
{
};


template <typename T, UInt8 K, typename HashValueType>
class AggregateFunctionUniqCombined final
    : public IAggregateFunctionDataHelper<AggregateFunctionUniqCombinedData<T, K, HashValueType>, AggregateFunctionUniqCombined<T, K, HashValueType>>
{
public:
    AggregateFunctionUniqCombined(const DataTypes & argument_types_, const Array & params_)
        : IAggregateFunctionDataHelper<AggregateFunctionUniqCombinedData<T, K, HashValueType>, AggregateFunctionUniqCombined<T, K, HashValueType>>(argument_types_, params_) {}

    String getName() const override
    {
        if constexpr (std::is_same_v<HashValueType, UInt64>)
            return "uniqCombined64";
        else
            return "uniqCombined";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if constexpr (!std::is_same_v<T, String>)
        {
            const auto & value = assert_cast<const ColumnVector<T> &>(*columns[0]).getElement(row_num);
            this->data(place).set.insert(detail::AggregateFunctionUniqCombinedTraits<T, HashValueType>::hash(value));
        }
        else
        {
            StringRef value = columns[0]->getDataAt(row_num);
            this->data(place).set.insert(CityHash_v1_0_2::CityHash64(value.data, value.size));
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
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

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).set.size());
    }
};

/** For multiple arguments. To compute, hashes them.
  * You can pass multiple arguments as is; You can also pass one argument - a tuple.
  * But (for the possibility of efficient implementation), you can not pass several arguments, among which there are tuples.
  */
template <bool is_exact, bool argument_is_tuple, UInt8 K, typename HashValueType>
class AggregateFunctionUniqCombinedVariadic final : public IAggregateFunctionDataHelper<AggregateFunctionUniqCombinedData<UInt64, K, HashValueType>,
                                                           AggregateFunctionUniqCombinedVariadic<is_exact, argument_is_tuple, K, HashValueType>>
{
private:
    size_t num_args = 0;

public:
    explicit AggregateFunctionUniqCombinedVariadic(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionUniqCombinedData<UInt64, K, HashValueType>,
            AggregateFunctionUniqCombinedVariadic<is_exact, argument_is_tuple, K, HashValueType>>(arguments, params)
    {
        if (argument_is_tuple)
            num_args = typeid_cast<const DataTypeTuple &>(*arguments[0]).getElements().size();
        else
            num_args = arguments.size();
    }

    String getName() const override
    {
        if constexpr (std::is_same_v<HashValueType, UInt64>)
            return "uniqCombined64";
        else
            return "uniqCombined";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).set.insert(typename AggregateFunctionUniqCombinedData<UInt64, K, HashValueType>::Set::value_type(
            UniqVariadicHash<is_exact, argument_is_tuple>::apply(num_args, columns, row_num)));
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
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

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).set.size());
    }
};

}
