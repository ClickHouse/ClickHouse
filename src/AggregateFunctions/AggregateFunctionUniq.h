#pragma once

#include <atomic>
#include <memory>
#include <type_traits>
#include <utility>
#include <city.h>

#include <base/bit_cast.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>

#include <Interpreters/AggregationCommon.h>

#include <Common/CombinedCardinalityEstimator.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HyperLogLogWithSmallSetOptimization.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/ThetaSketchData.h>
#include <AggregateFunctions/UniqExactSet.h>
#include <AggregateFunctions/UniqVariadicHash.h>
#include <AggregateFunctions/UniquesHashSet.h>

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace DB
{
struct Settings;

/// uniq

struct AggregateFunctionUniqUniquesHashSetData
{
    using Set = UniquesHashSet<DefaultHash<UInt64>>;
    Set set;

    constexpr static bool is_able_to_parallelize_merge = false;
    constexpr static bool is_parallelize_merge_prepare_needed = false;
    constexpr static bool is_variadic = false;

    static String getName() { return "uniq"; }
};

/// For a function that takes multiple arguments. Such a function pre-hashes them in advance, so TrivialHash is used here.
template <bool is_exact_, bool argument_is_tuple_>
struct AggregateFunctionUniqUniquesHashSetDataForVariadic
{
    using Set = UniquesHashSet<TrivialHash>;
    Set set;

    constexpr static bool is_able_to_parallelize_merge = false;
    constexpr static bool is_parallelize_merge_prepare_needed = false;
    constexpr static bool is_variadic = true;
    constexpr static bool is_exact = is_exact_;
    constexpr static bool argument_is_tuple = argument_is_tuple_;

    static String getName() { return "uniq"; }
};


/// uniqHLL12

template <typename T, bool is_able_to_parallelize_merge_>
struct AggregateFunctionUniqHLL12Data
{
    using Set = HyperLogLogWithSmallSetOptimization<T, 16, 12>;
    Set set;

    constexpr static bool is_able_to_parallelize_merge = is_able_to_parallelize_merge_;
    constexpr static bool is_parallelize_merge_prepare_needed = false;
    constexpr static bool is_variadic = false;

    static String getName() { return "uniqHLL12"; }
};

template <>
struct AggregateFunctionUniqHLL12Data<String, false>
{
    using Set = HyperLogLogWithSmallSetOptimization<UInt64, 16, 12>;
    Set set;

    constexpr static bool is_able_to_parallelize_merge = false;
    constexpr static bool is_parallelize_merge_prepare_needed = false;
    constexpr static bool is_variadic = false;

    static String getName() { return "uniqHLL12"; }
};

template <>
struct AggregateFunctionUniqHLL12Data<UUID, false>
{
    using Set = HyperLogLogWithSmallSetOptimization<UInt64, 16, 12>;
    Set set;

    constexpr static bool is_able_to_parallelize_merge = false;
    constexpr static bool is_parallelize_merge_prepare_needed = false;
    constexpr static bool is_variadic = false;

    static String getName() { return "uniqHLL12"; }
};

template <>
struct AggregateFunctionUniqHLL12Data<IPv6, false>
{
    using Set = HyperLogLogWithSmallSetOptimization<UInt64, 16, 12>;
    Set set;

    constexpr static bool is_able_to_parallelize_merge = false;
    constexpr static bool is_parallelize_merge_prepare_needed = false;
    constexpr static bool is_variadic = false;

    static String getName() { return "uniqHLL12"; }
};

template <bool is_exact_, bool argument_is_tuple_, bool is_able_to_parallelize_merge_>
struct AggregateFunctionUniqHLL12DataForVariadic
{
    using Set = HyperLogLogWithSmallSetOptimization<UInt64, 16, 12, TrivialHash>;
    Set set;

    constexpr static bool is_able_to_parallelize_merge = is_able_to_parallelize_merge_;
    constexpr static bool is_parallelize_merge_prepare_needed = false;
    constexpr static bool is_variadic = true;
    constexpr static bool is_exact = is_exact_;
    constexpr static bool argument_is_tuple = argument_is_tuple_;

    static String getName() { return "uniqHLL12"; }
};


/// uniqExact

template <typename T, bool is_able_to_parallelize_merge_>
struct AggregateFunctionUniqExactData
{
    using Key = T;

    /// When creating, the hash table must be small.
    using SingleLevelSet = HashSet<Key, HashCRC32<Key>, HashTableGrower<4>, HashTableAllocatorWithStackMemory<sizeof(Key) * (1 << 4)>>;
    using TwoLevelSet = TwoLevelHashSet<Key, HashCRC32<Key>>;
    using Set = UniqExactSet<SingleLevelSet, TwoLevelSet>;

    Set set;

    constexpr static bool is_able_to_parallelize_merge = is_able_to_parallelize_merge_;
    constexpr static bool is_parallelize_merge_prepare_needed = true;
    constexpr static bool is_variadic = false;

    static String getName() { return "uniqExact"; }
};

/// For rows, we put the SipHash values (128 bits) into the hash table.
template <bool is_able_to_parallelize_merge_>
struct AggregateFunctionUniqExactData<String, is_able_to_parallelize_merge_>
{
    using Key = UInt128;

    /// When creating, the hash table must be small.
    using SingleLevelSet = HashSet<Key, UInt128TrivialHash, HashTableGrower<3>, HashTableAllocatorWithStackMemory<sizeof(Key) * (1 << 3)>>;
    using TwoLevelSet = TwoLevelHashSet<Key, UInt128TrivialHash>;
    using Set = UniqExactSet<SingleLevelSet, TwoLevelSet>;

    Set set;

    constexpr static bool is_able_to_parallelize_merge = is_able_to_parallelize_merge_;
    constexpr static bool is_parallelize_merge_prepare_needed = true;
    constexpr static bool is_variadic = false;

    static String getName() { return "uniqExact"; }
};

/// For historical reasons IPv6 is treated as FixedString(16)
template <bool is_able_to_parallelize_merge_>
struct AggregateFunctionUniqExactData<IPv6, is_able_to_parallelize_merge_>
{
    using Key = UInt128;

    /// When creating, the hash table must be small.
    using SingleLevelSet = HashSet<Key, UInt128TrivialHash, HashTableGrower<3>, HashTableAllocatorWithStackMemory<sizeof(Key) * (1 << 3)>>;
    using TwoLevelSet = TwoLevelHashSet<Key, UInt128TrivialHash>;
    using Set = UniqExactSet<SingleLevelSet, TwoLevelSet>;

    Set set;

    constexpr static bool is_able_to_parallelize_merge = is_able_to_parallelize_merge_;
    constexpr static bool is_parallelize_merge_prepare_needed = true;
    constexpr static bool is_variadic = false;

    static String getName() { return "uniqExact"; }
};

template <bool is_exact_, bool argument_is_tuple_, bool is_able_to_parallelize_merge_>
struct AggregateFunctionUniqExactDataForVariadic : AggregateFunctionUniqExactData<String, is_able_to_parallelize_merge_>
{
    constexpr static bool is_able_to_parallelize_merge = is_able_to_parallelize_merge_;
    constexpr static bool is_parallelize_merge_prepare_needed = true;
    constexpr static bool is_variadic = true;
    constexpr static bool is_exact = is_exact_;
    constexpr static bool argument_is_tuple = argument_is_tuple_;
};

/// uniqTheta
#if USE_DATASKETCHES

struct AggregateFunctionUniqThetaData
{
    using Set = ThetaSketchData<UInt64>;
    Set set;

    constexpr static bool is_able_to_parallelize_merge = false;
    constexpr static bool is_parallelize_merge_prepare_needed = false;
    constexpr static bool is_variadic = false;

    static String getName() { return "uniqTheta"; }
};

template <bool is_exact_, bool argument_is_tuple_>
struct AggregateFunctionUniqThetaDataForVariadic : AggregateFunctionUniqThetaData
{
    constexpr static bool is_able_to_parallelize_merge = false;
    constexpr static bool is_parallelize_merge_prepare_needed = false;
    constexpr static bool is_variadic = true;
    constexpr static bool is_exact = is_exact_;
    constexpr static bool argument_is_tuple = argument_is_tuple_;
};

#endif

namespace detail
{

template <typename T>
struct IsUniqExactSet : std::false_type
{
};

template <typename T1, typename T2>
struct IsUniqExactSet<UniqExactSet<T1, T2>> : std::true_type
{
};


/** Hash function for uniq.
  */
template <typename T> struct AggregateFunctionUniqTraits
{
    static UInt64 hash(T x)
    {
        if constexpr (std::is_same_v<T, Float32> || std::is_same_v<T, Float64>)
        {
            return bit_cast<UInt64>(x);
        }
        else if constexpr (sizeof(T) <= sizeof(UInt64))
        {
            return x;
        }
        else
            return DefaultHash64<T>(x);
    }
};


/** The structure for the delegation work to add elements to the `uniq` aggregate functions.
  * Used for partial specialization to add strings.
  */
template <typename T, typename Data>
struct Adder
{
    /// We have to introduce this template parameter (and a bunch of ugly code dealing with it), because we cannot
    /// add runtime branches in whatever_hash_set::insert - it will immediately pop up in the perf top.
    template <bool use_single_level_hash_table = true>
    static void ALWAYS_INLINE add(Data & data, const IColumn ** columns, size_t num_args, size_t row_num)
    {
        if constexpr (Data::is_variadic)
        {
            if constexpr (IsUniqExactSet<typename Data::Set>::value)
                data.set.template insert<T, use_single_level_hash_table>(
                    UniqVariadicHash<Data::is_exact, Data::argument_is_tuple>::apply(num_args, columns, row_num));
            else
                data.set.insert(T{UniqVariadicHash<Data::is_exact, Data::argument_is_tuple>::apply(num_args, columns, row_num)});
        }
        else if constexpr (
            std::is_same_v<
                Data,
                AggregateFunctionUniqUniquesHashSetData> || std::is_same_v<Data, AggregateFunctionUniqHLL12Data<T, Data::is_able_to_parallelize_merge>>)
        {
            const auto & column = *columns[0];
            if constexpr (std::is_same_v<T, String> || std::is_same_v<T, IPv6>)
            {
                StringRef value = column.getDataAt(row_num);
                data.set.insert(CityHash_v1_0_2::CityHash64(value.data, value.size));
            }
            else
            {
                using ValueType = typename decltype(data.set)::value_type;
                const auto & value = assert_cast<const ColumnVector<T> &>(column).getElement(row_num);
                data.set.insert(static_cast<ValueType>(AggregateFunctionUniqTraits<T>::hash(value)));
            }
        }
        else if constexpr (std::is_same_v<Data, AggregateFunctionUniqExactData<T, Data::is_able_to_parallelize_merge>>)
        {
            const auto & column = *columns[0];
            if constexpr (std::is_same_v<T, String> || std::is_same_v<T, IPv6>)
            {
                StringRef value = column.getDataAt(row_num);

                SipHash hash;
                hash.update(value.data, value.size);
                const auto key = hash.get128();

                data.set.template insert<const UInt128 &, use_single_level_hash_table>(key);
            }
            else
            {
                data.set.template insert<const T &, use_single_level_hash_table>(
                    assert_cast<const ColumnVector<T> &>(column).getData()[row_num]);
            }
        }
#if USE_DATASKETCHES
        else if constexpr (std::is_same_v<Data, AggregateFunctionUniqThetaData>)
        {
            const auto & column = *columns[0];
            data.set.insertOriginal(column.getDataAt(row_num));
        }
#endif
    }

    static void ALWAYS_INLINE
    add(Data & data, const IColumn ** columns, size_t num_args, size_t row_begin, size_t row_end, const char8_t * flags, const UInt8 * null_map)
    {
        bool use_single_level_hash_table = true;
        if constexpr (Data::is_able_to_parallelize_merge)
            use_single_level_hash_table = data.set.isSingleLevel();

        if (use_single_level_hash_table)
            addImpl<true>(data, columns, num_args, row_begin, row_end, flags, null_map);
        else
            addImpl<false>(data, columns, num_args, row_begin, row_end, flags, null_map);

        if constexpr (Data::is_able_to_parallelize_merge)
        {
            if (data.set.isSingleLevel() && data.set.worthConvertingToTwoLevel(data.set.size()))
                data.set.convertToTwoLevel();
        }
    }

private:
    template <bool use_single_level_hash_table>
    static void ALWAYS_INLINE
    addImpl(Data & data, const IColumn ** columns, size_t num_args, size_t row_begin, size_t row_end, const char8_t * flags, const UInt8 * null_map)
    {
        if (!flags)
        {
            if (!null_map)
            {
                for (size_t row = row_begin; row < row_end; ++row)
                    add<use_single_level_hash_table>(data, columns, num_args, row);
            }
            else
            {
                for (size_t row = row_begin; row < row_end; ++row)
                    if (!null_map[row])
                        add<use_single_level_hash_table>(data, columns, num_args, row);
            }
        }
        else
        {
            if (!null_map)
            {
                for (size_t row = row_begin; row < row_end; ++row)
                    if (flags[row])
                        add<use_single_level_hash_table>(data, columns, num_args, row);
            }
            else
            {
                for (size_t row = row_begin; row < row_end; ++row)
                    if (!null_map[row] && flags[row])
                        add<use_single_level_hash_table>(data, columns, num_args, row);
            }
        }
    }
};

}


/// Calculates the number of different values approximately or exactly.
template <typename T, typename Data>
class AggregateFunctionUniq final : public IAggregateFunctionDataHelper<Data, AggregateFunctionUniq<T, Data>>
{
private:
    using DataSet = typename Data::Set;
    static constexpr size_t num_args = 1;
    static constexpr bool is_able_to_parallelize_merge = Data::is_able_to_parallelize_merge;
    static constexpr bool is_parallelize_merge_prepare_needed = Data::is_parallelize_merge_prepare_needed;

public:
    explicit AggregateFunctionUniq(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionUniq<T, Data>>(argument_types_, {}, std::make_shared<DataTypeUInt64>())
    {
    }

    String getName() const override { return Data::getName(); }

    bool allocatesMemoryInArena() const override { return false; }

    /// ALWAYS_INLINE is required to have better code layout for uniqHLL12 function
    void ALWAYS_INLINE add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        detail::Adder<T, Data>::add(this->data(place), columns, num_args, row_num);
    }

    void ALWAYS_INLINE addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *, ssize_t if_argument_pos)
        const override
    {
        const char8_t * flags = nullptr;
        if (if_argument_pos >= 0)
            flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();

        detail::Adder<T, Data>::add(this->data(place), columns, num_args, row_begin, row_end, flags, nullptr /* null_map */);
    }

    void addManyDefaults(
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        size_t /*length*/,
        Arena * /*arena*/) const override
    {
        detail::Adder<T, Data>::add(this->data(place), columns, num_args, 0);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const char8_t * flags = nullptr;
        if (if_argument_pos >= 0)
            flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();

        detail::Adder<T, Data>::add(this->data(place), columns, num_args, row_begin, row_end, flags, null_map);
    }

    bool isParallelizeMergePrepareNeeded() const override { return is_parallelize_merge_prepare_needed; }

    constexpr static bool parallelizeMergeWithKey() { return true; }

    void parallelizeMergePrepare(AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled) const override
    {
        if constexpr (is_parallelize_merge_prepare_needed)
        {
            std::vector<DataSet *> data_vec;
            data_vec.resize(places.size());

            for (size_t i = 0; i < data_vec.size(); ++i)
                data_vec[i] = &this->data(places[i]).set;

            DataSet::parallelizeMergePrepare(data_vec, thread_pool, is_cancelled);
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "parallelizeMergePrepare() is only implemented when is_parallelize_merge_prepare_needed is true for {} ", getName());
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).set.merge(this->data(rhs).set);
    }

    bool isAbleToParallelizeMerge() const override { return is_able_to_parallelize_merge; }
    bool canOptimizeEqualKeysRanges() const override { return !is_able_to_parallelize_merge; }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena *) const override
    {
        if constexpr (is_able_to_parallelize_merge)
            this->data(place).set.merge(this->data(rhs).set, &thread_pool, &is_cancelled);
        else
            this->data(place).set.merge(this->data(rhs).set);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).set.write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).set.read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).set.size());
    }
};


/** For multiple arguments. To compute, hashes them.
  * You can pass multiple arguments as is; You can also pass one argument - a tuple.
  * But (for the possibility of efficient implementation), you can not pass several arguments, among which there are tuples.
  */
template <typename Data>
class AggregateFunctionUniqVariadic final : public IAggregateFunctionDataHelper<Data, AggregateFunctionUniqVariadic<Data>>
{
private:
    using T = typename Data::Set::value_type;

    static constexpr size_t is_able_to_parallelize_merge = Data::is_able_to_parallelize_merge;
    static constexpr size_t argument_is_tuple = Data::argument_is_tuple;

    size_t num_args = 0;

public:
    explicit AggregateFunctionUniqVariadic(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionUniqVariadic<Data>>(arguments, {}, std::make_shared<DataTypeUInt64>())
    {
        if (argument_is_tuple)
            num_args = typeid_cast<const DataTypeTuple &>(*arguments[0]).getElements().size();
        else
            num_args = arguments.size();
    }

    String getName() const override { return Data::getName(); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        detail::Adder<T, Data>::add(this->data(place), columns, num_args, row_num);
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *, ssize_t if_argument_pos)
        const override
    {
        const char8_t * flags = nullptr;
        if (if_argument_pos >= 0)
            flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();

        detail::Adder<T, Data>::add(this->data(place), columns, num_args, row_begin, row_end, flags, nullptr /* null_map */);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const char8_t * flags = nullptr;
        if (if_argument_pos >= 0)
            flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();

        detail::Adder<T, Data>::add(this->data(place), columns, num_args, row_begin, row_end, flags, null_map);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).set.merge(this->data(rhs).set);
    }

    bool isAbleToParallelizeMerge() const override { return is_able_to_parallelize_merge; }
    bool canOptimizeEqualKeysRanges() const override { return !is_able_to_parallelize_merge; }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena *) const override
    {
        if constexpr (is_able_to_parallelize_merge)
            this->data(place).set.merge(this->data(rhs).set, &thread_pool, &is_cancelled);
        else
            this->data(place).set.merge(this->data(rhs).set);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).set.write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).set.read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).set.size());
    }
};

}
