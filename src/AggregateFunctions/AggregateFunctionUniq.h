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
#include <Common/CurrentThread.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HyperLogLogWithSmallSetOptimization.h>
#include <Common/assert_cast.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>

#include <AggregateFunctions/UniquesHashSet.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/ThetaSketchData.h>
#include <AggregateFunctions/UniqVariadicHash.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace detail
{
    enum class HTKind
    {
        SingleLevel,
        TwoLevel,
        Unknown
    };

    template <typename SingleLevelSet, typename TwoLevelSet>
    class Set
    {
        static_assert(std::is_same_v<typename SingleLevelSet::value_type, typename TwoLevelSet::value_type>);

    public:
        using value_type = typename SingleLevelSet::value_type;

        template <typename Arg, HTKind ht_kind = HTKind::Unknown>
        auto ALWAYS_INLINE insert(Arg && arg)
        {
            if constexpr (ht_kind == HTKind::TwoLevel)
                asTwoLevel().insert(std::forward<Arg>(arg));
            else
                asSingleLevel().insert(std::forward<Arg>(arg));
        }

        auto merge(const Set & other, ThreadPool * thread_pool = nullptr)
        {
            if (isSingleLevel() && other.isTwoLevel())
                convertToTwoLevel();

            if (isSingleLevel())
            {
                asSingleLevel().merge(other.asSingleLevel());
            }
            else
            {
                auto & lhs = asTwoLevel();
                const auto rhs_ptr = other.getTwoLevelSet();
                const auto & rhs = *rhs_ptr;
                if (!thread_pool)
                {
                    for (size_t i = 0; i < rhs.NUM_BUCKETS; ++i)
                        lhs.impls[i].merge(rhs.impls[i]);
                }
                else
                {
                    auto next_bucket_to_merge = std::make_shared<std::atomic_uint32_t>(0);

                    auto thread_func = [&lhs, &rhs, next_bucket_to_merge, thread_group = CurrentThread::getGroup()]()
                    {
                        if (thread_group)
                            CurrentThread::attachToIfDetached(thread_group);
                        setThreadName("UniqExactMerger");

                        while (true)
                        {
                            const auto bucket = next_bucket_to_merge->fetch_add(1);
                            if (bucket >= rhs.NUM_BUCKETS)
                                return;
                            lhs.impls[bucket].merge(rhs.impls[bucket]);
                        }
                    };

                    for (size_t i = 0; i < thread_pool->getMaxThreads(); ++i)
                        thread_pool->scheduleOrThrowOnError(thread_func);
                    thread_pool->wait();
                }
            }
        }

        void read(ReadBuffer & in)
        {
            asSingleLevel().read(in);
        }

        void write(WriteBuffer & out) const
        {
            if (isSingleLevel())
                asSingleLevel().write(out);
            else
                getTwoLevelSet()->writeAsSingleLevel(out);
        }

        size_t size() const { return isSingleLevel() ? asSingleLevel().size() : asTwoLevel().size(); }

        std::shared_ptr<TwoLevelSet> getTwoLevelSet() const
        {
            return two_level_set ? two_level_set : std::make_shared<TwoLevelSet>(asSingleLevel());
        }

        void convertToTwoLevel()
        {
            two_level_set = getTwoLevelSet();
            single_level_set.clear();
        }

        bool isSingleLevel() const { return !two_level_set; }
        bool isTwoLevel() const { return !!two_level_set; }

    private:
        SingleLevelSet & asSingleLevel() { return single_level_set; }
        const SingleLevelSet & asSingleLevel() const { return single_level_set; }

        TwoLevelSet & asTwoLevel() { return *two_level_set; }
        const TwoLevelSet & asTwoLevel() const { return *two_level_set; }

        SingleLevelSet single_level_set;
        std::shared_ptr<TwoLevelSet> two_level_set;
    };
}


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

template <>
struct AggregateFunctionUniqHLL12Data<UUID>
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
    using SingleLevelSet = HashSet<Key, HashCRC32<Key>, HashTableGrower<4>, HashTableAllocatorWithStackMemory<sizeof(Key) * (1 << 4)>>;
    using TwoLevelSet = TwoLevelHashSet<Key, HashCRC32<Key>>;
    using Set = detail::Set<SingleLevelSet, TwoLevelSet>;

    Set set;

    static String getName() { return "uniqExact"; }
};

/// For rows, we put the SipHash values (128 bits) into the hash table.
template <>
struct AggregateFunctionUniqExactData<String>
{
    using Key = UInt128;

    /// When creating, the hash table must be small.
    using SingleLevelSet = HashSet<Key, UInt128TrivialHash, HashTableGrower<3>, HashTableAllocatorWithStackMemory<sizeof(Key) * (1 << 3)>>;
    using TwoLevelSet = TwoLevelHashSet<Key, UInt128TrivialHash>;
    using Set = detail::Set<SingleLevelSet, TwoLevelSet>;

    Set set;

    static String getName() { return "uniqExact"; }
};


/// uniqTheta
#if USE_DATASKETCHES

struct AggregateFunctionUniqThetaData
{
    using Set = ThetaSketchData<UInt64>;
    Set set;

    static String getName() { return "uniqTheta"; }
};

#endif

namespace detail
{

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
    template <HTKind ht_kind = HTKind::Unknown>
    static void ALWAYS_INLINE add(Data & data, const IColumn & column, size_t row_num)
    {
        if constexpr (std::is_same_v<Data, AggregateFunctionUniqUniquesHashSetData>
            || std::is_same_v<Data, AggregateFunctionUniqHLL12Data<T>>)
        {
            if constexpr (!std::is_same_v<T, String>)
            {
                using ValueType = typename decltype(data.set)::value_type;
                const auto & value = assert_cast<const ColumnVector<T> &>(column).getElement(row_num);
                data.set.insert(static_cast<ValueType>(AggregateFunctionUniqTraits<T>::hash(value)));
            }
            else
            {
                StringRef value = column.getDataAt(row_num);
                data.set.insert(CityHash_v1_0_2::CityHash64(value.data, value.size));
            }
        }
        else if constexpr (std::is_same_v<Data, AggregateFunctionUniqExactData<T>>)
        {
            if constexpr (!std::is_same_v<T, String>)
            {
                using Type = decltype(std::declval<const ColumnVector<T> &>().getData()[row_num]);
                data.set.template insert<Type, ht_kind>(assert_cast<const ColumnVector<T> &>(column).getData()[row_num]);
            }
            else
            {
                StringRef value = column.getDataAt(row_num);

                UInt128 key;
                SipHash hash;
                hash.update(value.data, value.size);
                hash.get128(key);

                data.set.template insert<const UInt128 &, ht_kind>(key);
            }
        }
#if USE_DATASKETCHES
        else if constexpr (std::is_same_v<Data, AggregateFunctionUniqThetaData>)
        {
            data.set.insertOriginal(column.getDataAt(row_num));
        }
#endif
    }

    static void add(Data & data, const IColumn & column, size_t row_begin, size_t row_end, const char8_t * flags, const UInt8 * null_map)
    {
        HTKind ht_kind = HTKind::Unknown;
        if constexpr (std::is_same_v<Data, AggregateFunctionUniqExactData<T>>)
            ht_kind = data.set.isSingleLevel() ? HTKind::SingleLevel : HTKind::TwoLevel;

        if (ht_kind == HTKind::Unknown)
            add<HTKind::Unknown>(data, column, row_begin, row_end, flags, null_map);
        else if (ht_kind == HTKind::SingleLevel)
            add<HTKind::SingleLevel>(data, column, row_begin, row_end, flags, null_map);
        else
            add<HTKind::TwoLevel>(data, column, row_begin, row_end, flags, null_map);

        if constexpr (std::is_same_v<Data, AggregateFunctionUniqExactData<T>>)
        {
            if (data.set.isSingleLevel() && data.set.size() > 100'000)
                data.set.convertToTwoLevel();
        }
    }

private:
    template <HTKind ht_kind>
    static void add(Data & data, const IColumn & column, size_t row_begin, size_t row_end, const char8_t * flags, const UInt8 * null_map)
    {
        if (!flags)
        {
            if (!null_map)
            {
                for (size_t row = row_begin; row < row_end; ++row)
                    add<ht_kind>(data, column, row);
            }
            else
            {
                for (size_t row = row_begin; row < row_end; ++row)
                    if (!null_map[row])
                        add<ht_kind>(data, column, row);
            }
        }
        else
        {
            if (!null_map)
            {
                for (size_t row = row_begin; row < row_end; ++row)
                    if (flags[row])
                        add<ht_kind>(data, column, row);
            }
            else
            {
                for (size_t row = row_begin; row < row_end; ++row)
                    if (!null_map[row] && flags[row])
                        add<ht_kind>(data, column, row);
            }
        }
    }
};

}


/// Calculates the number of different values approximately or exactly.
template <typename T, typename Data>
class AggregateFunctionUniq final : public IAggregateFunctionDataHelper<Data, AggregateFunctionUniq<T, Data>>
{
public:
    explicit AggregateFunctionUniq(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionUniq<T, Data>>(argument_types_, {})
    {
    }

    String getName() const override { return Data::getName(); }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    /// ALWAYS_INLINE is required to have better code layout for uniqHLL12 function
    void ALWAYS_INLINE add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        detail::Adder<T, Data>::add(this->data(place), *columns[0], row_num);
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *, ssize_t if_argument_pos)
        const override
    {
        const char8_t * flags = nullptr;
        if (if_argument_pos >= 0)
            flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();

        detail::Adder<T, Data>::add(this->data(place), *columns[0], row_begin, row_end, flags, nullptr /* null_map */);
    }

    void addManyDefaults(
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        size_t /*length*/,
        Arena * /*arena*/) const override
    {
        detail::Adder<T, Data>::add(this->data(place), *columns[0], 0);
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

        detail::Adder<T, Data>::add(this->data(place), *columns[0], row_begin, row_end, flags, null_map);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).set.merge(this->data(rhs).set);
    }

    bool isAbleToParallelizeMerge() const override
    {
        /// Implemented only for uniqExact().
        return std::is_same_v<Data, AggregateFunctionUniqExactData<T>>;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, ThreadPool & thread_pool, Arena *) const override
    {
        if constexpr (std::is_same_v<Data, AggregateFunctionUniqExactData<T>>)
            this->data(place).set.merge(this->data(rhs).set, &thread_pool);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "merge() with thread pool parameter isn't implemented for {} ", getName());
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
template <typename Data, bool is_exact, bool argument_is_tuple>
class AggregateFunctionUniqVariadic final : public IAggregateFunctionDataHelper<Data, AggregateFunctionUniqVariadic<Data, is_exact, argument_is_tuple>>
{
private:
    size_t num_args = 0;

public:
    explicit AggregateFunctionUniqVariadic(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionUniqVariadic<Data, is_exact, argument_is_tuple>>(arguments, {})
    {
        if (argument_is_tuple)
            num_args = typeid_cast<const DataTypeTuple &>(*arguments[0]).getElements().size();
        else
            num_args = arguments.size();
    }

    String getName() const override { return Data::getName(); }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).set.insert(typename Data::Set::value_type(
            UniqVariadicHash<is_exact, argument_is_tuple>::apply(num_args, columns, row_num)));
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
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
