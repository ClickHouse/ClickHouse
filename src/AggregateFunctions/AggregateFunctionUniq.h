#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>
#include <city.h>

#include <base/bit_cast.h>
#include <base/normalizeNegativeZero.h>


#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>

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
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
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
struct AggregateFunctionUniqHLL12Data<std::string_view, false>
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
    static constexpr size_t initial_size_degree = 4;
    using SingleLevelSet = HashSetWithStackMemory<Key, HashCRC32<Key>, initial_size_degree>;
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
struct AggregateFunctionUniqExactData<std::string_view, is_able_to_parallelize_merge_>
{
    using Key = UInt128;

    /// When creating, the hash table must be small.
    static constexpr size_t initial_size_degree = 3;
    using SingleLevelSet = HashSetWithStackMemory<Key, UInt128TrivialHash, initial_size_degree>;
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
    static constexpr size_t initial_size_degree = 3;
    using SingleLevelSet = HashSetWithStackMemory<Key, UInt128TrivialHash, initial_size_degree>;
    using TwoLevelSet = TwoLevelHashSet<Key, UInt128TrivialHash>;
    using Set = UniqExactSet<SingleLevelSet, TwoLevelSet>;

    Set set;

    constexpr static bool is_able_to_parallelize_merge = is_able_to_parallelize_merge_;
    constexpr static bool is_parallelize_merge_prepare_needed = true;
    constexpr static bool is_variadic = false;

    static String getName() { return "uniqExact"; }
};

template <bool is_exact_, bool argument_is_tuple_, bool is_able_to_parallelize_merge_>
struct AggregateFunctionUniqExactDataForVariadic : AggregateFunctionUniqExactData<std::string_view, is_able_to_parallelize_merge_>
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
template <typename T, typename ColumnType>
struct AggregateFunctionUniqTraits
{
    static ALWAYS_INLINE auto hash(T x)
    {
        if constexpr (std::is_same_v<T, std::string_view>)
        {
            return CityHash_v1_0_2::CityHash64(x.data(), x.size());
        }
        else if constexpr (std::is_same_v<T, IPv6>)
        {
            /// For compatibility reasons IPv6 is hashed as FixedString(16).
            return CityHash_v1_0_2::CityHash64(reinterpret_cast<const char *>(&x), sizeof(x));
        }
        else if constexpr (is_floating_point<T>)
        {
            /// Negative zero is equal to positive zero, but has a different binary representation.
            return bit_cast<UInt64>(normalizeNegativeZero(x));
        }
        else if constexpr (sizeof(T) <= sizeof(UInt64))
        {
            return x;
        }
        else
        {
            return DefaultHash64<T>(x);
        }
    }

    static ALWAYS_INLINE auto hashExact(T x)
    {
        if constexpr (std::is_same_v<T, std::string_view>)
        {
            SipHash hash;
            hash.update(x.data(), x.size());
            return hash.get128();
        }
        else if constexpr (std::is_same_v<T, IPv6>)
        {
            SipHash hash;
            hash.update(reinterpret_cast<const char *>(&x), sizeof(x));
            return hash.get128();
        }
        else
        {
            return x;
        }
    }

    static ALWAYS_INLINE T value(const IColumn & column, size_t row_num)
    {
        if constexpr (std::is_same_v<T, std::string_view>)
        {
            return assert_cast<const ColumnType &>(column).getDataAt(row_num);
        }
        else
        {
            return assert_cast<const ColumnType &>(column).getData()[row_num];
        }
    }
};


/** The structure for the delegation work to add elements to the `uniq` aggregate functions.
  * Used for partial specialization to add strings.
  */
template <typename T, typename ColumnType, typename Data>
struct Adder
{
    /// We have to introduce this template parameter (and a bunch of ugly code dealing with it), because we cannot
    /// add runtime branches in whatever_hash_set::insert - it will immediately pop up in the perf top.
    template <SetLevelHint hint = Data::is_able_to_parallelize_merge ? SetLevelHint::unknown : SetLevelHint::singleLevel>
    static void ALWAYS_INLINE add(Data & data, const IColumn ** columns, size_t num_args, size_t row_num)
    {
        if constexpr (Data::is_variadic)
        {
            if constexpr (IsUniqExactSet<typename Data::Set>::value)
                data.set.template insert<T, hint>(UniqVariadicHash<Data::is_exact, Data::argument_is_tuple>::apply(num_args, columns, row_num));
            else
                data.set.insert(T{UniqVariadicHash<Data::is_exact, Data::argument_is_tuple>::apply(num_args, columns, row_num)});
        }
#if USE_DATASKETCHES
        else if constexpr (std::is_same_v<Data, AggregateFunctionUniqThetaData>)
        {
            const auto & column = *columns[0];
            if constexpr (is_floating_point<T>)
            {
                /// Negative zero is equal to positive zero, but has a different binary representation.
                const T value = normalizeNegativeZero(assert_cast<const ColumnType &>(column).getData()[row_num]);
                data.set.insertOriginal(std::string_view(reinterpret_cast<const char *>(&value), sizeof(value)));
            }
            else
                data.set.insertOriginal(column.getDataAt(row_num));
        }
#endif
        else
        {
            const auto value = AggregateFunctionUniqTraits<T, ColumnType>::value(*columns[0], row_num);
            const auto key = getKey(value);
            insertOneKey<hint>(data, key);
        }
    }

    static void addMany(Data & data, const IColumn ** columns, size_t num_args, size_t row_begin, size_t row_end, const char8_t * flags, const UInt8 * null_map)
    {
        bool use_single_level_hash_table = true;
        if constexpr (Data::is_able_to_parallelize_merge)
            use_single_level_hash_table = data.set.isSingleLevel();

        if (use_single_level_hash_table)
            addManyImpl<SetLevelHint::singleLevel>(data, columns, num_args, row_begin, row_end, flags, null_map);
        else
            addManyImpl<SetLevelHint::twoLevel>(data, columns, num_args, row_begin, row_end, flags, null_map);

        if constexpr (Data::is_able_to_parallelize_merge)
        {
            if (data.set.isSingleLevel() && data.set.worthConvertingToTwoLevel(data.set.size()))
                data.set.convertToTwoLevel();
        }
    }

private:
    static constexpr bool is_uniques_hash_set = std::is_same_v<Data, AggregateFunctionUniqUniquesHashSetData>;
    static constexpr bool is_hll = std::is_same_v<Data, AggregateFunctionUniqHLL12Data<T, Data::is_able_to_parallelize_merge>>;
    static constexpr bool is_uniq_exact = std::is_same_v<Data, AggregateFunctionUniqExactData<T, Data::is_able_to_parallelize_merge>>;

    /// The batched path applies to non-variadic functions whose sets support insertion of whole chunks of keys.
    static constexpr bool is_batchable = is_uniques_hash_set || is_hll || is_uniq_exact;
    static constexpr size_t hash_chunk_size = 256;

    /// Returns the key inserted into the set:
    /// for `uniq` and `uniqHLL12` a 64-bit hash of the value cast to the set's value type,
    /// for `uniqExact` the value itself for native types, a 128-bit SipHash otherwise.
    static ALWAYS_INLINE auto getKey(T value)
    {
        if constexpr (is_uniques_hash_set || is_hll)
        {
            using ValueType = typename Data::Set::value_type;
            return static_cast<ValueType>(AggregateFunctionUniqTraits<T, ColumnType>::hash(value));
        }
        else if constexpr (is_uniq_exact)
        {
            return AggregateFunctionUniqTraits<T, ColumnType>::hashExact(value);
        }
        else
        {
            static_assert(false, "Unsupported set type");
        }
    }

    template <SetLevelHint hint, typename Key>
    static ALWAYS_INLINE void insertOneKey(Data & data, Key key)
    {
        if constexpr (is_uniq_exact)
            data.set.template insert<const Key &, hint>(key);
        else if constexpr (is_uniques_hash_set || is_hll)
            data.set.insert(key);
        else
            static_assert(false, "Unsupported set type");
    }

    template <SetLevelHint hint, typename Key>
    static ALWAYS_INLINE void insertManyKeys(Data & data, const Key * keys, size_t num_keys)
    {
        if constexpr (is_uniq_exact)
            data.set.template insertMany<hint>(keys, num_keys);
        else if constexpr (is_uniques_hash_set)
            data.set.template insertMany<Key, std::identity{}>(keys, num_keys);
        else if constexpr (is_hll)
            data.set.insertMany(keys, num_keys);
        else
            static_assert(false, "Unsupported set type");
    }

    /// Computes keys for a chunk of rows ahead into a stack buffer, so that the key computation
    /// pipelines independently of the set inserts, and inserts whole chunks via insertMany.
    /// Caches the last value to skip consecutive duplicates (skipping a duplicate never changes
    /// the state of the set); the cache is disabled adaptively when such duplicates are rare.
    template <SetLevelHint hint>
    static void addManyChunked(Data & data, const IColumn & column, size_t row_begin, size_t row_end)
    {
        if (row_begin >= row_end)
            return;

        /// Shortcut for numeric type for which hash function is cheap,
        /// and it doesn't make sense to batch its calculation with last value cache.
        if constexpr (!std::is_same_v<T, std::string_view> && !std::is_same_v<T, IPv6>)
        {
            if constexpr (is_uniques_hash_set)
            {
                const auto & column_data = assert_cast<const ColumnType &>(column).getData();
                data.set.template insertMany<T, AggregateFunctionUniqTraits<T, ColumnType>::hash>(column_data.data() + row_begin, row_end - row_begin);
                return;
            }
        }

        using Key = decltype(getKey(AggregateFunctionUniqTraits<T, ColumnType>::value(column, row_begin)));
        std::array<Key, hash_chunk_size> keys; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - only the first num_keys entries are written before read

        size_t row = row_begin;

        bool use_last_value_cache = true;
        T last_value{};
        size_t cache_hits = 0;
        size_t num_processed = 0;

        while (row < row_end)
        {
            const size_t chunk_size = std::min(hash_chunk_size, row_end - row);
            size_t num_keys = 0;

            if (use_last_value_cache)
            {
                last_value = AggregateFunctionUniqTraits<T, ColumnType>::value(column, row);
                keys[num_keys++] = getKey(last_value);

                for (size_t i = 1; i < chunk_size; ++i)
                {
                    auto value = AggregateFunctionUniqTraits<T, ColumnType>::value(column, row + i);
                    if (bitEquals(value, last_value))
                        continue;

                    last_value = value;
                    keys[num_keys++] = getKey(value);
                }

                /// Disable the cache for the rest of the batch when consecutive duplicates are rare.
                cache_hits += chunk_size - num_keys;
                num_processed += chunk_size;

                if (cache_hits * 2 <= num_processed)
                    use_last_value_cache = false;
            }
            else
            {
                num_keys = chunk_size;

                for (size_t i = 0; i < chunk_size; ++i)
                    keys[i] = getKey(AggregateFunctionUniqTraits<T, ColumnType>::value(column, row + i));
            }

            insertManyKeys<hint>(data, keys.data(), num_keys);
            row += chunk_size;
        }
    }

    template <SetLevelHint hint>
    static void addManyImpl(
        Data & data,
        const IColumn ** columns,
        size_t num_args,
        size_t row_begin,
        size_t row_end,
        const char8_t * flags,
        const UInt8 * null_map)
    {
        if (flags && null_map)
        {
            for (size_t row = row_begin; row < row_end; ++row)
                if (!null_map[row] && flags[row])
                    add<hint>(data, columns, num_args, row);
        }
        else if (flags)
        {
            for (size_t row = row_begin; row < row_end; ++row)
                if (flags[row])
                    add<hint>(data, columns, num_args, row);
        }
        else if (null_map)
        {
            for (size_t row = row_begin; row < row_end; ++row)
                if (!null_map[row])
                    add<hint>(data, columns, num_args, row);
        }
        else if constexpr (!is_batchable)
        {
            for (size_t row = row_begin; row < row_end; ++row)
                add<hint>(data, columns, num_args, row);
        }
        else
        {
            chassert(num_args == 1);
            addManyChunked<hint>(data, *columns[0], row_begin, row_end);
        }
    }
};

}


/// Calculates the number of different values approximately or exactly.
template <typename T, typename ColumnType, typename Data>
class AggregateFunctionUniq final : public IAggregateFunctionDataHelper<Data, AggregateFunctionUniq<T, ColumnType, Data>>
{
private:
    using DataSet = typename Data::Set;
    static constexpr size_t num_args = 1;
    static constexpr bool is_able_to_parallelize_merge = Data::is_able_to_parallelize_merge;
    static constexpr bool is_parallelize_merge_prepare_needed = Data::is_parallelize_merge_prepare_needed;

public:
    explicit AggregateFunctionUniq(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionUniq<T, ColumnType, Data>>(argument_types_, {}, std::make_shared<DataTypeUInt64>())
    {
    }

    String getName() const override { return Data::getName(); }

    bool allocatesMemoryInArena() const override { return false; }

    /// ALWAYS_INLINE is required to have better code layout for uniqHLL12 function
    void ALWAYS_INLINE add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        detail::Adder<T, ColumnType, Data>::add(this->data(place), columns, num_args, row_num);
    }

    void ALWAYS_INLINE addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *, ssize_t if_argument_pos)
        const override
    {
        const char8_t * flags = nullptr;
        if (if_argument_pos >= 0)
            flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();

        detail::Adder<T, ColumnType, Data>::addMany(this->data(place), columns, num_args, row_begin, row_end, flags, nullptr /* null_map */);
    }

    void addManyDefaults(
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        size_t /*length*/,
        Arena * /*arena*/) const override
    {
        detail::Adder<T, ColumnType, Data>::add(this->data(place), columns, num_args, 0);
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

        detail::Adder<T, ColumnType, Data>::addMany(this->data(place), columns, num_args, row_begin, row_end, flags, null_map);
    }

    bool isParallelizeMergePrepareNeeded() const override { return is_parallelize_merge_prepare_needed; }

    constexpr static bool parallelizeMergeWithKey() { return true; }

    void parallelizeMergePrepare(AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled) const override
    {
        if constexpr (is_parallelize_merge_prepare_needed)
        {
            DataSet::parallelizeMergePrepare(places, [this](AggregateDataPtr p) { return &this->data(p).set; }, thread_pool, is_cancelled);
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "parallelizeMergePrepare() is only implemented when is_parallelize_merge_prepare_needed is true for {} ", getName());
        }
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).set.merge(this->data(rhs).set);
    }

    bool isAbleToParallelizeMerge() const override { return is_able_to_parallelize_merge; }
    bool canOptimizeEqualKeysRanges() const override { return !is_able_to_parallelize_merge; }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena *) const override
    {
        if constexpr (is_able_to_parallelize_merge)
            this->data(place).set.merge(this->data(rhs).set, &thread_pool, &is_cancelled);
        else
            this->data(place).set.merge(this->data(rhs).set);
    }

    void parallelizeMergeMulti(AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena * arena) const override
    {
        if constexpr (is_able_to_parallelize_merge)
        {
            DataSet::parallelizeMergeMulti(places, [this](AggregateDataPtr p) { return &this->data(p).set; }, thread_pool, is_cancelled);
        }
        else
        {
            IAggregateFunction::parallelizeMergeMulti(places, thread_pool, is_cancelled, arena);
        }
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
        detail::Adder<T, ColumnTuple, Data>::add(this->data(place), columns, num_args, row_num);
    }

    ALWAYS_INLINE void addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *, ssize_t if_argument_pos)
        const override
    {
        const char8_t * flags = nullptr;
        if (if_argument_pos >= 0)
            flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();

        detail::Adder<T, ColumnTuple, Data>::addMany(this->data(place), columns, num_args, row_begin, row_end, flags, nullptr /* null_map */);
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

        detail::Adder<T, ColumnTuple, Data>::addMany(this->data(place), columns, num_args, row_begin, row_end, flags, null_map);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).set.merge(this->data(rhs).set);
    }

    bool isAbleToParallelizeMerge() const override { return is_able_to_parallelize_merge; }
    bool canOptimizeEqualKeysRanges() const override { return !is_able_to_parallelize_merge; }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena *) const override
    {
        if constexpr (is_able_to_parallelize_merge)
            this->data(place).set.merge(this->data(rhs).set, &thread_pool, &is_cancelled);
        else
            this->data(place).set.merge(this->data(rhs).set);
    }

    void parallelizeMergeMulti(AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena * arena) const override
    {
        if constexpr (is_able_to_parallelize_merge)
        {
            using DataSet = typename Data::Set;
            DataSet::parallelizeMergeMulti(places, [this](AggregateDataPtr p) { return &this->data(p).set; }, thread_pool, is_cancelled);
        }
        else
        {
            IAggregateFunction::parallelizeMergeMulti(places, thread_pool, is_cancelled, arena);
        }
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
