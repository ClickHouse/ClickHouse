#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnNullable.h>
#include "Common/HashTable/FixedHashSet.h"
#include "Common/HashTable/HashMap.h"
#include "Common/HashTable/HashTable.h"
#include "Common/HashTable/StringHashMap.h"
#include "Common/HashTable/TwoLevelStringHashMap.h"
#include <Common/assert_cast.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include "Interpreters/AggregatedData.h"
#include <Interpreters/AggregationCommon.h>
#include <Analyzer/SortNode.h>
#include <algorithm>
#include <queue>
#include <type_traits>
#include <typeinfo>
#include <iostream>
#include <sstream>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace ColumnsHashing
{

struct HashMethodContextSettings
{
    size_t max_threads;
};

/// Generic context for HashMethod. Context is shared between multiple threads, all methods must be thread-safe.
/// Is used for caching.
class HashMethodContext
{
public:
    virtual ~HashMethodContext() = default;

    using Settings = HashMethodContextSettings;
};

using HashMethodContextPtr = std::shared_ptr<HashMethodContext>;

struct LastElementCacheStats
{
    UInt64 hits = 0;
    UInt64 misses = 0;

    void update(size_t num_tries, size_t num_misses)
    {
        hits += num_tries - num_misses;
        misses += num_misses;
    }
};

struct OptimizationDataOneExpression
{
    UInt64 index_of_expression_in_group_by;
    SortDirection sort_direction;
    bool is_type_signed_integer;
};

namespace columns_hashing_impl
{

struct LastElementCacheBase
{
    bool empty = true;
    bool found = false;
    UInt64 misses = 0;

    void onNewValue(bool is_found)
    {
        empty = false;
        found = is_found;
        ++misses;
    }

    bool hasOnlyOneValue() const { return found && misses == 1; }
};

template <typename Value, bool nullable> struct LastElementCache;

template <typename Value>
struct LastElementCache<Value, true> : public LastElementCacheBase
{
    Value value{};
    bool is_null = false;

    template <typename Key>
    bool check(const Key & key) const { return !is_null && value.first == key; }

    bool check(const Value & rhs) const { return !is_null && value == rhs; }
};

template <typename Value>
struct LastElementCache<Value, false> : public LastElementCacheBase
{
    Value value{};

    template <typename Key>
    bool check(const Key & key) const { return value.first == key; }

    bool check(const Value & rhs) const { return value == rhs; }
};

template <typename Mapped>
class EmplaceResultImpl
{
    Mapped & value;
    Mapped & cached_value;
    bool inserted;

public:
    EmplaceResultImpl(Mapped & value_, Mapped & cached_value_, bool inserted_)
            : value(value_), cached_value(cached_value_), inserted(inserted_) {}

    bool isInserted() const { return inserted; }
    auto & getMapped() const { return value; }

    void setMapped(const Mapped & mapped)
    {
        cached_value = mapped;
        value = mapped;
    }
};

template <>
class EmplaceResultImpl<void>
{
    bool inserted;

public:
    explicit EmplaceResultImpl(bool inserted_) : inserted(inserted_) {}
    bool isInserted() const { return inserted; }
};

/// FindResult optionally may contain pointer to value and offset in hashtable buffer.
/// Only bool found is required.
/// So we will have 4 different specializations for FindResultImpl
class FindResultImplBase
{
    bool found;

public:
    explicit FindResultImplBase(bool found_) : found(found_) {}
    bool isFound() const { return found; }
};

template <bool need_offset = false>
class FindResultImplOffsetBase
{
public:
    constexpr static bool has_offset = need_offset;
    explicit FindResultImplOffsetBase(size_t /* off */) {}
};

template <>
class FindResultImplOffsetBase<true>
{
    size_t offset;
public:
    constexpr static bool has_offset = true;

    explicit FindResultImplOffsetBase(size_t off) : offset(off) {}
    ALWAYS_INLINE size_t getOffset() const { return offset; }
};

template <typename Mapped, bool need_offset = false>
class FindResultImpl : public FindResultImplBase, public FindResultImplOffsetBase<need_offset>
{
    Mapped * value;

public:
    FindResultImpl()
        : FindResultImplBase(false), FindResultImplOffsetBase<need_offset>(0) // NOLINT(clang-analyzer-optin.cplusplus.UninitializedObject)  intentionally allow uninitialized value here
    {}

    FindResultImpl(Mapped * value_, bool found_, size_t off)
        : FindResultImplBase(found_), FindResultImplOffsetBase<need_offset>(off), value(value_) {}
    Mapped & getMapped() const { return *value; }  /// NOLINT(clang-analyzer-core.uninitialized.UndefReturn)
};

template <bool need_offset>
class FindResultImpl<void, need_offset> : public FindResultImplBase, public FindResultImplOffsetBase<need_offset>
{
public:
    FindResultImpl(bool found_, size_t off) : FindResultImplBase(found_), FindResultImplOffsetBase<need_offset>(off) {}
};

template <typename T, typename = void>
struct HasBegin : std::false_type {};

template <typename T>
struct HasBegin<T, std::void_t<decltype(std::declval<const T&>().begin())>> : std::true_type {};

template <typename T, typename = void>
struct HasForEachMapped : std::false_type {};

template <typename T>
struct HasForEachMapped<T, std::void_t<decltype(std::declval<const T&>().forEachMapped())>> : std::true_type {};

template <typename T, typename = void>
struct HasKey : std::false_type {};

template <typename T>
struct HasKey<T, std::void_t<decltype(std::declval<const T&>().key)>> : std::true_type {};

template <typename T, typename ArgType, typename = void>
struct HasErase : std::false_type {};

template <typename T, typename ArgType>
struct HasErase<T, ArgType, std::void_t<decltype(std::declval<T>().erase(std::declval<ArgType>()))>> : std::true_type {};

template <typename T, typename = void>
struct HasCompareWithInt64t : std::false_type {};

template <typename T>
struct HasCompareWithInt64t<T, std::void_t<decltype(std::declval<const T&>() == std::declval<int64_t>())>> : std::true_type {};

template <typename T, typename = void>
struct HasCout : std::false_type {};

template <typename T>
struct HasCout<T, std::void_t<decltype(std::declval<std::ostream&>() << std::declval<T>())>> : std::true_type {};

template <typename T>
struct MakeSignedType
{
    using type = typename std::conditional<
        std::is_unsigned_v<T> && std::is_integral_v<T>,
        std::make_signed<T>,
        std::type_identity<T>
    >::type::type;
};

template <typename KeyHolder1, typename KeyHolder2>
static bool compareKeyHolders(const KeyHolder1 & lhs, const KeyHolder2 & rhs, const std::vector<OptimizationDataOneExpression> & optimization_indexes)
{
    const auto & lhs_key = keyHolderGetKey(lhs);
    const auto & rhs_key = keyHolderGetKey(rhs);

    assert(optimization_indexes.size() == 1); // TODO remove after supporting several expressions in findOptimizationSublistIndexes

    if (optimization_indexes[0].is_type_signed_integer)
    {
        auto lhs_key_signed = static_cast<typename MakeSignedType<KeyHolder1>::type>(lhs_key);
        auto rhs_key_signed = static_cast<typename MakeSignedType<KeyHolder2>::type>(rhs_key);
        if (optimization_indexes[0].sort_direction == SortDirection::ASCENDING)
            return lhs_key_signed < rhs_key_signed;
        return rhs_key_signed < lhs_key_signed;
    }

    if (optimization_indexes[0].sort_direction == SortDirection::ASCENDING)
        return lhs_key < rhs_key;
    return rhs_key < lhs_key;
}

template <typename Derived, typename Value, typename Mapped, bool consecutive_keys_optimization, bool need_offset = false, bool nullable = false>
class HashMethodBase
{
public:
    using EmplaceResult = EmplaceResultImpl<Mapped>;
    using FindResult = FindResultImpl<Mapped, need_offset>;
    static constexpr bool has_mapped = !std::is_same_v<Mapped, void>;
    using Cache = LastElementCache<Value, nullable>;

    static HashMethodContextPtr createContext(const HashMethodContextSettings &) { return nullptr; }

    template <typename Data>
    ALWAYS_INLINE EmplaceResult emplaceKey(Data & data, size_t row, Arena & pool)
    {
        if constexpr (nullable)
        {
            if (isNullAt(row))
            {
                if constexpr (consecutive_keys_optimization)
                {
                    if (!cache.is_null)
                    {
                        cache.onNewValue(true);
                        cache.is_null = true;
                    }
                }

                bool has_null_key = data.hasNullKeyData();
                data.hasNullKeyData() = true;

                if constexpr (has_mapped)
                    return EmplaceResult(data.getNullKeyData(), data.getNullKeyData(), !has_null_key);
                else
                    return EmplaceResult(!has_null_key);
            }
        }

        auto key_holder = static_cast<Derived &>(*this).getKeyHolder(row, pool);
        return emplaceImpl(key_holder, data);
    }

    template <typename Data, typename... Args>
    ALWAYS_INLINE std::pair<std::optional<EmplaceResult>, std::vector<int64_t>> emplaceKeyOptimization(
        Data & data,
        size_t row,
        Arena & pool,
        const std::vector<OptimizationDataOneExpression> & optimization_indexes,
        size_t limit_plus_offset_length,
        size_t max_allowable_fill)
    {
        if constexpr (nullable)
        {
            if (isNullAt(row))
            {
                if constexpr (consecutive_keys_optimization)
                {
                    if (!cache.is_null)
                    {
                        cache.onNewValue(true);
                        cache.is_null = true;
                    }
                }

                bool has_null_key = data.hasNullKeyData();
                data.hasNullKeyData() = true;

                if constexpr (has_mapped)
                    return {EmplaceResult(data.getNullKeyData(), data.getNullKeyData(), !has_null_key), {}};
                else
                    return {EmplaceResult(!has_null_key), {}};
            }
        }

        auto key_holder = static_cast<Derived &>(*this).getKeyHolder(row, pool);
        return emplaceImplOptimization(key_holder, data, limit_plus_offset_length, optimization_indexes, max_allowable_fill);
    }

    template <typename Data, typename Compare>
    ALWAYS_INLINE std::pair<std::optional<EmplaceResult>, std::vector<int64_t>> emplaceKeyOptimizationWithHeap(
        Data & data,
        size_t row,
        Arena & pool,
        const std::vector<OptimizationDataOneExpression> & optimization_indexes,
        size_t limit_plus_offset_length,
        size_t max_allowable_fill,
        std::vector<typename Data::iterator>& top_keys_heap,
        Compare heap_cmp)
    {
        if constexpr (nullable)
        {
            if (isNullAt(row))
            {
                if constexpr (consecutive_keys_optimization)
                {
                    if (!cache.is_null)
                    {
                        cache.onNewValue(true);
                        cache.is_null = true;
                    }
                }

                bool has_null_key = data.hasNullKeyData();
                data.hasNullKeyData() = true;

                if constexpr (has_mapped)
                    return {EmplaceResult(data.getNullKeyData(), data.getNullKeyData(), !has_null_key), {}};
                else
                    return {EmplaceResult(!has_null_key), {}};
            }
        }

        auto key_holder = static_cast<Derived &>(*this).getKeyHolder(row, pool);
        return emplaceImplOptimizationWithHeap(key_holder, data, limit_plus_offset_length, optimization_indexes, max_allowable_fill, top_keys_heap, heap_cmp);
    }

    template <typename Data>
    ALWAYS_INLINE FindResult findKey(Data & data, size_t row, Arena & pool)
    {
        if constexpr (nullable)
        {
            if (isNullAt(row))
            {
                bool has_null_key = data.hasNullKeyData();

                if constexpr (consecutive_keys_optimization)
                {
                    if (!cache.is_null)
                    {
                        cache.onNewValue(has_null_key);
                        cache.is_null = true;
                    }
                }

                if constexpr (has_mapped)
                    return FindResult(&data.getNullKeyData(), has_null_key, 0);
                else
                    return FindResult(has_null_key, 0);
            }
        }

        auto key_holder = static_cast<Derived &>(*this).getKeyHolder(row, pool);
        return findKeyImpl(keyHolderGetKey(key_holder), data);
    }

    template <typename Data>
    ALWAYS_INLINE size_t getHash(const Data & data, size_t row, Arena & pool)
    {
        auto key_holder = static_cast<Derived &>(*this).getKeyHolder(row, pool);
        return data.hash(keyHolderGetKey(key_holder));
    }

    ALWAYS_INLINE void resetCache()
    {
        if constexpr (consecutive_keys_optimization)
        {
            cache.empty = true;
            cache.found = false;
            cache.misses = 0;
        }
    }

    ALWAYS_INLINE bool hasOnlyOneValueSinceLastReset() const
    {
        if constexpr (consecutive_keys_optimization)
            return cache.hasOnlyOneValue();
        return false;
    }

    ALWAYS_INLINE UInt64 getCacheMissesSinceLastReset() const
    {
        if constexpr (consecutive_keys_optimization)
            return cache.misses;
        return 0;
    }

    ALWAYS_INLINE bool isNullAt(size_t row) const
    {
        if constexpr (nullable)
        {
            return null_map->getBool(row);
        }
        else
        {
            return false;
        }
    }

protected:
    Cache cache;
    const IColumn * null_map = nullptr;
    bool has_null_data = false;

    /// column argument only for nullable column
    explicit HashMethodBase(const IColumn * column = nullptr)
    {
        if constexpr (consecutive_keys_optimization)
        {
            if constexpr (has_mapped)
            {
                /// Init PairNoInit elements.
                cache.value.second = Mapped();
                cache.value.first = {};
            }
            else
                cache.value = Value();
        }

        if constexpr (nullable)
            null_map = &checkAndGetColumn<ColumnNullable>(*column).getNullMapColumn();
    }

    template <typename Data, typename KeyHolder>
    ALWAYS_INLINE EmplaceResult emplaceImpl(KeyHolder & key_holder, Data & data)
    {
        if constexpr (consecutive_keys_optimization)
        {
            if (cache.found && cache.check(keyHolderGetKey(key_holder)))
            {
                if constexpr (has_mapped)
                    return EmplaceResult(cache.value.second, cache.value.second, false);
                else
                    return EmplaceResult(false);
            }
        }

        typename Data::LookupResult it;
        bool inserted = false;
        data.emplace(key_holder, it, inserted);

        [[maybe_unused]] Mapped * cached = nullptr;
        if constexpr (has_mapped)
            cached = &it->getMapped();

        if constexpr (has_mapped)
        {
            if (inserted)
            {
                new (&it->getMapped()) Mapped();
            }
        }

        if constexpr (consecutive_keys_optimization)
        {
            cache.onNewValue(true);

            if constexpr (nullable)
                cache.is_null = false;

            if constexpr (has_mapped)
            {
                cache.value.first = it->getKey();
                cache.value.second = it->getMapped();
                cached = &cache.value.second;
            }
            else
            {
                cache.value = it->getKey();
            }
        }

        if constexpr (has_mapped)
            return EmplaceResult(it->getMapped(), *cached, inserted);
        else
            return EmplaceResult(inserted);
    }

    template <typename Data, typename KeyHolder, typename Compare>
    ALWAYS_INLINE std::pair<std::optional<EmplaceResult>, std::vector<int64_t>> emplaceImplOptimizationWithHeap(
        KeyHolder & key_holder,
        Data & data,
        size_t limit_plus_offset_length,
        const std::vector<OptimizationDataOneExpression> & optimization_indexes,
        size_t max_allowable_fill,
        std::vector<typename Data::iterator>& top_keys_heap,
        Compare heap_cmp)
    {
        std::vector<int64_t> catched;
        // if constexpr (HasCout<KeyHolder>::value) {
        //     std::stringstream ss2; ss2 << "qqqq: " << key_holder << std::endl; std::cout << ss2.str() << std::endl;
        // }
        std::stringstream ss3; ss3 << "typeid(key_holder).name(): " << typeid(key_holder).name() << std::endl; std::cout << ss3.str() << std::endl;
        if constexpr (HasCompareWithInt64t<KeyHolder>::value)
        {
            if constexpr (HasCout<KeyHolder>::value) {
                std::stringstream ss2; ss2 << "key_holder: " << key_holder << std::endl; std::cout << ss2.str() << std::endl;
            }
            if (key_holder == 4611789366909569002)
                catched.push_back(4611789366909569002);
            if (key_holder == 4611834982401952180)
                catched.push_back(4611834982401952180);
            if (key_holder == 4611856466334090543)
                catched.push_back(4611856466334090543);
        }
        chassert(limit_plus_offset_length > 0);
        if constexpr (consecutive_keys_optimization)
        {
            if (cache.found && cache.check(keyHolderGetKey(key_holder)))
            {
                if constexpr (has_mapped)
                    return {EmplaceResult(cache.value.second, cache.value.second, false), catched};
                else
                    return {EmplaceResult(false), catched};
            }
        }

        if constexpr (!std::is_same_v<decltype(key_holder), const VoidKey>) { // TODO VoidKey doesn't work in compareKeyHolders
            if constexpr (!std::is_same_v<KeyHolder, DB::SerializedKeyHolder>
                       && !std::is_same_v<KeyHolder, DB::ArenaKeyHolder>
                       && !std::is_same_v<KeyHolder, Int128>
                       && !std::is_same_v<KeyHolder, UInt128>
                       && !std::is_same_v<KeyHolder, Int256>
                       && !std::is_same_v<KeyHolder, UInt256>) { // TODO support all types
                if constexpr (HasBegin<Data>::value)
                {
                    if constexpr (!std::is_same_v<decltype(data.begin()->getKey()), const VoidKey>)
                    {
                        // First check if key_holder more than top of top_keys_heap. If more, it could be skipped.
                        if (top_keys_heap.size() >= limit_plus_offset_length && // TODO ==
                            compareKeyHolders(top_keys_heap.front()->getKey(), key_holder, optimization_indexes))
                        {
                            return {std::nullopt, catched};
                        }
                    }
                } else
                {
                    // TODO
                }
            }
        }

        typename Data::LookupResult it;
        bool inserted = false;

        if constexpr (!std::is_same_v<decltype(key_holder), const VoidKey>) { // TODO VoidKey doesn't work in compareKeyHolders
            if constexpr (!std::is_same_v<KeyHolder, DB::SerializedKeyHolder>
                       && !std::is_same_v<KeyHolder, DB::ArenaKeyHolder>
                       && !std::is_same_v<KeyHolder, Int128>
                       && !std::is_same_v<KeyHolder, UInt128>
                       && !std::is_same_v<KeyHolder, Int256>
                       && !std::is_same_v<KeyHolder, UInt256>) { // TODO support all types
                if (data.size() >= max_allowable_fill) // TODO ==
                {
                    assert(optimization_indexes.size() == 1 && optimization_indexes[0].index_of_expression_in_group_by == 0); // TODO support arbitrary number of expressions in findOptimizationSublistIndexes
                    if constexpr (HasBegin<Data>::value)
                    {
                        if constexpr (!std::is_same_v<decltype(data.begin()->getKey()), const VoidKey>)
                        {
                            // TODO Remove after support more types
                            assert(static_cast<bool>(!std::is_same_v<decltype(data.begin()->getKey()), StringRef>));
                            assert(static_cast<bool>(!std::is_same_v<decltype(data.begin()->getKey()), Int128>));
                            assert(static_cast<bool>(!std::is_same_v<decltype(data.begin()->getKey()), UInt128>));
                            assert(static_cast<bool>(!std::is_same_v<decltype(data.begin()->getKey()), Int256>));
                            assert(static_cast<bool>(!std::is_same_v<decltype(data.begin()->getKey()), UInt256>));

                            // Create new HT with only top elements and replace data with it
                            Data new_data(max_allowable_fill);
                            for (auto& old_it : top_keys_heap)
                            {
                                new_data.emplace(old_it->getKey(), it, inserted);
                                it->getMapped() = std::move(old_it->getMapped());
                            }
                            data = std::move(new_data);

                            // Renew top_keys_heap from data iterators
                            size_t i = 0;
                            top_keys_heap.resize(data.size()); // TODO remove after check
                            for (auto iterator = data.begin(); iterator != data.end(); ++iterator)
                            {
                                top_keys_heap[i] = iterator;
                                ++i;
                            }
                            std::make_heap(top_keys_heap.begin(), top_keys_heap.end(), heap_cmp);
                        }
                    } else if constexpr (HasForEachMapped<Data>::value)
                    {
                        // TODO implement
                        // data.forEachMapped([](auto el) {
                        // });
                    }
                }
            }
        }

        inserted = false;
        data.emplace(key_holder, it, inserted);

        // If inserted, we have to update top_keys_heap. If is filled, pop the largest element.
        if constexpr (!std::is_same_v<decltype(key_holder), const VoidKey>) { // TODO VoidKey doesn't work in compareKeyHolders
            if constexpr (!std::is_same_v<KeyHolder, DB::SerializedKeyHolder>
                       && !std::is_same_v<KeyHolder, DB::ArenaKeyHolder>
                       && !std::is_same_v<KeyHolder, Int128>
                       && !std::is_same_v<KeyHolder, UInt128>
                       && !std::is_same_v<KeyHolder, Int256>
                       && !std::is_same_v<KeyHolder, UInt256>) { // TODO support all types
                if constexpr (HasBegin<Data>::value)
                {
                    if constexpr (!std::is_same_v<decltype(data.begin()->getKey()), const VoidKey>)
                    {
                        if (inserted)
                        {
                            if (top_keys_heap.size() == limit_plus_offset_length)
                            {
                                std::pop_heap(top_keys_heap.begin(), top_keys_heap.end(), heap_cmp);
                                top_keys_heap.pop_back();
                            }
                            using Iterator = typename Data::iterator;
                            top_keys_heap.push_back(Iterator(&data, it));
                            std::push_heap(top_keys_heap.begin(), top_keys_heap.end(), heap_cmp);
                        }
                    }
                } else
                {
                    // TODO
                }
            }
        }

        [[maybe_unused]] Mapped * cached = nullptr;
        if constexpr (has_mapped)
            cached = &it->getMapped();

        if constexpr (has_mapped)
        {
            if (inserted)
            {
                new (&it->getMapped()) Mapped();
            }
        }

        if constexpr (consecutive_keys_optimization)
        {
            cache.onNewValue(true);

            if constexpr (nullable)
                cache.is_null = false;

            if constexpr (has_mapped)
            {
                cache.value.first = it->getKey();
                cache.value.second = it->getMapped();
                cached = &cache.value.second;
            }
            else
            {
                cache.value = it->getKey();
            }
        }

        if constexpr (has_mapped)
            return {EmplaceResult(it->getMapped(), *cached, inserted), catched};
        else
            return {EmplaceResult(inserted), catched};
    }

    template <typename Data, typename KeyHolder>
    ALWAYS_INLINE std::pair<std::optional<EmplaceResult>, std::vector<int64_t>> emplaceImplOptimization(
        KeyHolder & key_holder,
        Data & data,
        size_t limit_plus_offset_length,
        const std::vector<OptimizationDataOneExpression> & optimization_indexes,
        size_t max_allowable_fill)
    {
        std::vector<int64_t> catched;
        if constexpr (HasCompareWithInt64t<KeyHolder>::value)
        {
            // if constexpr (HasCout<KeyHolder>::value) {
            //     std::stringstream ss2; ss2 << "key_holder: " << key_holder << std::endl; std::cout << ss2.str() << std::endl;
            // }
            if (key_holder == 4611789366909569002)
                catched.push_back(4611789366909569002);
            if (key_holder == 4611834982401952180)
                catched.push_back(4611834982401952180);
            if (key_holder == 4611856466334090543)
                catched.push_back(4611856466334090543);
        }
        chassert(limit_plus_offset_length > 0);
        if constexpr (consecutive_keys_optimization)
        {
            if (cache.found && cache.check(keyHolderGetKey(key_holder)))
            {
                if constexpr (has_mapped)
                    return {EmplaceResult(cache.value.second, cache.value.second, false), catched};
                else
                    return {EmplaceResult(false), catched};
            }
        }

        if constexpr (!std::is_same_v<decltype(key_holder), const VoidKey>) { // TODO VoidKey doesn't work in compareKeyHolders
            if constexpr (!std::is_same_v<KeyHolder, DB::SerializedKeyHolder>
                       && !std::is_same_v<KeyHolder, DB::ArenaKeyHolder>
                       && !std::is_same_v<KeyHolder, Int128>
                       && !std::is_same_v<KeyHolder, UInt128>
                       && !std::is_same_v<KeyHolder, Int256>
                       && !std::is_same_v<KeyHolder, UInt256>) { // TODO support all types
                if (data.size() >= max_allowable_fill) // TODO ==
                {
                    assert(optimization_indexes.size() == 1 && optimization_indexes[0].index_of_expression_in_group_by == 0); // TODO support arbitrary number of expressions in findOptimizationSublistIndexes
                    if constexpr (HasBegin<Data>::value)
                    {
                        if constexpr (!std::is_same_v<decltype(data.begin()->getKey()), const VoidKey> && HasErase<Data, decltype(keyHolderGetKey(key_holder))>::value)
                        {
                            // TODO Remove after support more types
                            assert(static_cast<bool>(!std::is_same_v<decltype(data.begin()->getKey()), StringRef>));
                            assert(static_cast<bool>(!std::is_same_v<decltype(data.begin()->getKey()), Int128>));
                            assert(static_cast<bool>(!std::is_same_v<decltype(data.begin()->getKey()), UInt128>));
                            assert(static_cast<bool>(!std::is_same_v<decltype(data.begin()->getKey()), Int256>));
                            assert(static_cast<bool>(!std::is_same_v<decltype(data.begin()->getKey()), UInt256>));

                            // apply n-th element to data
                            std::vector<typename Data::iterator> data_iterators;
                            data_iterators.reserve(data.size());
                            for (auto iter = data.begin(); iter != data.end(); ++iter)
                                data_iterators.push_back(iter);
                            std::nth_element(data_iterators.begin(), data_iterators.begin() + (limit_plus_offset_length - 1), data_iterators.end(), [&optimization_indexes](const typename Data::iterator& lhs, const typename Data::iterator& rhs)
                            {
                                return compareKeyHolders(lhs->getKey(), rhs->getKey(), optimization_indexes);
                            });

                            // erase excess elements
                            /* Here I first copy elements to erase into separate vector, and then erase them by value.
                            It's slower than to erase then by iterators. Previously I tried applying nth_element
                            to a vector of iterators and then erase, but it seems that they were invalidating after erase.
                            TODO Maybe there is a way not to invalidate them?
                            */
                            std::vector<std::remove_const_t<std::remove_reference_t<decltype(data.begin()->getKey())>>> elements_to_erase;
                            elements_to_erase.reserve(data_iterators.size() - limit_plus_offset_length);
                            for (size_t i = limit_plus_offset_length; i < data_iterators.size(); ++i)
                                elements_to_erase.push_back(data_iterators[i]->getKey());
                            for (const auto element : elements_to_erase)
                                data.erase(element);
                        }
                    } else if constexpr (HasForEachMapped<Data>::value)
                    {
                        // TODO implement
                        // data.forEachMapped([](auto el) {
                        // });
                    }
                }
            }
        }

        typename Data::LookupResult it;
        bool inserted = false;
        data.emplace(key_holder, it, inserted);

        [[maybe_unused]] Mapped * cached = nullptr;
        if constexpr (has_mapped)
            cached = &it->getMapped();

        if constexpr (has_mapped)
        {
            if (inserted)
            {
                new (&it->getMapped()) Mapped();
            }
        }

        if constexpr (consecutive_keys_optimization)
        {
            cache.onNewValue(true);

            if constexpr (nullable)
                cache.is_null = false;

            if constexpr (has_mapped)
            {
                cache.value.first = it->getKey();
                cache.value.second = it->getMapped();
                cached = &cache.value.second;
            }
            else
            {
                cache.value = it->getKey();
            }
        }

        if constexpr (has_mapped)
            return {EmplaceResult(it->getMapped(), *cached, inserted), catched};
        else
            return {EmplaceResult(inserted), catched};
    }

    template <typename Data, typename Key>
    ALWAYS_INLINE FindResult findKeyImpl(Key key, Data & data)
    {
        if constexpr (consecutive_keys_optimization)
        {
            /// It's possible to support such combination, but code will became more complex.
            /// Now there's not place where we need this options enabled together
            static_assert(!FindResult::has_offset, "`consecutive_keys_optimization` and `has_offset` are conflicting options");
            if (likely(!cache.empty) && cache.check(key))
            {
                if constexpr (has_mapped)
                    return FindResult(&cache.value.second, cache.found, 0);
                else
                    return FindResult(cache.found, 0);
            }
        }

        auto it = data.find(key);

        if constexpr (consecutive_keys_optimization)
        {
            cache.onNewValue(it != nullptr);

            if constexpr (nullable)
                cache.is_null = false;

            if constexpr (has_mapped)
            {
                cache.value.first = key;
                if (it)
                    cache.value.second = it->getMapped();
            }
            else
            {
                cache.value = key;
            }
        }

        size_t offset = 0;
        if constexpr (FindResult::has_offset)
            offset = it ? data.offsetInternal(it) : 0;

        if constexpr (has_mapped)
            return FindResult(it ? &it->getMapped() : nullptr, it != nullptr, offset);
        else
            return FindResult(it != nullptr, offset);
    }
};

template <typename T>
struct MappedCache : public PaddedPODArray<T> {};

template <>
struct MappedCache<void> {};


/// This class is designed to provide the functionality that is required for
/// supporting nullable keys in HashMethodKeysFixed. If there are
/// no nullable keys, this class is merely implemented as an empty shell.
template <typename Key, bool has_nullable_keys>
class BaseStateKeysFixed;

/// Case where nullable keys are supported.
template <typename Key>
class BaseStateKeysFixed<Key, true>
{
protected:
    explicit BaseStateKeysFixed(const ColumnRawPtrs & key_columns)
    {
        null_maps.reserve(key_columns.size());
        actual_columns.reserve(key_columns.size());

        for (const auto & col : key_columns)
        {
            if (const auto * nullable_col = checkAndGetColumn<ColumnNullable>(col))
            {
                actual_columns.push_back(&nullable_col->getNestedColumn());
                null_maps.push_back(&nullable_col->getNullMapColumn());
            }
            else
            {
                actual_columns.push_back(col);
                null_maps.push_back(nullptr);
            }
        }
    }

    /// Return the columns which actually contain the values of the keys.
    /// For a given key column, if it is nullable, we return its nested
    /// column. Otherwise we return the key column itself.
    const ColumnRawPtrs & getActualColumns() const
    {
        return actual_columns;
    }

    /// Create a bitmap that indicates whether, for a particular row,
    /// a key column bears a null value or not.
    KeysNullMap<Key> createBitmap(size_t row) const
    {
        KeysNullMap<Key> bitmap{};

        for (size_t k = 0; k < null_maps.size(); ++k)
        {
            if (null_maps[k] != nullptr)
            {
                const auto & null_map = assert_cast<const ColumnUInt8 &>(*null_maps[k]).getData();
                if (null_map[row] == 1)
                {
                    size_t bucket = k / 8;
                    size_t offset = k % 8;
                    bitmap[bucket] |= UInt8(1) << offset;
                }
            }
        }

        return bitmap;
    }

private:
    ColumnRawPtrs actual_columns;
    ColumnRawPtrs null_maps;
};

/// Case where nullable keys are not supported.
template <typename Key>
class BaseStateKeysFixed<Key, false>
{
protected:
    explicit BaseStateKeysFixed(const ColumnRawPtrs & columns) : actual_columns(columns) {}

    const ColumnRawPtrs & getActualColumns() const { return actual_columns; }

    KeysNullMap<Key> createBitmap(size_t) const
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Internal error: calling createBitmap() for non-nullable keys is forbidden");
    }

private:
    ColumnRawPtrs actual_columns;
};

}

}

}
