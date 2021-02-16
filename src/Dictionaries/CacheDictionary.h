#pragma once

#include <atomic>
#include <chrono>
#include <cmath>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include <vector>

#include <pcg_random.hpp>

#include <common/logger_useful.h>

#include <Common/randomSeed.h>
#include <Common/ThreadPool.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/CurrentMetrics.h>

#include <Dictionaries/IDictionary.h>
#include <Dictionaries/ICacheDictionaryStorage.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryHelpers.h>
#include <Dictionaries/CacheDictionaryUpdateQueue.h>

namespace DB
{

namespace ErrorCodes
{
}

/*
 *
 * This dictionary is stored in a cache that has a fixed number of cells.
 * These cells contain frequently used elements.
 * When searching for a dictionary, the cache is searched first and special heuristic is used:
 * while looking for the key, we take a look only at max_collision_length elements.
 * So, our cache is not perfect. It has errors like "the key is in cache, but the cache says that it does not".
 * And in this case we simply ask external source for the key which is faster.
 * You have to keep this logic in mind.
 * */
template <DictionaryKeyType dictionary_key_type>
class CacheDictionary final : public IDictionary
{
public:
    using KeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::simple, UInt64, StringRef>;
    static_assert(dictionary_key_type != DictionaryKeyType::range, "Range key type is not supported by cache dictionary");

    CacheDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        CacheDictionaryStoragePtr cache_storage_ptr_,
        CacheDictionaryUpdateQueueConfiguration update_queue_configuration_,
        DictionaryLifetime dict_lifetime_,
        bool allow_read_expired_keys_);

    ~CacheDictionary() override;

    std::string getTypeName() const override { return "Cache"; }

    size_t getElementCount() const override;

    size_t getBytesAllocated() const override;

    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    double getHitRate() const override
    {
        return static_cast<double>(hit_count.load(std::memory_order_acquire)) / query_count.load(std::memory_order_relaxed);
    }

    double getLoadFactor() const override { return static_cast<double>(getElementCount()) / size; }

    bool supportUpdates() const override { return false; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<CacheDictionary>(
                getDictionaryID(),
                dict_struct,
                getSourceAndUpdateIfNeeded()->clone(),
                cache_storage_ptr,
                update_queue.getConfiguration(),
                dict_lifetime,
                allow_read_expired_keys);
    }

    const IDictionarySource * getSource() const override;

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.getAttribute(attribute_name).injective;
    }

    DictionaryKeyType getKeyType() const override
    {
        return dictionary_key_type == DictionaryKeyType::simple ? DictionaryKeyType::simple : DictionaryKeyType::complex;
    }

    ColumnPtr getColumn(
        const std::string& attribute_name,
        const DataTypePtr & result_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        const ColumnPtr & default_values_column) const override;

    Columns getColumns(
        const Strings & attribute_names,
        const DataTypes & result_types,
        const Columns & key_columns,
        const DataTypes & key_types,
        const Columns & default_values_columns) const;

    ColumnUInt8::Ptr hasKeys(const Columns & key_columns, const DataTypes & key_types) const override;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

    std::exception_ptr getLastException() const override;

    bool hasHierarchy() const override { return dictionary_key_type == DictionaryKeyType::simple && hierarchical_attribute; }

    void toParent(const PaddedPODArray<UInt64> & ids, PaddedPODArray<UInt64> & out) const override;

    void isInVectorVector(
        const PaddedPODArray<UInt64> & child_ids,
        const PaddedPODArray<UInt64> & ancestor_ids,
        PaddedPODArray<UInt8> & out) const override;

    void isInVectorConstant(
        const PaddedPODArray<UInt64> & child_ids,
        const UInt64 ancestor_id, PaddedPODArray<UInt8> & out) const override;

    void isInConstantVector(
        const UInt64 child_id,
        const PaddedPODArray<UInt64> & ancestor_ids,
        PaddedPODArray<UInt8> & out) const override;

private:

    Columns getColumnsImpl(
        const Strings & attribute_names,
        const Columns & key_columns,
        const PaddedPODArray<KeyType> & keys,
        const Columns & default_values_columns) const;

    static MutableColumns aggregateColumns(
        const PaddedPODArray<KeyType> & keys,
        const DictionaryStorageFetchRequest & request,
        const MutableColumns & fetched_columns_from_storage,
        const HashMap<KeyType, size_t> & found_keys_to_fetched_columns_from_storage_index,
        const MutableColumns & fetched_columns_during_update,
        const HashMap<KeyType, size_t> & found_keys_to_fetched_columns_during_update_index,
        const std::vector<DefaultValueProvider> & default_value_providers);

    void setupHierarchicalAttribute();

    void update(CacheDictionaryUpdateUnitPtr<dictionary_key_type> & update_unit_ptr);

    using SharedDictionarySourcePtr = std::shared_ptr<IDictionarySource>;

    /// Update dictionary source pointer if required and return it. Thread safe.
    /// MultiVersion is not used here because it works with constant pointers.
    /// For some reason almost all methods in IDictionarySource interface are
    /// not constant.
    SharedDictionarySourcePtr getSourceAndUpdateIfNeeded() const
    {
        std::lock_guard lock(source_mutex);
        if (error_count)
        {
            /// Recover after error: we have to clone the source here because
            /// it could keep connections which should be reset after error.
            auto new_source_ptr = source_ptr->clone();
            source_ptr = std::move(new_source_ptr);
        }

        return source_ptr;
    }

    template <typename AncestorType>
    void isInImpl(const PaddedPODArray<Key> & child_ids, const AncestorType & ancestor_ids, PaddedPODArray<UInt8> & out) const;

    static Columns filterResultColumnsForRequest(MutableColumns & mutable_columns, const DictionaryStorageFetchRequest & request);

    const DictionaryStructure dict_struct;

    /// Dictionary source should be used with mutex
    mutable std::mutex source_mutex;
    mutable SharedDictionarySourcePtr source_ptr;

    CacheDictionaryStoragePtr cache_storage_ptr;
    mutable CacheDictionaryUpdateQueue<dictionary_key_type> update_queue;

    const DictionaryLifetime dict_lifetime;

    Poco::Logger * log;

    size_t size;

    const bool allow_read_expired_keys;

    mutable pcg64 rnd_engine;

    /// This lock is used for the inner cache state update function lock it for
    /// write, when it need to update cache state all other functions just
    /// readers. Surprisingly this lock is also used for last_exception pointer.
    mutable std::shared_mutex rw_lock;

    const DictionaryAttribute * hierarchical_attribute = nullptr;

    mutable std::exception_ptr last_exception;
    mutable std::atomic<size_t> error_count {0};
    mutable std::atomic<std::chrono::system_clock::time_point> backoff_end_time{std::chrono::system_clock::time_point{}};

    mutable std::atomic<size_t> hit_count{0};
    mutable std::atomic<size_t> query_count{0};

};

extern template class CacheDictionary<DictionaryKeyType::simple>;
extern template class CacheDictionary<DictionaryKeyType::complex>;

}
