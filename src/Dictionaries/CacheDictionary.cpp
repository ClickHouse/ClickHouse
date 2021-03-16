#include "CacheDictionary.h"

#include <memory>

#include <ext/range.h>
#include <ext/size.h>
#include <ext/map.h>
#include <ext/chrono_io.h>

#include <Core/Defines.h>
#include <Common/CurrentMetrics.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashSet.h>
#include <Common/ProfileEvents.h>
#include <Common/ProfilingScopedRWLock.h>
#include <Dictionaries/DictionaryBlockInputStream.h>

namespace ProfileEvents
{
extern const Event DictCacheKeysRequested;
extern const Event DictCacheKeysRequestedMiss;
extern const Event DictCacheKeysRequestedFound;
extern const Event DictCacheKeysExpired;
extern const Event DictCacheKeysNotFound;
extern const Event DictCacheKeysHit;
extern const Event DictCacheRequestTimeNs;
extern const Event DictCacheRequests;
extern const Event DictCacheLockWriteNs;
extern const Event DictCacheLockReadNs;
}

namespace CurrentMetrics
{
extern const Metric DictCacheRequests;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int CACHE_DICTIONARY_UPDATE_FAIL;
    extern const int TYPE_MISMATCH;
    extern const int UNSUPPORTED_METHOD;
}

template <DictionaryKeyType dictionary_key_type>
CacheDictionary<dictionary_key_type>::CacheDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    CacheDictionaryStoragePtr cache_storage_ptr_,
    CacheDictionaryUpdateQueueConfiguration update_queue_configuration_,
    DictionaryLifetime dict_lifetime_,
    bool allow_read_expired_keys_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr{std::move(source_ptr_)}
    , cache_storage_ptr(cache_storage_ptr_)
    , update_queue(
        dict_id_.getNameForLogs(),
        update_queue_configuration_,
        [this](CacheDictionaryUpdateUnitPtr<dictionary_key_type> unit_to_update)
        {
            update(unit_to_update);
        })
    , dict_lifetime(dict_lifetime_)
    , log(&Poco::Logger::get("ExternalDictionaries"))
    , allow_read_expired_keys(allow_read_expired_keys_)
    , rnd_engine(randomSeed())
{
    if (!source_ptr->supportsSelectiveLoad())
        throw Exception{full_name + ": source cannot be used with CacheDictionary", ErrorCodes::UNSUPPORTED_METHOD};

    setupHierarchicalAttribute();
}

template <DictionaryKeyType dictionary_key_type>
CacheDictionary<dictionary_key_type>::~CacheDictionary()
{
    update_queue.stopAndWait();
}

template <DictionaryKeyType dictionary_key_type>
size_t CacheDictionary<dictionary_key_type>::getElementCount() const
{
    const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};
    return cache_storage_ptr->getSize();
}

template <DictionaryKeyType dictionary_key_type>
size_t CacheDictionary<dictionary_key_type>::getBytesAllocated() const
{
    /// In case of existing string arena we check the size of it.
    /// But the same appears in setAttributeValue() function, which is called from update() function
    /// which in turn is called from another thread.
    const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};
    return cache_storage_ptr->getBytesAllocated();
}

template <DictionaryKeyType dictionary_key_type>
double CacheDictionary<dictionary_key_type>::getLoadFactor() const
{
    const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};
    return static_cast<double>(cache_storage_ptr->getSize()) / cache_storage_ptr->getMaxSize();
}

template <DictionaryKeyType dictionary_key_type>
std::exception_ptr CacheDictionary<dictionary_key_type>::getLastException() const
{
    const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};
    return last_exception;
}

template <DictionaryKeyType dictionary_key_type>
const IDictionarySource * CacheDictionary<dictionary_key_type>::getSource() const
{
    /// Mutex required here because of the getSourceAndUpdateIfNeeded() function
    /// which is used from another thread.
    std::lock_guard lock(source_mutex);
    return source_ptr.get();
}

template <DictionaryKeyType dictionary_key_type>
void CacheDictionary<dictionary_key_type>::toParent(const PaddedPODArray<UInt64> & ids [[maybe_unused]], PaddedPODArray<UInt64> & out [[maybe_unused]]) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::simple)
    {
        /// Run update on requested keys before fetch from storage
        const auto & attribute_name = hierarchical_attribute->name;

        auto result_type = std::make_shared<DataTypeUInt64>();
        auto input_column = result_type->createColumn();
        auto & input_column_typed = assert_cast<ColumnVector<UInt64> &>(*input_column);
        auto & data = input_column_typed.getData();
        data.insert(ids.begin(), ids.end());

        auto column = getColumn({attribute_name}, result_type, {std::move(input_column)}, {result_type}, {nullptr});
        const auto & result_column_typed = assert_cast<const ColumnVector<UInt64> &>(*column);
        const auto & result_data = result_column_typed.getData();

        out.assign(result_data);
    }
    else
        throw Exception("Hierarchy is not supported for complex key CacheDictionary", ErrorCodes::UNSUPPORTED_METHOD);
}


/// Allow to use single value in same way as array.
static inline UInt64 getAt(const PaddedPODArray<UInt64> & arr, const size_t idx)
{
    return arr[idx];
}
static inline UInt64 getAt(const UInt64 & value, const size_t)
{
    return value;
}

template <DictionaryKeyType dictionary_key_type>
template <typename AncestorType>
void CacheDictionary<dictionary_key_type>::isInImpl(const PaddedPODArray<Key> & child_ids, const AncestorType & ancestor_ids, PaddedPODArray<UInt8> & out) const
{
    /// Transform all children to parents until ancestor id or null_value will be reached.

    size_t out_size = out.size();
    memset(out.data(), 0xFF, out_size); /// 0xFF means "not calculated"

    const auto null_value = hierarchical_attribute->null_value.get<UInt64>();

    PaddedPODArray<Key> children(out_size, 0);
    PaddedPODArray<Key> parents(child_ids.begin(), child_ids.end());

    for (size_t i = 0; i < DBMS_HIERARCHICAL_DICTIONARY_MAX_DEPTH; ++i)
    {
        size_t out_idx = 0;
        size_t parents_idx = 0;
        size_t new_children_idx = 0;

        while (out_idx < out_size)
        {
            /// Already calculated
            if (out[out_idx] != 0xFF)
            {
                ++out_idx;
                continue;
            }

            /// No parent
            if (parents[parents_idx] == null_value)
            {
                out[out_idx] = 0;
            }
            /// Found ancestor
            else if (parents[parents_idx] == getAt(ancestor_ids, parents_idx))
            {
                out[out_idx] = 1;
            }
            /// Loop detected
            else if (children[new_children_idx] == parents[parents_idx])
            {
                out[out_idx] = 1;
            }
            /// Found intermediate parent, add this value to search at next loop iteration
            else
            {
                children[new_children_idx] = parents[parents_idx];
                ++new_children_idx;
            }

            ++out_idx;
            ++parents_idx;
        }

        if (new_children_idx == 0)
            break;

        /// Transform all children to its parents.
        children.resize(new_children_idx);
        parents.resize(new_children_idx);

        toParent(children, parents);
    }
}

template <DictionaryKeyType dictionary_key_type>
void CacheDictionary<dictionary_key_type>::isInVectorVector(
    const PaddedPODArray<UInt64> & child_ids, const PaddedPODArray<UInt64> & ancestor_ids, PaddedPODArray<UInt8> & out) const
{
    isInImpl(child_ids, ancestor_ids, out);
}

template <DictionaryKeyType dictionary_key_type>
void CacheDictionary<dictionary_key_type>::isInVectorConstant(const PaddedPODArray<UInt64> & child_ids, const UInt64 ancestor_id, PaddedPODArray<UInt8> & out) const
{
    isInImpl(child_ids, ancestor_id, out);
}

template <DictionaryKeyType dictionary_key_type>
void CacheDictionary<dictionary_key_type>::isInConstantVector(const UInt64 child_id, const PaddedPODArray<UInt64> & ancestor_ids, PaddedPODArray<UInt8> & out) const
{
    /// Special case with single child value.

    const auto null_value = hierarchical_attribute->null_value.get<UInt64>();

    PaddedPODArray<Key> child(1, child_id);
    PaddedPODArray<Key> parent(1);
    std::vector<Key> ancestors(1, child_id);

    /// Iteratively find all ancestors for child.
    for (size_t i = 0; i < DBMS_HIERARCHICAL_DICTIONARY_MAX_DEPTH; ++i)
    {
        toParent(child, parent);

        if (parent[0] == null_value)
            break;

        child[0] = parent[0];
        ancestors.push_back(parent[0]);
    }

    /// Assuming short hierarchy, so linear search is Ok.
    for (size_t i = 0, out_size = out.size(); i < out_size; ++i)
        out[i] = std::find(ancestors.begin(), ancestors.end(), ancestor_ids[i]) != ancestors.end();
}

template <DictionaryKeyType dictionary_key_type>
void CacheDictionary<dictionary_key_type>::setupHierarchicalAttribute()
{
    /// TODO: Move this to DictionaryStructure
    for (const auto & attribute : dict_struct.attributes)
    {
        if (attribute.hierarchical)
        {
            hierarchical_attribute = &attribute;

            if (attribute.underlying_type != AttributeUnderlyingType::utUInt64)
                throw Exception{full_name + ": hierarchical attribute must be UInt64.", ErrorCodes::TYPE_MISMATCH};
        }
    }
}

template <DictionaryKeyType dictionary_key_type>
ColumnPtr CacheDictionary<dictionary_key_type>::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & result_type,
    const Columns & key_columns,
    const DataTypes & key_types,
    const ColumnPtr & default_values_column) const
{
    return getColumns({attribute_name}, {result_type}, key_columns, key_types, {default_values_column}).front();
}

template <DictionaryKeyType dictionary_key_type>
Columns CacheDictionary<dictionary_key_type>::getColumns(
    const Strings & attribute_names,
    const DataTypes &,
    const Columns & key_columns,
    const DataTypes & key_types,
    const Columns & default_values_columns) const
{
    if (dictionary_key_type == DictionaryKeyType::complex)
        dict_struct.validateKeyTypes(key_types);

    Arena complex_keys_arena;
    DictionaryKeysExtractor<dictionary_key_type> extractor(key_columns, complex_keys_arena);
    auto & keys = extractor.getKeys();

    return getColumnsImpl(attribute_names, key_columns, keys, default_values_columns);
}

template <DictionaryKeyType dictionary_key_type>
Columns CacheDictionary<dictionary_key_type>::getColumnsImpl(
    const Strings & attribute_names,
    const Columns & key_columns,
    const PaddedPODArray<KeyType> & keys,
    const Columns & default_values_columns) const
{
    /**
    * Flow of getColumsImpl
    * 1. Get fetch result from storage
    * 2. If all keys are found in storage and not expired
    *   2.1. If storage returns fetched columns in order of keys then result is returned to client.
    *   2.2. If storage does not return fetched columns in order of keys then reorder
    *    result columns and return result to client.
    * 3. If all keys are found in storage but some of them are expired and we allow to read expired keys
    * start async request to source and perform actions from step 2 for result returned from storage.
    * 4. If some keys are found and some are not, start sync update from source.
    * 5. Aggregate columns returned from storage and source, if key is not found in storage and in source
    * use default value.
    */

    DictionaryStorageFetchRequest request(dict_struct, attribute_names, default_values_columns);

    FetchResult result_of_fetch_from_storage;

    {
        /// Write lock on storage
        const ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};

        result_of_fetch_from_storage = cache_storage_ptr->fetchColumnsForKeys(keys, request);
    }

    size_t found_keys_size = result_of_fetch_from_storage.found_keys_size;
    size_t expired_keys_size = result_of_fetch_from_storage.expired_keys_size;
    size_t not_found_keys_size = result_of_fetch_from_storage.not_found_keys_size;

    ProfileEvents::increment(ProfileEvents::DictCacheKeysHit, found_keys_size);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysExpired, expired_keys_size);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysNotFound, not_found_keys_size);

    query_count.fetch_add(keys.size());
    hit_count.fetch_add(found_keys_size);

    MutableColumns & fetched_columns_from_storage = result_of_fetch_from_storage.fetched_columns;
    const PaddedPODArray<KeyState> & key_index_to_state_from_storage = result_of_fetch_from_storage.key_index_to_state;

    bool source_returns_fetched_columns_in_order_of_keys = cache_storage_ptr->returnsFetchedColumnsInOrderOfRequestedKeys();

    if (not_found_keys_size == 0 && expired_keys_size == 0)
    {
        /// All keys were found in storage

        if (source_returns_fetched_columns_in_order_of_keys)
            return request.filterRequestedColumns(fetched_columns_from_storage);
        else
        {
            /// Reorder result from storage to requested keys indexes
            MutableColumns aggregated_columns = aggregateColumnsInOrderOfKeys(
                keys,
                request,
                fetched_columns_from_storage,
                key_index_to_state_from_storage);

            return request.filterRequestedColumns(aggregated_columns);
        }
    }

    size_t keys_to_update_size = not_found_keys_size + expired_keys_size;
    auto update_unit = std::make_shared<CacheDictionaryUpdateUnit<dictionary_key_type>>(key_columns, key_index_to_state_from_storage, request, keys_to_update_size);

    HashMap<KeyType, size_t> requested_keys_to_fetched_columns_during_update_index;
    MutableColumns fetched_columns_during_update = request.makeAttributesResultColumns();

    if (not_found_keys_size == 0 && expired_keys_size > 0 && allow_read_expired_keys)
    {
        /// Start async update only if allow read expired keys and all keys are found
        update_queue.tryPushToUpdateQueueOrThrow(update_unit);

        if (source_returns_fetched_columns_in_order_of_keys)
            return request.filterRequestedColumns(fetched_columns_from_storage);
        else
        {
            /// Reorder result from storage to requested keys indexes
            MutableColumns aggregated_columns = aggregateColumnsInOrderOfKeys(
                keys,
                request,
                fetched_columns_from_storage,
                key_index_to_state_from_storage);

            return request.filterRequestedColumns(aggregated_columns);
        }
    }
    else
    {
        /// Start sync update
        update_queue.tryPushToUpdateQueueOrThrow(update_unit);
        update_queue.waitForCurrentUpdateFinish(update_unit);

        requested_keys_to_fetched_columns_during_update_index = std::move(update_unit->requested_keys_to_fetched_columns_during_update_index);
        fetched_columns_during_update = std::move(update_unit->fetched_columns_during_update);
    }

    MutableColumns aggregated_columns = aggregateColumns(
        keys,
        request,
        fetched_columns_from_storage,
        key_index_to_state_from_storage,
        fetched_columns_during_update,
        requested_keys_to_fetched_columns_during_update_index);

    return request.filterRequestedColumns(aggregated_columns);
}

template <DictionaryKeyType dictionary_key_type>
ColumnUInt8::Ptr CacheDictionary<dictionary_key_type>::hasKeys(const Columns & key_columns, const DataTypes & key_types) const
{
    /**
    * Flow of hasKeys. It is similar to getColumns. But there is an important detail, if key is identified with default value in storage
    * it means that in hasKeys result this key will be false.
    *
    * 1. Get fetch result from storage
    * 2. If all keys are found in storage and not expired and there are no default keys return that we have all keys.
    * Otherwise set allow_expired_keys_during_aggregation and go to step 5.
    * 3. If all keys are found in storage and some of them are expired and allow_read_expired keys is true return that we have all keys.
    * Otherwise set allow_expired_keys_during_aggregation and go to step 5.
    * 4. If not all keys are found in storage start sync update from source.
    * 5. Start aggregation of keys from source and storage.
    * If we allow read expired keys from step 2 or 3 then count them as founded in storage.
    * Check if key was found in storage not default for that key set true in result array.
    * Check that key was fetched during update for that key set true in result array.
    */

    if (dictionary_key_type == DictionaryKeyType::complex)
        dict_struct.validateKeyTypes(key_types);

    Arena complex_keys_arena;
    DictionaryKeysExtractor<dictionary_key_type> extractor(key_columns, complex_keys_arena);
    const auto & keys = extractor.getKeys();

    /// We make empty request just to fetch if keys exists
    DictionaryStorageFetchRequest request(dict_struct, {}, {});

    FetchResult result_of_fetch_from_storage;

    {
        /// Write lock on storage
        const ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};

        result_of_fetch_from_storage = cache_storage_ptr->fetchColumnsForKeys(keys, request);
    }

    size_t found_keys_size = result_of_fetch_from_storage.found_keys_size;
    size_t expired_keys_size = result_of_fetch_from_storage.expired_keys_size;
    size_t not_found_keys_size = result_of_fetch_from_storage.not_found_keys_size;

    ProfileEvents::increment(ProfileEvents::DictCacheKeysHit, found_keys_size);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysExpired, expired_keys_size);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysNotFound, not_found_keys_size);

    query_count.fetch_add(keys.size());
    hit_count.fetch_add(found_keys_size);

    size_t keys_to_update_size = expired_keys_size + not_found_keys_size;
    auto update_unit = std::make_shared<CacheDictionaryUpdateUnit<dictionary_key_type>>(key_columns, result_of_fetch_from_storage.key_index_to_state, request, keys_to_update_size);

    HashMap<KeyType, size_t> requested_keys_to_fetched_columns_during_update_index;
    bool allow_expired_keys_during_aggregation = false;

    if (not_found_keys_size == 0 && expired_keys_size == 0)
    {
        /// All keys were found in storage

        if (result_of_fetch_from_storage.default_keys_size == 0)
            return ColumnUInt8::create(keys.size(), true);

        allow_expired_keys_during_aggregation = true;
    }
    else if (not_found_keys_size == 0 && expired_keys_size > 0 && allow_read_expired_keys)
    {
        /// Start async update only if allow read expired keys and all keys are found
        update_queue.tryPushToUpdateQueueOrThrow(update_unit);

        if (result_of_fetch_from_storage.default_keys_size == 0)
            return ColumnUInt8::create(keys.size(), true);

        allow_expired_keys_during_aggregation = true;
    }
    else
    {
        /// Start sync update
        update_queue.tryPushToUpdateQueueOrThrow(update_unit);
        update_queue.waitForCurrentUpdateFinish(update_unit);

        requested_keys_to_fetched_columns_during_update_index = std::move(update_unit->requested_keys_to_fetched_columns_during_update_index);
    }

    auto result = ColumnUInt8::create(keys.size(), false);
    auto & data = result->getData();

    for (size_t key_index = 0; key_index < keys.size(); ++key_index)
    {
        auto key = keys[key_index];

        bool valid_expired_key = allow_expired_keys_during_aggregation && result_of_fetch_from_storage.key_index_to_state[key_index].isExpired();

        if (result_of_fetch_from_storage.key_index_to_state[key_index].isFound() || valid_expired_key)
        {
            /// Check if key was fetched from cache
            data[key_index] = !result_of_fetch_from_storage.key_index_to_state[key_index].isDefault();
        }

        if (requested_keys_to_fetched_columns_during_update_index.has(key))
        {
            /// Check if key was not in cache and was fetched during update
            data[key_index] = true;
        }
    }

    return result;
}

template <DictionaryKeyType dictionary_key_type>
MutableColumns CacheDictionary<dictionary_key_type>::aggregateColumnsInOrderOfKeys(
    const PaddedPODArray<KeyType> & keys,
    const DictionaryStorageFetchRequest & request,
    const MutableColumns & fetched_columns,
    const PaddedPODArray<KeyState> & key_index_to_state)
{
    MutableColumns aggregated_columns = request.makeAttributesResultColumns();

    /// If keys were returned not in order of keys, aggregate fetched columns in order of requested keys.

    for (size_t fetch_request_index = 0; fetch_request_index < request.attributesSize(); ++fetch_request_index)
    {
        if (!request.shouldFillResultColumnWithIndex(fetch_request_index))
            continue;

        const auto & aggregated_column = aggregated_columns[fetch_request_index];
        const auto & fetched_column = fetched_columns[fetch_request_index];

        for (size_t key_index = 0; key_index < keys.size(); ++key_index)
        {
            auto state = key_index_to_state[key_index];

            if (state.isNotFound())
                continue;

            aggregated_column->insertFrom(*fetched_column, state.getFetchedColumnIndex());
        }
    }

    return aggregated_columns;
}

template <DictionaryKeyType dictionary_key_type>
MutableColumns CacheDictionary<dictionary_key_type>::aggregateColumns(
        const PaddedPODArray<KeyType> & keys,
        const DictionaryStorageFetchRequest & request,
        const MutableColumns & fetched_columns_from_storage,
        const PaddedPODArray<KeyState> & key_index_to_fetched_columns_from_storage_result,
        const MutableColumns & fetched_columns_during_update,
        const HashMap<KeyType, size_t> & found_keys_to_fetched_columns_during_update_index)
{
    /**
    * Aggregation of columns fetched from storage and from source during update.
    * If key was found in storage add it to result.
    * If key was found in source during update add it to result.
    * If key was not found in storage or in source during update add default value.
    */

    MutableColumns aggregated_columns = request.makeAttributesResultColumns();

    for (size_t fetch_request_index = 0; fetch_request_index < request.attributesSize(); ++fetch_request_index)
    {
        if (!request.shouldFillResultColumnWithIndex(fetch_request_index))
            continue;

        const auto & aggregated_column = aggregated_columns[fetch_request_index];
        const auto & fetched_column_from_storage = fetched_columns_from_storage[fetch_request_index];
        const auto & fetched_column_during_update = fetched_columns_during_update[fetch_request_index];
        const auto & default_value_provider = request.defaultValueProviderAtIndex(fetch_request_index);

        for (size_t key_index = 0; key_index < keys.size(); ++key_index)
        {
            auto key = keys[key_index];

            auto key_state_from_storage = key_index_to_fetched_columns_from_storage_result[key_index];
            if (key_state_from_storage.isFound())
            {
                /// Check and insert value if key was fetched from cache
                aggregated_column->insertFrom(*fetched_column_from_storage, key_state_from_storage.getFetchedColumnIndex());
                continue;
            }

            /// Check and insert value if key was not in cache and was fetched during update
            const auto * find_iterator_in_fetch_during_update = found_keys_to_fetched_columns_during_update_index.find(key);
            if (find_iterator_in_fetch_during_update)
            {
                aggregated_column->insertFrom(*fetched_column_during_update, find_iterator_in_fetch_during_update->getMapped());
                continue;
            }

            /// Insert default value
            aggregated_column->insert(default_value_provider.getDefaultValue(key_index));
        }
    }

    return aggregated_columns;
}

template <DictionaryKeyType dictionary_key_type>
BlockInputStreamPtr CacheDictionary<dictionary_key_type>::getBlockInputStream(const Names & column_names, size_t max_block_size) const
{
    using BlockInputStreamType = DictionaryBlockInputStream<Key>;
    std::shared_ptr<BlockInputStreamType> stream;

    {
        /// Write lock on storage
        const ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};

        if constexpr (dictionary_key_type == DictionaryKeyType::simple)
            stream = std::make_shared<BlockInputStreamType>(shared_from_this(), max_block_size, cache_storage_ptr->getCachedSimpleKeys(), column_names);
        else
        {
            auto keys = cache_storage_ptr->getCachedComplexKeys();
            stream = std::make_shared<BlockInputStreamType>(shared_from_this(), max_block_size, keys, column_names);
        }
    }

    return stream;
}

template <DictionaryKeyType dictionary_key_type>
void CacheDictionary<dictionary_key_type>::update(CacheDictionaryUpdateUnitPtr<dictionary_key_type> update_unit_ptr)
{
    /**
    * Update has following flow.
    * 1. Filter only necessary keys to request, keys that are expired or not found.
    * And create not_found_keys hash_set including each requested key.
    * In case of simple_keys we need to fill requested_keys_vector with requested value key.
    * In case of complex_keys we need to fill requested_complex_key_rows with requested row.
    * 2. Create stream from source with necessary keys to request using method for simple or complex keys.
    * 3. Create fetched columns during update variable. This columns will aggregate columns that we fetch from source.
    * 4. When block is fetched from source. Split it into keys columns and attributes columns.
    * Insert attributes columns into associated fetched columns during update.
    * Create KeysExtractor and extract keys from keys columns.
    * Update map of requested found key to fetched column index.
    * Remove found key from not_found_keys.
    * 5. Add aggregated columns during update into storage.
    * 6. Add not found keys as default into storage.
    */
    CurrentMetrics::Increment metric_increment{CurrentMetrics::DictCacheRequests};

    size_t found_keys_size = 0;

    DictionaryKeysExtractor<dictionary_key_type> requested_keys_extractor(update_unit_ptr->key_columns, update_unit_ptr->complex_key_arena);
    const auto & requested_keys = requested_keys_extractor.getKeys();

    HashSet<KeyType> not_found_keys;

    std::vector<UInt64> requested_keys_vector;
    std::vector<size_t> requested_complex_key_rows;

    auto & key_index_to_state_from_storage = update_unit_ptr->key_index_to_state;

    for (size_t i = 0; i < key_index_to_state_from_storage.size(); ++i)
    {
        if (key_index_to_state_from_storage[i].isExpired()
            || key_index_to_state_from_storage[i].isNotFound())
        {
            if constexpr (dictionary_key_type == DictionaryKeyType::simple)
                requested_keys_vector.emplace_back(requested_keys[i]);
            else
                requested_complex_key_rows.emplace_back(i);

            auto requested_key = requested_keys[i];
            not_found_keys.insert(requested_key);
        }
    }

    size_t requested_keys_size = update_unit_ptr->keys_to_update_size;
    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequested, requested_keys_size);

    const auto & fetch_request = update_unit_ptr->request;

    const auto now = std::chrono::system_clock::now();

    if (now > backoff_end_time.load())
    {
        try
        {
            auto current_source_ptr = getSourceAndUpdateIfNeeded();

            Stopwatch watch;
            BlockInputStreamPtr stream;

            if constexpr (dictionary_key_type == DictionaryKeyType::simple)
                stream = current_source_ptr->loadIds(requested_keys_vector);
            else
                stream = current_source_ptr->loadKeys(update_unit_ptr->key_columns, requested_complex_key_rows);

            stream->readPrefix();

            size_t skip_keys_size_offset = dict_struct.getKeysSize();
            PaddedPODArray<KeyType> found_keys_in_source;

            Columns fetched_columns_during_update = fetch_request.makeAttributesResultColumnsNonMutable();

            while (Block block = stream->read())
            {
                Columns key_columns;
                key_columns.reserve(skip_keys_size_offset);

                auto block_columns = block.getColumns();

                /// Split into keys columns and attribute columns
                for (size_t i = 0; i < skip_keys_size_offset; ++i)
                {
                    key_columns.emplace_back(*block_columns.begin());
                    block_columns.erase(block_columns.begin());
                }

                DictionaryKeysExtractor<dictionary_key_type> keys_extractor(key_columns, update_unit_ptr->complex_key_arena);
                const auto & keys_extracted_from_block = keys_extractor.getKeys();

                for (size_t index_of_attribute = 0; index_of_attribute < fetched_columns_during_update.size(); ++index_of_attribute)
                {
                    auto & column_to_update = fetched_columns_during_update[index_of_attribute];
                    auto column = block.safeGetByPosition(skip_keys_size_offset + index_of_attribute).column;
                    column_to_update->assumeMutable()->insertRangeFrom(*column, 0, keys_extracted_from_block.size());
                }

                for (size_t i = 0; i < keys_extracted_from_block.size(); ++i)
                {
                    auto fetched_key_from_source = keys_extracted_from_block[i];
                    not_found_keys.erase(fetched_key_from_source);
                    update_unit_ptr->requested_keys_to_fetched_columns_during_update_index[fetched_key_from_source] = found_keys_size;
                    found_keys_in_source.emplace_back(fetched_key_from_source);
                    ++found_keys_size;
                }
            }

            PaddedPODArray<KeyType> not_found_keys_in_source;
            not_found_keys_in_source.reserve(not_found_keys.size());

            for (auto & cell : not_found_keys)
                not_found_keys_in_source.emplace_back(cell.getKey());

            auto & update_unit_ptr_mutable_columns = update_unit_ptr->fetched_columns_during_update;
            for (const auto & fetched_column : fetched_columns_during_update)
                update_unit_ptr_mutable_columns.emplace_back(fetched_column->assumeMutable());

            stream->readSuffix();

            {
                /// Lock for cache modification
                ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};
                cache_storage_ptr->insertColumnsForKeys(found_keys_in_source, fetched_columns_during_update);
                cache_storage_ptr->insertDefaultKeys(not_found_keys_in_source);

                error_count = 0;
                last_exception = std::exception_ptr{};
                backoff_end_time = std::chrono::system_clock::time_point{};
            }

            ProfileEvents::increment(ProfileEvents::DictCacheRequestTimeNs, watch.elapsed());
        }
        catch (...)
        {
            /// Lock just for last_exception safety
            ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};
            ++error_count;
            last_exception = std::current_exception();
            backoff_end_time = now + std::chrono::seconds(calculateDurationWithBackoff(rnd_engine, error_count));

            tryLogException(last_exception, log,
                            "Could not update cache dictionary '" + getDictionaryID().getNameForLogs() +
                            "', next update is scheduled at " + ext::to_string(backoff_end_time.load()));
            try
            {
                std::rethrow_exception(last_exception);
            }
            catch (...)
            {
                throw DB::Exception(ErrorCodes::CACHE_DICTIONARY_UPDATE_FAIL,
                    "Update failed for dictionary {} : {}",
                    getDictionaryID().getNameForLogs(),
                    getCurrentExceptionMessage(true /*with stack trace*/,
                                               true /*check embedded stack trace*/));
            }
        }

        ProfileEvents::increment(ProfileEvents::DictCacheKeysRequestedMiss, requested_keys_size - found_keys_size);
        ProfileEvents::increment(ProfileEvents::DictCacheKeysRequestedFound, found_keys_size);
        ProfileEvents::increment(ProfileEvents::DictCacheRequests);
    }
    else
    {
        /// Won't request source for keys
        throw DB::Exception(ErrorCodes::CACHE_DICTIONARY_UPDATE_FAIL,
            "Query contains keys that are not present in cache or expired. Could not update cache dictionary {} now, because nearest update is scheduled at {}. Try again later.",
            getDictionaryID().getNameForLogs(),
            ext::to_string(backoff_end_time.load()));
    }
}

template class CacheDictionary<DictionaryKeyType::simple>;
template class CacheDictionary<DictionaryKeyType::complex>;

}
