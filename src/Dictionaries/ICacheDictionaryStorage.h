#pragma once

#include <Common/PODArray.h>
#include <Common/HashTable/HashMap.h>
#include <Columns/IColumn.h>
#include <Dictionaries/DictionaryHelpers.h>

namespace DB
{

/// Result of fetch from CacheDictionaryStorage
template <typename KeyType>
struct KeysStorageFetchResult
{
    /// Fetched column values
    MutableColumns fetched_columns;

    /// Found key to index in fetched_columns
    HashMap<KeyType, size_t> found_keys_to_fetched_columns_index;

    /// Expired key to index in fetched_columns
    HashMap<KeyType, size_t> expired_keys_to_fetched_columns_index;

    /// Keys that are not found in storage
    PaddedPODArray<KeyType> not_found_or_expired_keys;

    /// Indexes of requested keys that are not found in storage
    PaddedPODArray<size_t> not_found_or_expired_keys_indexes;

};

using SimpleKeysStorageFetchResult = KeysStorageFetchResult<UInt64>;
using ComplexKeysStorageFetchResult = KeysStorageFetchResult<StringRef>;

class ICacheDictionaryStorage
{
public:

    virtual ~ICacheDictionaryStorage() = default;

    /// Necessary if all keys are found we can return result to client without additional aggregation
    virtual bool returnFetchedColumnsDuringFetchInOrderOfRequestedKeys() const = 0;

    /// Does storage support simple keys
    virtual bool supportsSimpleKeys() const = 0;

    /// Fetch columns for keys, this method is not write thread safe
    virtual SimpleKeysStorageFetchResult fetchColumnsForKeys(
        const PaddedPODArray<UInt64> & keys,
        const DictionaryStorageFetchRequest & fetch_request) = 0;

    /// Fetch columns for keys, this method is not write thread safe
    virtual void insertColumnsForKeys(const PaddedPODArray<UInt64> & keys, Columns columns) = 0;

    /// Return cached simple keys
    virtual PaddedPODArray<UInt64> getCachedSimpleKeys() const = 0;

    /// Does storage support complex keys
    virtual bool supportsComplexKeys() const = 0;

    /// Fetch columns for keys, this method is not write thread safe
    virtual ComplexKeysStorageFetchResult fetchColumnsForKeys(
        const PaddedPODArray<StringRef> & keys,
        const DictionaryStorageFetchRequest & column_fetch_requests) = 0;

    /// Fetch columns for keys, this method is not write thread safe
    virtual void insertColumnsForKeys(const PaddedPODArray<StringRef> & keys, Columns columns) = 0;

    /// Return cached simple keys
    virtual PaddedPODArray<StringRef> getCachedComplexKeys() const = 0;

    /// Return size of keys in storage
    virtual size_t getSize() const = 0;

    /// Return bytes allocated in storage
    virtual size_t getBytesAllocated() const = 0;

};

using CacheDictionaryStoragePtr = std::shared_ptr<ICacheDictionaryStorage>;

}
