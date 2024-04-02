#pragma once

#include <Common/PODArray.h>
#include <Common/HashTable/HashMap.h>
#include <Columns/IColumn.h>
#include <Dictionaries/DictionaryHelpers.h>

namespace DB
{

struct KeyState
{
    enum State: uint8_t
    {
        not_found = 0,
        expired = 1,
        found = 2,
    };

    KeyState(State state_, size_t fetched_column_index_)
        : state(state_)
        , fetched_column_index(fetched_column_index_)
    {}

    KeyState(State state_) /// NOLINT
        : state(state_)
    {}

    inline bool isFound() const { return state == State::found; }
    inline bool isExpired() const { return state == State::expired; }
    inline bool isNotFound() const { return state == State::not_found; }
    inline bool isDefault() const { return is_default; }
    inline void setDefault() { is_default = true; }
    inline void setDefaultValue(bool is_default_value) { is_default = is_default_value; }
    /// Valid only if keyState is found or expired
    inline size_t getFetchedColumnIndex() const { return fetched_column_index; }
    inline void setFetchedColumnIndex(size_t fetched_column_index_value) { fetched_column_index = fetched_column_index_value; }
private:
    State state = not_found;
    size_t fetched_column_index = 0;
    bool is_default = false;
};

/// Result of fetch from CacheDictionaryStorage
template <typename KeyType>
struct KeysStorageFetchResult
{
    /// Fetched column values
    MutableColumns fetched_columns;

    PaddedPODArray<KeyState> key_index_to_state;

    size_t expired_keys_size = 0;

    size_t found_keys_size = 0;

    size_t not_found_keys_size = 0;

    size_t default_keys_size = 0;

};

using SimpleKeysStorageFetchResult = KeysStorageFetchResult<UInt64>;
using ComplexKeysStorageFetchResult = KeysStorageFetchResult<StringRef>;

class ICacheDictionaryStorage
{
public:

    virtual ~ICacheDictionaryStorage() = default;

    /// Necessary if all keys are found we can return result to client without additional aggregation
    virtual bool returnsFetchedColumnsInOrderOfRequestedKeys() const = 0;

    /// Name of storage
    virtual String getName() const = 0;

    /// Does storage support simple keys
    virtual bool supportsSimpleKeys() const = 0;

    /// Fetch columns for keys, this method is not write thread safe
    virtual SimpleKeysStorageFetchResult fetchColumnsForKeys(
        const PaddedPODArray<UInt64> & keys,
        const DictionaryStorageFetchRequest & fetch_request) = 0;

    /// Fetch columns for keys, this method is not write thread safe
    virtual void insertColumnsForKeys(const PaddedPODArray<UInt64> & keys, Columns columns) = 0;

    /// Insert default keys
    virtual void insertDefaultKeys(const PaddedPODArray<UInt64> & keys) = 0;

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

    /// Insert default keys
    virtual void insertDefaultKeys(const PaddedPODArray<StringRef> & keys) = 0;

    /// Return cached complex keys.
    /// It is client responsibility to ensure keys proper lifetime.
    virtual PaddedPODArray<StringRef> getCachedComplexKeys() const = 0;

    /// Return size of keys in storage
    virtual size_t getSize() const = 0;

    /// Returns storage load factor
    virtual double getLoadFactor() const = 0;

    /// Return bytes allocated in storage
    virtual size_t getBytesAllocated() const = 0;

};

using CacheDictionaryStoragePtr = std::shared_ptr<ICacheDictionaryStorage>;

}
