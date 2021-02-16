#pragma once

#include <Common/PODArray.h>
#include <Common/HashTable/HashMap.h>
#include <Columns/IColumn.h>
#include <Dictionaries/DictionaryHelpers.h>

namespace DB
{

template <typename KeyType>
struct KeysStorageFetchResult
{

    MutableColumns fetched_columns;

    HashMap<KeyType, size_t> found_keys_to_fetched_columns_index;

    HashMap<KeyType, size_t> expired_keys_to_fetched_columns_index;

    PaddedPODArray<KeyType> not_found_or_expired_keys;

    PaddedPODArray<size_t> not_found_or_expired_keys_indexes;

};

using SimpleKeysStorageFetchResult = KeysStorageFetchResult<UInt64>;
using ComplexKeysStorageFetchResult = KeysStorageFetchResult<StringRef>;

class DictionaryStorageFetchRequest
{
public:
    DictionaryStorageFetchRequest(const DictionaryStructure & structure, const Strings & attributes_names_to_fetch)
        : attributes_to_fetch_names_set(attributes_names_to_fetch.begin(), attributes_names_to_fetch.end())
        , attributes_to_fetch_filter(structure.attributes.size(), false)
    {
        size_t attributes_size = structure.attributes.size();
        attributes_to_fetch_types.reserve(attributes_size);

        for (size_t i = 0; i < attributes_size; ++i)
        {
            const auto & name = structure.attributes[i].name;
            const auto & type = structure.attributes[i].type;
            attributes_to_fetch_types.emplace_back(type);

            if (attributes_to_fetch_names_set.find(name) != attributes_to_fetch_names_set.end())
            {
                attributes_to_fetch_filter[i] = true;
            }
        }
    }

    DictionaryStorageFetchRequest() = default;

    size_t attributesSize() const
    {
        return attributes_to_fetch_types.size();
    }

    bool containsAttribute(const String & attribute_name) const
    {
        return attributes_to_fetch_names_set.find(attribute_name) != attributes_to_fetch_names_set.end();
    }

    bool shouldFillResultColumnWithIndex(size_t attribute_index) const
    {
        return attributes_to_fetch_filter[attribute_index];
    }

    MutableColumns makeAttributesResultColumns() const
    {
        MutableColumns result;
        result.reserve(attributes_to_fetch_types.size());

        for (const auto & type : attributes_to_fetch_types)
            result.emplace_back(type->createColumn());

        return result;
    }
private:
    std::unordered_set<String> attributes_to_fetch_names_set;
    std::vector<bool> attributes_to_fetch_filter;
    DataTypes attributes_to_fetch_types;
};

class ICacheDictionaryStorage
{
public:

    virtual ~ICacheDictionaryStorage() = default;

    virtual bool supportsSimpleKeys() const = 0;

    virtual SimpleKeysStorageFetchResult fetchColumnsForKeys(
        const PaddedPODArray<UInt64> & keys,
        const DictionaryStorageFetchRequest & fetch_request) const = 0;

    virtual void insertColumnsForKeys(const PaddedPODArray<UInt64> & keys, Columns columns) = 0;

    virtual PaddedPODArray<UInt64> getCachedSimpleKeys() const = 0;

    virtual bool supportsComplexKeys() const = 0;

    virtual ComplexKeysStorageFetchResult fetchColumnsForKeys(
        const PaddedPODArray<StringRef> & keys,
        const DictionaryStorageFetchRequest & column_fetch_requests) const = 0;

    virtual void insertColumnsForKeys(const PaddedPODArray<StringRef> & keys, Columns columns) = 0;

    virtual PaddedPODArray<StringRef> getCachedComplexKeys() const = 0;

    virtual size_t getSize() const = 0;

    virtual size_t getBytesAllocated() const = 0;

};

using CacheDictionaryStoragePtr = std::shared_ptr<ICacheDictionaryStorage>;

}
