#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesBinaryEncoding.h>

#include <array>
#include <unordered_map>

namespace DB
{

/// Cache of simple (parameterless) data types and their serializations,
/// pre-filled at construction time. Avoids repeated DataTypeFactory lookups
/// and shared_ptr allocations for commonly used types.
/// Thread-safe: immutable after construction.
class SimpleDataTypesCache
{
public:
    struct Element
    {
        String name;
        DataTypePtr type;
        SerializationPtr serialization;
    };

    static const SimpleDataTypesCache & instance();

    bool hasElement(BinaryTypeIndex index) const;

    /// O(1) lookup by BinaryTypeIndex. Returns the cached element.
    const Element & getElement(BinaryTypeIndex index) const;

    /// O(1) lookup by BinaryTypeIndex. Returns pre-cached type.
    DataTypePtr getType(BinaryTypeIndex index) const;

    /// O(1) lookup by BinaryTypeIndex. Returns pre-cached serialization.
    SerializationPtr getSerialization(BinaryTypeIndex index) const;

    /// Lookup by type name. Returns pre-cached element for simple types, nullptr otherwise.
    const Element * findByName(const String & type_name) const;

    /// Lookup by type name. Returns pre-cached type for simple types,
    /// falls back to DataTypeFactory for others.
    DataTypePtr getType(const String & type_name) const;

    /// Lookup serialization by type name. Returns pre-cached serialization
    /// for simple types, falls back to DataTypeFactory for others.
    SerializationPtr getSerialization(const String & type_name) const;

private:
    SimpleDataTypesCache();
    void addSimpleType(BinaryTypeIndex index, const String & type_name);

    std::array<Element, BINARY_TYPE_INDEX_SIZE> by_index{};
    std::unordered_map<String, Element> by_name;
};

/// Return the singleton instance of the simple data type cache.
const SimpleDataTypesCache & getSimpleDataTypesCache();

/// Thread-local cache for data type lookups by name.
/// Checks the global SimpleDataTypesCache first; only caches
/// non-simple types (e.g. DateTime64(9), Variant types) in its own map.
class DataTypesCache
{
public:
    DataTypePtr getType(const String & type_name);
    SerializationPtr getSerialization(const String & type_name);

private:
    static constexpr size_t MAX_ELEMENTS = 16;

    struct Element
    {
        DataTypePtr type;
        SerializationPtr serialization;
    };

    const Element & getCacheElement(const String & type_name);

    std::unordered_map<String, Element> cache;
};

/// Return instance of a thread local cache.
/// Cache is relatively small, so it's ok to have separate instance per thread to avoid using mutex inside the cache.
DataTypesCache & getDataTypesCache();

}
