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
/// Thread-local to avoid atomic refcount contention on shared_ptr
/// when multiple threads return copies of the same DataTypePtr.
class SimpleDataTypesCache
{
public:
    struct Element
    {
        String name;
        DataTypePtr type;
        SerializationPtr serialization;
    };

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

    SimpleDataTypesCache();

private:
    void addSimpleType(BinaryTypeIndex index, const String & type_name);

    std::array<Element, BINARY_TYPE_INDEX_SIZE> by_index{};
    std::unordered_map<String, Element> by_name;
};

/// Return a thread-local instance of the simple data type cache.
const SimpleDataTypesCache & getSimpleDataTypesCache();

/// Thread-local, name-keyed cache of data types and their default serializations; simple types are
/// served by SimpleDataTypesCache instead.
class DataTypesCache
{
public:
    DataTypePtr getType(const String & type_name);
    SerializationPtr getSerialization(const String & type_name);

    /// Keys by `type->getName()` and, on a miss, takes the type as given rather than parsing that name.
    SerializationPtr getSerialization(const DataTypePtr & type);

private:
    /// Sized to cover a full set of Dynamic variants (up to 255) plus types from the
    /// shared variant, so that interleaved values of many distinct non-simple types
    /// (e.g. a Dynamic(max_types=N) column with N > 16 complex variants) do not
    /// constantly clear and rebuild the cache. Matches ColumnDynamic::SERIALIZATION_CACHE_MAX_SIZE.
    static constexpr size_t MAX_ELEMENTS = 256;

    struct Element
    {
        DataTypePtr type;
        /// Null for an element that was not stored; callers then build it from `type`.
        SerializationPtr serialization;
    };

    /// The stored element for `type_name`, or a fresh one that is deliberately not stored.
    /// `known_type`, when set, is the type whose `getName` produced `type_name`, so a miss uses it as is.
    Element getElement(const String & type_name, const DataTypePtr & known_type = {});

    std::unordered_map<String, Element> cache;
};

/// Return instance of a thread local cache.
/// Cache is relatively small, so it's ok to have separate instance per thread to avoid using mutex inside the cache.
DataTypesCache & getDataTypesCache();

}
