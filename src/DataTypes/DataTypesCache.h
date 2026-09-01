#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Interpreters/Context_fwd.h>

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

/// Thread-local cache for data type lookups by name.
/// Checks the thread-local SimpleDataTypesCache first; only caches
/// non-simple types (e.g. DateTime64(9), Variant types) in its own map.
///
/// The cache is scoped to a single query: a type name does not uniquely identify
/// a type/serialization across queries, because construction depends on the current
/// query context. For example, `DateTime` without an explicit timezone captures
/// the query's `session_timezone` setting at construction (see TimezoneMixin and
/// `DateLUT::instance`), and `JSON` serializations are built against the current
/// query context and hold mutable per-use state that must not be shared between
/// queries (see the comment in SerializationJSON::create). Since the cache lives
/// in a long-lived thread, it must be cleared whenever the thread starts serving
/// a different query; otherwise a stale entry produces wrong results (e.g. DateTime
/// values rendered in another query's timezone).
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

    /// Clear the cache if the thread is now attached to a different query context
    /// than the one the cache was populated under.
    void clearIfQueryContextChanged();

    std::unordered_map<String, Element> cache;

    /// The query context the cached entries were created under (null for threads
    /// not attached to any query). Holding a weak_ptr keeps the control block alive,
    /// which makes the owner-based identity comparison immune to address reuse.
    ContextWeakPtr query_context;
};

/// Return instance of a thread local cache.
/// Cache is relatively small, so it's ok to have separate instance per thread to avoid using mutex inside the cache.
DataTypesCache & getDataTypesCache();

}
