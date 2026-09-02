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
/// `DateLUT::instance`). Since the cache lives in a long-lived thread, it must be
/// cleared whenever the thread starts serving a different query, and also when
/// `session_timezone` is mutated in place on the same context (clickhouse-client
/// does this between queries of one session); otherwise a stale entry produces
/// wrong results (e.g. DateTime values rendered in another query's timezone).
///
/// Serializations with `supportsPooling() == false` (e.g. SerializationJSON, which
/// holds mutable per-use state, see the comment in SerializationJSON::create) are
/// never cached at all, not even within one query: for them only the type is cached
/// and a fresh serialization is built on every lookup. Note that query-scoped
/// invalidation alone would not be enough for them anyway: clickhouse-client keeps
/// one client context attached to the client thread for the whole session, so
/// consecutive client queries can share one "query scope".
class DataTypesCache
{
public:
    DataTypePtr getType(const String & type_name);
    SerializationPtr getSerialization(const String & type_name);

    /// Same as getSerialization(type_name), but on a cache miss reuses the already
    /// constructed `type` instead of parsing `type_name` through DataTypeFactory.
    /// `type_name` must be equal to `type->getName()`.
    SerializationPtr getSerialization(const String & type_name, const DataTypePtr & type);

private:
    /// Sized to cover a full set of Dynamic variants (up to 255) plus types from the
    /// shared variant, so that interleaved values of many distinct non-simple types
    /// (e.g. a Dynamic(max_types=N) column with N > 16 complex variants) do not
    /// constantly clear and rebuild the cache. Matches ColumnDynamic::SERIALIZATION_CACHE_MAX_SIZE.
    static constexpr size_t MAX_ELEMENTS = 256;

    struct Element
    {
        DataTypePtr type;
        /// Null if the default serialization of `type` has `supportsPooling() == false`:
        /// such serializations keep mutable per-use state and must be rebuilt for every use
        /// instead of being served from the cache.
        SerializationPtr serialization;
    };

    /// If `known_type` is provided, it is used on a cache miss instead of a DataTypeFactory lookup.
    const Element & getCacheElement(const String & type_name, const DataTypePtr * known_type = nullptr);

    /// Clear the cache if the thread is now attached to a different query context
    /// than the one the cache was populated under, or if `session_timezone` was
    /// changed in place on the same context.
    void clearIfQueryContextChanged();

    std::unordered_map<String, Element> cache;

    /// The query context the cached entries were created under (null for threads
    /// not attached to any query). Holding a weak_ptr keeps the control block alive,
    /// which makes the owner-based identity comparison immune to address reuse.
    ContextWeakPtr query_context;

    /// The value of `session_timezone` the cached entries were created under. Tracked
    /// in addition to the context identity because clickhouse-client keeps one
    /// long-lived client context for the whole session and mutates the setting
    /// in place between queries (see `ClientBase::onTimezoneUpdate`).
    String session_timezone;
};

/// Return instance of a thread local cache.
/// Cache is relatively small, so it's ok to have separate instance per thread to avoid using mutex inside the cache.
DataTypesCache & getDataTypesCache();

}
