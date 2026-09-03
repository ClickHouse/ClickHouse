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
/// This scoping is also what makes it safe to pool serializations with
/// `supportsPooling() == false` here, e.g. SerializationJSON: its own contract
/// (see the comment in SerializationJSON::create) forbids sharing *across queries*,
/// because its extraction tree accumulates mutable, context-dependent state (its
/// documented example is exactly the timezone case this cache already invalidates
/// on). Reuse *within* one query is already an established, trusted pattern for the
/// very same object (see ColumnDynamic's per-column `serialization_cache`, used
/// throughout its binary insert/deserialize paths).
///
/// IMPORTANT: this safety only holds for the ways SerializationJSON is currently used
/// through this cache (type resolution, binary serialization, and text *output*) - none
/// of which touch the mutable extraction-tree caches or the parser choice. A cached
/// serialization from here must never be used for *text deserialization*: that path is
/// exactly what SerializationJSON::create's "do NOT pool" comment is about, and two
/// gaps this cache does not track would then matter. First, `DataTypeObject::doGetSerialization`
/// reads `allow_simdjson` at construction; a client session that flips it in place would
/// keep the previously-built parser, and rapidjson (`RAPIDJSON_PARSE_DEFAULT_FLAGS` lacks
/// `kParseFullPrecisionFlag`) is not a drop-in replacement for simdjson's parsing the way
/// it is for output - it can round Float64 differently and has no nesting-depth cap. Second,
/// the extraction tree's caches would then legitimately accumulate per-query state and need
/// the same cross-query invalidation this cache does not extend to their internals.
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
