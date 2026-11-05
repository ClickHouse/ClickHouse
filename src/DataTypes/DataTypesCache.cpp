#include <DataTypes/DataTypesCache.h>
#include <DataTypes/DataTypeFactory.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/CurrentThread.h>

namespace DB
{

namespace Setting
{
    extern const SettingsTimezone session_timezone;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

SimpleDataTypesCache::SimpleDataTypesCache()
{
    addSimpleType(BinaryTypeIndex::Nothing, "Nothing");
    addSimpleType(BinaryTypeIndex::UInt8, "UInt8");
    addSimpleType(BinaryTypeIndex::UInt16, "UInt16");
    addSimpleType(BinaryTypeIndex::UInt32, "UInt32");
    addSimpleType(BinaryTypeIndex::UInt64, "UInt64");
    addSimpleType(BinaryTypeIndex::UInt128, "UInt128");
    addSimpleType(BinaryTypeIndex::UInt256, "UInt256");
    addSimpleType(BinaryTypeIndex::Int8, "Int8");
    addSimpleType(BinaryTypeIndex::Int16, "Int16");
    addSimpleType(BinaryTypeIndex::Int32, "Int32");
    addSimpleType(BinaryTypeIndex::Int64, "Int64");
    addSimpleType(BinaryTypeIndex::Int128, "Int128");
    addSimpleType(BinaryTypeIndex::Int256, "Int256");
    addSimpleType(BinaryTypeIndex::BFloat16, "BFloat16");
    addSimpleType(BinaryTypeIndex::Float32, "Float32");
    addSimpleType(BinaryTypeIndex::Float64, "Float64");
    addSimpleType(BinaryTypeIndex::Date, "Date");
    addSimpleType(BinaryTypeIndex::Date32, "Date32");
    addSimpleType(BinaryTypeIndex::String, "String");
    addSimpleType(BinaryTypeIndex::UUID, "UUID");
    addSimpleType(BinaryTypeIndex::IPv4, "IPv4");
    addSimpleType(BinaryTypeIndex::IPv6, "IPv6");
    addSimpleType(BinaryTypeIndex::Bool, "Bool");
}

void SimpleDataTypesCache::addSimpleType(BinaryTypeIndex index, const String & type_name)
{
    auto type = DataTypeFactory::instance().get(type_name);
    Element element{type_name, type, type->getDefaultSerialization()};
    by_index[static_cast<uint8_t>(index)] = element;
    by_name.emplace(type_name, std::move(element));
}

bool SimpleDataTypesCache::hasElement(BinaryTypeIndex index) const
{
    uint8_t index_value = static_cast<uint8_t>(index);
    return index_value < by_index.size() && by_index[index_value].type != nullptr;
}

const SimpleDataTypesCache::Element & SimpleDataTypesCache::getElement(BinaryTypeIndex index) const
{
    uint8_t index_value = static_cast<uint8_t>(index);
    if (index_value >= by_index.size() || by_index[index_value].type == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid binary type index: {}", index);

    return by_index[index_value];
}

DataTypePtr SimpleDataTypesCache::getType(BinaryTypeIndex index) const
{
    uint8_t index_value = static_cast<uint8_t>(index);
    if (index_value >= by_index.size() || by_index[index_value].type == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid binary type index: {}", index);

    return by_index[index_value].type;
}

SerializationPtr SimpleDataTypesCache::getSerialization(BinaryTypeIndex index) const
{
    uint8_t index_value = static_cast<uint8_t>(index);
    if (index_value >= by_index.size() || by_index[index_value].serialization == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid binary type index: {}", index);

    return by_index[index_value].serialization;
}

const SimpleDataTypesCache::Element * SimpleDataTypesCache::findByName(const String & type_name) const
{
    auto it = by_name.find(type_name);
    return it != by_name.end() ? &it->second : nullptr;
}

DataTypePtr SimpleDataTypesCache::getType(const String & type_name) const
{
    if (const auto * elem = findByName(type_name))
        return elem->type;

    return DataTypeFactory::instance().get(type_name);
}

SerializationPtr SimpleDataTypesCache::getSerialization(const String & type_name) const
{
    if (const auto * elem = findByName(type_name))
        return elem->serialization;

    auto type = DataTypeFactory::instance().get(type_name);
    return type->getDefaultSerialization();
}

const SimpleDataTypesCache & getSimpleDataTypesCache()
{
    thread_local SimpleDataTypesCache cache;
    return cache;
}

DataTypePtr DataTypesCache::getType(const String & type_name)
{
    /// Check the thread-local cache of simple types first.
    if (const auto * elem = getSimpleDataTypesCache().findByName(type_name))
        return elem->type;

    return getCacheElement(type_name).type;
}

SerializationPtr DataTypesCache::getSerialization(const String & type_name)
{
    /// Check the thread-local cache of simple types first.
    if (const auto * elem = getSimpleDataTypesCache().findByName(type_name))
        return elem->serialization;

    return getCacheElement(type_name).serialization;
}

SerializationPtr DataTypesCache::getSerialization(const String & type_name, const DataTypePtr & type)
{
    /// Check the thread-local cache of simple types first.
    if (const auto * elem = getSimpleDataTypesCache().findByName(type_name))
        return elem->serialization;

    return getCacheElement(type_name, &type).serialization;
}

void DataTypesCache::clearIfQueryContextChanged()
{
    ContextPtr current_query_context = CurrentThread::tryGetQueryContext();

    /// Owner-based identity comparison. It does not lock the weak_ptr, and since we hold
    /// the weak_ptr (keeping the control block alive), an expired context cannot be
    /// confused with a new one allocated at the same address.
    bool same_query_context = !query_context.owner_before(current_query_context) && !current_query_context.owner_before(query_context);

    /// Context identity is not enough: clickhouse-client keeps one long-lived client context
    /// (attached to the client thread by a single query scope) for the whole session and
    /// mutates `session_timezone` in place between queries (see `ClientBase::onTimezoneUpdate`),
    /// so also track the value of the setting the cached types may have captured.
    const String * current_session_timezone
        = current_query_context ? &current_query_context->getSettingsRef()[Setting::session_timezone].value : nullptr;
    bool same_session_timezone
        = current_session_timezone ? *current_session_timezone == session_timezone : session_timezone.empty();

    if (same_query_context && same_session_timezone)
        return;

    /// The thread is now serving a different query, or `session_timezone` changed in place
    /// on the same context. Cached types/serializations may depend on the previous query's
    /// context (e.g. `session_timezone` captured by DateTime types), so they must not be reused.
    cache.clear();
    query_context = current_query_context;
    session_timezone = current_session_timezone ? *current_session_timezone : String();
}

const DataTypesCache::Element & DataTypesCache::getCacheElement(const String & type_name, const DataTypePtr * known_type)
{
    clearIfQueryContextChanged();

    auto it = cache.find(type_name);
    if (it != cache.end())
        return it->second;

    /// If cache is full, just clear it.
    if (cache.size() >= MAX_ELEMENTS)
        cache.clear();

    auto type = known_type ? *known_type : DataTypeFactory::instance().get(type_name);
    it = cache.emplace(type_name, Element{type, type->getDefaultSerialization()}).first;
    return it->second;
}

DataTypesCache & getDataTypesCache()
{
    thread_local static DataTypesCache data_types_cache;
    return data_types_cache;
}

}
