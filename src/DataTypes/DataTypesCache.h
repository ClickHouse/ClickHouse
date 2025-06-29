#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>

namespace DB
{

const size_t MAX_DATA_TYPES_ELEMENTS = 4096;

/// Simple cache of data types and their serializations to avoid creating
/// them from String using FormatFactory or creating shared_ptr explicitly.
/// It is helpful when we need to create the same data types multiple times
/// (for example in Dynamic data type).
class DataTypesCache
{
public:
    struct Element
    {
        DataTypePtr type;
        SerializationPtr serialization;
    };

    DataTypePtr getType(const String & type_name)
    {
        auto it = cache.find(type_name);
        if (it != cache.end())
            return it->second.type;

        /// If cache is full, just clear it.
        if (cache.size() >= MAX_DATA_TYPES_ELEMENTS)
            cache.clear();

        auto type = DataTypeFactory::instance().get(type_name);
        it = cache.emplace(type_name, Element{type, type->getDefaultSerialization()}).first;
        return it->second.type;
    }

    SerializationPtr getSerialization(const String & type_name)
    {
        auto it = cache.find(type_name);
        if (it != cache.end())
            return it->second.serialization;

        /// If cache is full, just clear it.
        if (cache.size() >= MAX_DATA_TYPES_ELEMENTS)
            cache.clear();
        auto type = DataTypeFactory::instance().get(type_name);
        it = cache.emplace(type_name, Element{type, type->getDefaultSerialization()}).first;
        return it->second.serialization;
    }

private:

    std::unordered_map<String, Element> cache;
};

/// Return instance of a thread local cache.
/// Cache is relatively small, so it's ok to have separate instance per thread to avoid using mutex inside the cache.
DataTypesCache & getDataTypesCache();

}
