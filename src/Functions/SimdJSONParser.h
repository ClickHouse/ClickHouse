#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

#if USE_SIMDJSON
#    include <Core/Types.h>
#    include <Common/Exception.h>
#    include <common/StringRef.h>

#    include <simdjson/jsonparser.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}

/// This class can be used as an argument for the template class FunctionJSON.
/// It provides ability to parse JSONs using simdjson library.
struct SimdJSONParser
{
    static constexpr bool need_preallocate = true;

    void preallocate(size_t max_size)
    {
        if (!pj.allocate_capacity(max_size))
            throw Exception{"Can not allocate memory for " + std::to_string(max_size) + " units when parsing JSON",
                            ErrorCodes::CANNOT_ALLOCATE_MEMORY};
    }

    bool parse(const StringRef & json) { return !json_parse(json.data, json.size, pj); }

    using Iterator = simdjson::ParsedJson::Iterator;
    Iterator getRoot() { return Iterator{pj}; }

    static bool isInt64(const Iterator & it) { return it.is_integer(); }
    static bool isUInt64(const Iterator &) { return false; /* See https://github.com/lemire/simdjson/issues/68 */ }
    static bool isDouble(const Iterator & it) { return it.is_double(); }
    static bool isString(const Iterator & it) { return it.is_string(); }
    static bool isArray(const Iterator & it) { return it.is_array(); }
    static bool isObject(const Iterator & it) { return it.is_object(); }
    static bool isBool(const Iterator & it) { return it.get_type() == 't' || it.get_type() == 'f'; }
    static bool isNull(const Iterator & it) { return it.is_null(); }

    static Int64 getInt64(const Iterator & it) { return it.get_integer(); }
    static UInt64 getUInt64(const Iterator &) { return 0; /* isUInt64() never returns true */ }
    static double getDouble(const Iterator & it) { return it.get_double(); }
    static bool getBool(const Iterator & it) { return it.get_type() == 't'; }
    static StringRef getString(const Iterator & it) { return StringRef{it.get_string(), it.get_string_length()}; }

    static size_t sizeOfArray(const Iterator & it)
    {
        size_t size = 0;
        Iterator it2 = it;
        if (it2.down())
        {
            do
                ++size;
            while (it2.next());
        }
        return size;
    }

    static bool firstArrayElement(Iterator & it) { return it.down(); }

    static bool arrayElementByIndex(Iterator & it, size_t index)
    {
        if (!it.down())
            return false;
        while (index--)
            if (!it.next())
                return false;
        return true;
    }

    static bool nextArrayElement(Iterator & it) { return it.next(); }

    static size_t sizeOfObject(const Iterator & it)
    {
        size_t size = 0;
        Iterator it2 = it;
        if (it2.down())
        {
            do
                ++size;
            while (it2.next() && it2.next()); //-V501
        }
        return size;
    }

    static bool firstObjectMember(Iterator & it) { return it.down() && it.next(); }

    static bool firstObjectMember(Iterator & it, StringRef & first_key)
    {
        if (!it.down())
            return false;
        first_key.data = it.get_string();
        first_key.size = it.get_string_length();
        return it.next();
    }

    static bool objectMemberByIndex(Iterator & it, size_t index)
    {
        if (!it.down())
            return false;
        while (index--)
            if (!it.next() || !it.next()) //-V501
                return false;
        return it.next();
    }

    static bool objectMemberByName(Iterator & it, const StringRef & name) { return it.move_to_key(name.data); }
    static bool nextObjectMember(Iterator & it) { return it.next() && it.next(); } //-V501

    static bool nextObjectMember(Iterator & it, StringRef & next_key)
    {
        if (!it.next())
            return false;
        next_key.data = it.get_string();
        next_key.size = it.get_string_length();
        return it.next();
    }

    static bool isObjectMember(const Iterator & it) { return it.get_scope_type() == '{'; }

    static StringRef getKey(const Iterator & it)
    {
        Iterator it2 = it;
        it2.prev();
        return StringRef{it2.get_string(), it2.get_string_length()};
    }

private:
    simdjson::ParsedJson pj;
};

}

#endif
