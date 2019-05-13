#pragma once

#include <Common/config.h>
#if USE_SIMDJSON

#include <common/StringRef.h>
#include <Common/Exception.h>
#include <Core/Types.h>

#ifdef __clang__
    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wold-style-cast"
    #pragma clang diagnostic ignored "-Wnewline-eof"
#endif

#include <simdjson/jsonparser.h>

#ifdef __clang__
    #pragma clang diagnostic pop
#endif


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
        if (!pj.allocateCapacity(max_size))
            throw Exception{"Can not allocate memory for " + std::to_string(max_size) + " units when parsing JSON",
                            ErrorCodes::CANNOT_ALLOCATE_MEMORY};
    }

    bool parse(const char * data, size_t size) { return !json_parse(data, size, pj); }

    using Iterator = ParsedJson::iterator;
    Iterator getRoot() { return Iterator{pj}; }

    static bool downToArray(Iterator & it) { return it.down(); }

    static bool downToObject(Iterator & it) { return it.down() && it.next(); }

    static bool downToObject(Iterator & it, StringRef & first_key)
    {
        if (!it.down())
            return false;
        first_key.data = it.get_string();
        first_key.size = it.get_string_length();
        return it.next();
    }

    static bool parentScopeIsObject(const Iterator & it) { return it.get_scope_type() == '{'; }

    static bool next(Iterator & it) { return it.next(); }

    static bool nextKeyValue(Iterator & it) { return it.next() && it.next(); }

    static bool nextKeyValue(Iterator & it, StringRef & key)
    {
        if (!it.next())
            return false;
        key.data = it.get_string();
        key.size = it.get_string_length();
        return it.next();
    }

    static bool isInteger(const Iterator & it) { return it.is_integer(); }

    static bool isFloat(const Iterator & it) { return it.is_double(); }

    static bool isString(const Iterator & it) { return it.is_string(); }

    static bool isArray(const Iterator & it) { return it.is_array(); }

    static bool isObject(const Iterator & it) { return it.is_object(); }

    static bool isBool(const Iterator & it) { return it.get_type() == 't' || it.get_type() == 'f'; }

    static bool isNull(const Iterator & it) { return it.get_type() == 'n'; }

    static StringRef getKey(const Iterator & it)
    {
        Iterator it2 = it;
        it2.prev();
        return StringRef{it2.get_string(), it2.get_string_length()};
    }

    static StringRef getString(const Iterator & it) { return StringRef{it.get_string(), it.get_string_length()}; }

    static Int64 getInteger(const Iterator & it) { return it.get_integer(); }

    static double getFloat(const Iterator & it) { return it.get_double(); }

    static bool getBool(const Iterator & it) { return it.get_type() == 't'; }

private:
    ParsedJson pj;
};

}
#endif
