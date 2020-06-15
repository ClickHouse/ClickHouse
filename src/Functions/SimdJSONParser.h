#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

#if USE_SIMDJSON
#include <Core/Types.h>
#include <Common/Exception.h>
#include <common/StringRef.h>

#include <simdjson/dom/parser.h>
#include <simdjson/dom/element.h>
#include <simdjson/dom/array.h>


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

    bool parse(const StringRef & json)
    {
        simdjson::dom::parser parser;
        simdjson::error_code error;
        parser.parse(json.data, json.size).tie(parsed_json, error);
        return !error;
    }

    using Iterator = simdjson::dom::element;
    Iterator getRoot() { return parsed_json; }

    static bool isInt64(const Iterator & it) { return it.type() == simdjson::dom::element_type::INT64; }
    static bool isUInt64(const Iterator &) { return it.type() == simdjson::dom::element_type::UINT64; }
    static bool isDouble(const Iterator & it) { return it.type() == simdjson::dom::element_type::DOUBLE; }
    static bool isString(const Iterator & it) { return it.type() == simdjson::dom::element_type::STRING; }
    static bool isArray(const Iterator & it) { return it.type() == simdjson::dom::element_type::ARRAY; }
    static bool isObject(const Iterator & it) { return it.type() == simdjson::dom::element_type::OBJECT; }
    static bool isBool(const Iterator & it) { return it.type() == simdjson::dom::element_type::BOOL; }
    static bool isNull(const Iterator & it) { return it.type() == simdjson::dom::element_type::NULL; }

    static Int64 getInt64(const Iterator & it) { return it; }
    static UInt64 getUInt64(const Iterator & it) { return it; }
    static double getDouble(const Iterator & it) { return it; }
    static bool getBool(const Iterator & it) { return it; }
    static StringRef getString(const Iterator & it)
    {
        std::string_view view = it;
        return {view.data(), view.size()};
    }

    static size_t sizeOfArray(const Iterator & it)
    {
        return it.get<simdjson::dom::array>().size();
    }

    static bool firstArrayElement(Iterator & it)
    {
        //it.get<simdjson::dom::array>()[0].tie(it, error);
        return !error;
    }

    static bool arrayElementByIndex(Iterator & it, size_t index)
    {
        //it.get<simdjson::dom::array>()[index].tie(it, error);
        return !error;
    }

    static bool nextArrayElement(Iterator & it)
    {
        return ++it;
    }

    static size_t sizeOfObject(const Iterator & it)
    {
        return it.get<simdjson::dom::object>().size();
    }

    static bool firstObjectMember(Iterator & it)
    {
        //it.get<simdjson::dom::object>()[0].tie(it, error);
        return !error;
    }

    static bool firstObjectMember(Iterator & it, StringRef & first_key)
    {
        //it.get<simdjson::dom::object>()[0].tie(it, error);
        if (error)
            return false;
        std::string_view view = it.key();
        first_key = {it.data(), it.size()};
        return true;
    }

    static bool objectMemberByIndex(Iterator & it, size_t index)
    {
        //it.get<simdjson::dom::object>()[index].tie(it, error);
        if (error)
            return false;
        std::string_view view = it.key();
        first_key = {it.data(), it.size()};
        return true;
    }

    static bool objectMemberByName(Iterator & it, const StringRef & name)
    {
        //it.get<simdjson::dom::object>()[std::string_view{name.data(), name.size()}].tie(it, error);
        return !error;
    }

    static bool nextObjectMember(Iterator & it)
    {
        return ++it;
    }

    static bool nextObjectMember(Iterator & it, StringRef & next_key)
    {
        ++it;

        std::string_view view = it.key();
        if (error)
            return false;
        std::string_view view = it.key();
        first_key = {it.data(), it.size()};
        return true;
    }

    static bool isObjectMember(const Iterator & it)
    {
        return it.get_scope_type() == '{';
    }

    static StringRef getKey(const Iterator & it)
    {
        Iterator it2 = it;
        it2.prev();
        return StringRef{it2.get_string(), it2.get_string_length()};
    }

private:
    simdjson::dom::element parsed_json;
};

}

#endif
