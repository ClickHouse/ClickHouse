#pragma once

#include "config_functions.h"

#if USE_SIMDJSON
#    include <base/types.h>
#    include <Common/Exception.h>
#    include <base/defines.h>
#    include <simdjson.h>

#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}

/// This class can be used as an argument for the template class FunctionJSON.
/// It provides ability to parse JSONs using simdjson library.
struct DomSimdJSONParser
{
    class Array;
    class Object;

    /// References an element in a JSON document, representing a JSON null, boolean, string, number,
    /// array or object.
    class Element
    {
    public:
        ALWAYS_INLINE Element() {} /// NOLINT
        ALWAYS_INLINE Element(const simdjson::dom::element & element_) : element(element_) {} /// NOLINT

        ALWAYS_INLINE bool isInt64() const { return element.type() == simdjson::dom::element_type::INT64; }
        ALWAYS_INLINE bool isUInt64() const { return element.type() == simdjson::dom::element_type::UINT64; }
        ALWAYS_INLINE bool isDouble() const { return element.type() == simdjson::dom::element_type::DOUBLE; }
        ALWAYS_INLINE bool isString() const { return element.type() == simdjson::dom::element_type::STRING; }
        ALWAYS_INLINE bool isArray() const { return element.type() == simdjson::dom::element_type::ARRAY; }
        ALWAYS_INLINE bool isObject() const { return element.type() == simdjson::dom::element_type::OBJECT; }
        ALWAYS_INLINE bool isBool() const { return element.type() == simdjson::dom::element_type::BOOL; }
        ALWAYS_INLINE bool isNull() const { return element.type() == simdjson::dom::element_type::NULL_VALUE; }

        ALWAYS_INLINE Int64 getInt64() const { return element.get_int64().value_unsafe(); }
        ALWAYS_INLINE UInt64 getUInt64() const { return element.get_uint64().value_unsafe(); }
        ALWAYS_INLINE double getDouble() const { return element.get_double().value_unsafe(); }
        ALWAYS_INLINE bool getBool() const { return element.get_bool().value_unsafe(); }
        ALWAYS_INLINE std::string_view getString() const { return element.get_string().value_unsafe(); }
        ALWAYS_INLINE Array getArray() const;
        ALWAYS_INLINE Object getObject() const;

        ALWAYS_INLINE simdjson::dom::element getElement() const { return element; }

    private:
        simdjson::dom::element element;
    };

    /// References an array in a JSON document.
    class Array
    {
    public:
        class Iterator
        {
        public:
            ALWAYS_INLINE Iterator(const simdjson::dom::array::iterator & it_) : it(it_) {} /// NOLINT
            ALWAYS_INLINE Element operator*() const { return *it; }
            ALWAYS_INLINE Iterator & operator++() { ++it; return *this; }
            ALWAYS_INLINE Iterator operator++(int) { auto res = *this; ++it; return res; } /// NOLINT
            ALWAYS_INLINE friend bool operator!=(const Iterator & left, const Iterator & right) { return left.it != right.it; }
            ALWAYS_INLINE friend bool operator==(const Iterator & left, const Iterator & right) { return !(left != right); }
        private:
            simdjson::dom::array::iterator it;
        };

        ALWAYS_INLINE Array(const simdjson::dom::array & array_) : array(array_) {} /// NOLINT
        ALWAYS_INLINE Iterator begin() const { return array.begin(); }
        ALWAYS_INLINE Iterator end() const { return array.end(); }
        ALWAYS_INLINE size_t size() const { return array.size(); }
        ALWAYS_INLINE Element operator[](size_t index) const { assert(index < size()); return array.at(index).value_unsafe(); }

    private:
        simdjson::dom::array array;
    };

    using KeyValuePair = std::pair<std::string_view, Element>;

    /// References an object in a JSON document.
    class Object
    {
    public:
        class Iterator
        {
        public:
            ALWAYS_INLINE Iterator(const simdjson::dom::object::iterator & it_) : it(it_) {} /// NOLINT
            ALWAYS_INLINE KeyValuePair operator*() const { const auto & res = *it; return {res.key, res.value}; }
            ALWAYS_INLINE Iterator & operator++() { ++it; return *this; }
            ALWAYS_INLINE Iterator operator++(int) { auto res = *this; ++it; return res; } /// NOLINT
            ALWAYS_INLINE friend bool operator!=(const Iterator & left, const Iterator & right) { return left.it != right.it; }
            ALWAYS_INLINE friend bool operator==(const Iterator & left, const Iterator & right) { return !(left != right); }
        private:
            simdjson::dom::object::iterator it;
        };

        ALWAYS_INLINE Object(const simdjson::dom::object & object_) : object(object_) {} /// NOLINT
        ALWAYS_INLINE Iterator begin() const { return object.begin(); }
        ALWAYS_INLINE Iterator end() const { return object.end(); }
        ALWAYS_INLINE size_t size() const { return object.size(); }

        bool find(std::string_view key, Element & result) const
        {
            auto x = object.at_key(key);
            if (x.error())
                return false;

            result = x.value_unsafe();
            return true;
        }

        /// Optional: Provides access to an object's element by index.
        KeyValuePair operator[](size_t index) const
        {
            assert(index < size());
            auto it = object.begin();
            while (index--)
                ++it;
            const auto & res = *it;
            return {res.key, res.value};
        }

    private:
        simdjson::dom::object object;
    };

    /// Parses a JSON document, returns the reference to its root element if succeeded.
    bool parse(std::string_view json, Element & result)
    {
        auto document = parser.parse(json.data(), json.size());
        if (document.error())
            return false;

        result = document.value_unsafe();
        return true;
    }

    /// Optional: Allocates memory to parse JSON documents faster.
    void reserve(size_t max_size)
    {
        if (parser.allocate(max_size) != simdjson::error_code::SUCCESS)
            throw Exception{"Couldn't allocate " + std::to_string(max_size) + " bytes when parsing JSON",
                            ErrorCodes::CANNOT_ALLOCATE_MEMORY};
    }

private:
    simdjson::dom::parser parser;
};

inline ALWAYS_INLINE DomSimdJSONParser::Array DomSimdJSONParser::Element::getArray() const
{
    return element.get_array().value_unsafe();
}

inline ALWAYS_INLINE DomSimdJSONParser::Object DomSimdJSONParser::Element::getObject() const
{
    return element.get_object().value_unsafe();
}


/// This class can be used as an argument for the template class FunctionJSON.
/// It provides ability to parse JSONs using simdjson library.
struct OnDemandSimdJSONParser
{
    class Array;
    class Object;

    /// References an element in a JSON document, representing a JSON null, boolean, string, number,
    /// array or object.
    class Element
    {
    public:
        ALWAYS_INLINE Element() {} /// NOLINT
        ALWAYS_INLINE Element(const simdjson::ondemand::value & value_) : value(value_) {} /// NOLINT

        ALWAYS_INLINE bool isInt64() const { auto res = value.get_number_type(); return !res.error() && res.value() == simdjson::ondemand::number_type::signed_integer; }
        ALWAYS_INLINE bool isUInt64() const { auto res = value.get_number_type(); return !res.error() && res.value() == simdjson::ondemand::number_type::unsigned_integer; }
        ALWAYS_INLINE bool isDouble() const { auto res = value.get_number_type(); return !res.error() && res.value() == simdjson::ondemand::number_type::floating_point_number; }
        ALWAYS_INLINE bool isString() const { return value.type() == simdjson::ondemand::json_type::string; }
        ALWAYS_INLINE bool isArray() const { return value.type() == simdjson::ondemand::json_type::array; }
        ALWAYS_INLINE bool isObject() const { return value.type() == simdjson::ondemand::json_type::object; }
        ALWAYS_INLINE bool isBool() const { return value.type() == simdjson::ondemand::json_type::boolean; }
        ALWAYS_INLINE bool isNull() const { return value.type() == simdjson::ondemand::json_type::null; }

        ALWAYS_INLINE Int64 getInt64() const { return value.get_int64().value(); }
        ALWAYS_INLINE UInt64 getUInt64() const { return value.get_uint64().value(); }
        ALWAYS_INLINE double getDouble() const { return value.get_double().value(); }
        ALWAYS_INLINE bool getBool() const { return value.get_bool().value(); }
        ALWAYS_INLINE std::string_view getString() const { return value.get_string().value(); }
        ALWAYS_INLINE Array getArray() const;
        ALWAYS_INLINE Object getObject() const;

        ALWAYS_INLINE simdjson::ondemand::value getElement() const { return value; }

    private:
        mutable simdjson::ondemand::value value;
    };

    /// References an array in a JSON document.
    class Array
    {
    public:
        class Iterator
        {
        public:
            ALWAYS_INLINE Iterator(const simdjson::ondemand::array_iterator & it_) : it(it_) {} /// NOLINT
            ALWAYS_INLINE Element operator*() const { return (*it).value(); }
            ALWAYS_INLINE Iterator & operator++() { ++it; return *this; }
            ALWAYS_INLINE Iterator operator++(int) { auto res = *this; ++it; return res; } /// NOLINT
            ALWAYS_INLINE friend bool operator!=(const Iterator & left, const Iterator & right) { return left.it != right.it; }
            ALWAYS_INLINE friend bool operator==(const Iterator & left, const Iterator & right) { return !(left != right); }
        private:
            mutable simdjson::ondemand::array_iterator it;
        };

        ALWAYS_INLINE Array(const simdjson::ondemand::array & array_) : array(array_) {} /// NOLINT
        ALWAYS_INLINE Iterator begin() const { return array.begin().value(); }
        ALWAYS_INLINE Iterator end() const { return array.end().value(); }
        ALWAYS_INLINE size_t size() const { return array.count_elements().value(); }
        ALWAYS_INLINE Element operator[](size_t index) const { return array.at(index).value(); }

    private:
        mutable simdjson::ondemand::array array;
    };

    using KeyValuePair = std::pair<std::string_view, Element>;

    /// References an object in a JSON document.
    class Object
    {
    public:
        class Iterator
        {
        public:
            ALWAYS_INLINE Iterator(const simdjson::ondemand::object_iterator & it_) : it(it_) {} /// NOLINT

            ALWAYS_INLINE KeyValuePair operator*() const
            {
                auto field = (*it);
                std::string_view key = field.unescaped_key().value();
                simdjson::ondemand::value value = field.value();
                return {key, value};
            }

            ALWAYS_INLINE Iterator & operator++() { ++it; return *this; }
            ALWAYS_INLINE Iterator operator++(int) { auto res = *this; ++it; return res; } /// NOLINT
            ALWAYS_INLINE friend bool operator!=(const Iterator & left, const Iterator & right) { return left.it != right.it; }
            ALWAYS_INLINE friend bool operator==(const Iterator & left, const Iterator & right) { return !(left != right); }
        private:
            mutable simdjson::ondemand::object_iterator it;
        };

        ALWAYS_INLINE Object(const simdjson::ondemand::object & object_) : object(object_) {} /// NOLINT
        ALWAYS_INLINE Iterator begin() const { return object.begin().value(); }
        ALWAYS_INLINE Iterator end() const { return object.end().value(); }
        ALWAYS_INLINE size_t size() const { return object.count_fields().value(); }

        bool find(std::string_view key, Element & result) const
        {
            auto x = object.find_field_unordered(key);
            if (x.error())
                return false;

            result = x.value_unsafe();
            return true;
        }

        /// Optional: Provides access to an object's element by index.
        KeyValuePair operator[](size_t index) const
        {
            auto it = object.begin();
            while (index--)
                ++it;
            auto field = (*it);
            std::string_view key = field.unescaped_key().value();
            simdjson::ondemand::value value = field.value();
            return {key, value};
        }

    private:
        mutable simdjson::ondemand::object object;
    };

    /// Parses a JSON document, returns the reference to its root element if succeeded.
    bool parse(std::string_view json, Element & result)
    {
        padstr = json;
        auto document = parser.iterate(padstr);
        if (document.error())
        {
            LOG_INFO(&Poco::Logger::get("!!!"), "parser.iterate -> error");
            return false;
        }

        LOG_INFO(&Poco::Logger::get("!!!"), "parser.iterate -> ok");
        result = document.get_value().value();
        LOG_INFO(&Poco::Logger::get("!!!"), "parser.iterate -> gotresult");
        return true;
    }

    /// Optional: Allocates memory to parse JSON documents faster.
    void reserve(size_t max_size)
    {
        if (parser.allocate(max_size) != simdjson::error_code::SUCCESS)
            throw Exception{"Couldn't allocate " + std::to_string(max_size) + " bytes when parsing JSON",
                            ErrorCodes::CANNOT_ALLOCATE_MEMORY};
    }

private:
    simdjson::ondemand::parser parser;
    simdjson::padded_string padstr;
};

inline ALWAYS_INLINE OnDemandSimdJSONParser::Array OnDemandSimdJSONParser::Element::getArray() const
{
    return value.get_array().value();
}

inline ALWAYS_INLINE OnDemandSimdJSONParser::Object OnDemandSimdJSONParser::Element::getObject() const
{
    return value.get_object().value();
}

//using SimdJSONParser = DomSimdJSONParser;
using SimdJSONParser = OnDemandSimdJSONParser;

}

#endif
