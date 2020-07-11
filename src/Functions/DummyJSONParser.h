#pragma once

#include <Common/Exception.h>
#include <Core/Types.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// This class can be used as an argument for the template class FunctionJSON when we unable to parse JSONs.
/// It can't do anything useful and just throws an exception.
struct DummyJSONParser
{
    class Array;
    class Object;

    class Element
    {
    public:
        Element() {}
        bool isInt64() const { return false; }
        bool isUInt64() const { return false; }
        bool isDouble() const { return false; }
        bool isString() const { return false; }
        bool isArray() const { return false; }
        bool isObject() const { return false; }
        bool isBool() const { return false; }
        bool isNull() const { return false; }

        Int64 getInt64() const { return 0; }
        UInt64 getUInt64() const { return 0; }
        double getDouble() const { return 0; }
        bool getBool() const { return false; }
        std::string_view getString() const { return {}; }
        Array getArray() const;
        Object getObject() const;
    };

    class Array
    {
    public:
        class Iterator
        {
        public:
            Element operator*() const { return {}; }
            Iterator & operator ++() { return *this; }
            Iterator operator ++(int) { return *this; }
            friend bool operator ==(const Iterator &, const Iterator &) { return true; }
            friend bool operator !=(const Iterator &, const Iterator &) { return false; }
        };

        Iterator begin() const { return {}; }
        Iterator end() const { return {}; }
        size_t size() const { return 0; }
        Element operator[](size_t) const { return {}; }
    };

    class Object
    {
    public:
        using KeyValuePair = std::pair<std::string_view, Element>;

        class Iterator
        {
        public:
            KeyValuePair operator *() const { return {}; }
            Iterator & operator ++() { return *this; }
            Iterator operator ++(int) { return *this; }
            friend bool operator ==(const Iterator &, const Iterator &) { return true; }
            friend bool operator !=(const Iterator &, const Iterator &) { return false; }
        };

        Iterator begin() const { return {}; }
        Iterator end() const { return {}; }
        size_t size() const { return 0; }
        KeyValuePair operator[](size_t) const { return {}; }
        bool find(const std::string_view &, Element &) const { return false; }
    };

    bool parse(const std::string_view &, Element &) { throw Exception{"Functions JSON* are not supported", ErrorCodes::NOT_IMPLEMENTED}; }
};

}
