#pragma once

#include "config.h"

#if USE_SIMDJSON
#    include <base/types.h>
#    include <base/defines.h>
#    include <simdjson.h>
#    include <Common/JSONParsers/ElementTypes.h>
#    include <Common/PODArray_fwd.h>
#    include <Common/PODArray.h>
#    include <charconv>

namespace DB
{

/// Format elements of basic types into string.
/// The original implementation is mini_formatter in simdjson.h. But it is not public API, so we
/// add a implementation here.
class SimdJSONBasicFormatter
{
public:
    explicit SimdJSONBasicFormatter(PaddedPODArray<UInt8> & buffer_) : buffer(buffer_) {}
    void comma() { oneChar(','); }
    /** Start an array, prints [ **/
    void startArray() { oneChar('['); }
    /** End an array, prints ] **/
    void endArray() { oneChar(']'); }
    /** Start an array, prints { **/
    void startObject() { oneChar('{'); }
    /** Start an array, prints } **/
    void endObject() { oneChar('}'); }
    /** Prints a true **/
    void trueAtom()
    {
        const char * s = "true";
        buffer.insert(s, s + 4);
    }
    /** Prints a false **/
    void falseAtom()
    {
        const char * s = "false";
        buffer.insert(s, s + 5);
    }
    /** Prints a null **/
    void nullAtom()
    {
        const char * s = "null";
        buffer.insert(s, s + 4);
    }
    /** Prints a number **/
    void number(int64_t x)
    {
        char number_buffer[24];
        auto res = std::to_chars(number_buffer, number_buffer + sizeof(number_buffer), x);
        buffer.insert(number_buffer, res.ptr);
    }
    /** Prints a number **/
    void number(uint64_t x)
    {
        char number_buffer[24];
        auto res = std::to_chars(number_buffer, number_buffer + sizeof(number_buffer), x);
        buffer.insert(number_buffer, res.ptr);
    }
    /** Prints a number **/
    void number(double x)
    {
        char number_buffer[24];
        auto res = std::to_chars(number_buffer, number_buffer + sizeof(number_buffer), x);
        buffer.insert(number_buffer, res.ptr);
    }
    /** Prints a key (string + colon) **/
    void key(std::string_view unescaped)
    {
        string(unescaped);
        oneChar(':');
    }
    /** Prints a string. The string is escaped as needed. **/
    void string(std::string_view unescaped)
    {
        oneChar('\"');
        size_t i = 0;
        // Fast path for the case where we have no control character, no ", and no backslash.
        // This should include most keys.
        //
        // We would like to use 'bool' but some compilers take offense to bitwise operation
        // with bool types.
        constexpr static char needs_escaping[] = {
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        for (; i + 8 <= unescaped.length(); i += 8)
        {
            // Poor's man vectorization. This could get much faster if we used SIMD.
            //
            // It is not the case that replacing '|' with '||' would be neutral performance-wise.
            if (needs_escaping[uint8_t(unescaped[i])] | needs_escaping[uint8_t(unescaped[i + 1])]
                | needs_escaping[uint8_t(unescaped[i + 2])] | needs_escaping[uint8_t(unescaped[i + 3])]
                | needs_escaping[uint8_t(unescaped[i + 4])] | needs_escaping[uint8_t(unescaped[i + 5])]
                | needs_escaping[uint8_t(unescaped[i + 6])] | needs_escaping[uint8_t(unescaped[i + 7])])
            {
                break;
            }
        }
        for (; i < unescaped.length(); i++)
        {
            if (needs_escaping[uint8_t(unescaped[i])])
            {
                break;
            }
        }
        // The following is also possible and omits a 256-byte table, but it is slower:
        // for (; (i < unescaped.length()) && (uint8_t(unescaped[i]) > 0x1F)
        //      && (unescaped[i] != '\"') && (unescaped[i] != '\\'); i++) {}

        // At least for long strings, the following should be fast. We could
        // do better by integrating the checks and the insertion.
        buffer.insert(unescaped.data(), unescaped.data() + i);  /// NOLINT(bugprone-suspicious-stringview-data-usage)
        // We caught a control character if we enter this loop (slow).
        // Note that we are do not restart from the beginning, but rather we continue
        // from the point where we encountered something that requires escaping.
        for (; i < unescaped.length(); i++)
        {
            switch (unescaped[i])
            {
                case '\"': {
                    const char * s = "\\\"";
                    buffer.insert(s, s + 2);
                }
                break;
                case '\\': {
                    const char * s = "\\\\";
                    buffer.insert(s, s + 2);
                }
                break;
                default:
                    if (uint8_t(unescaped[i]) <= 0x1F)
                    {
                        // If packed, this uses 8 * 32 bytes.
                        // Note that we expect most compilers to embed this code in the data
                        // section.
                        constexpr static simdjson::escape_sequence escaped[32] = {
                            {6, "\\u0000"}, {6, "\\u0001"}, {6, "\\u0002"}, {6, "\\u0003"}, {6, "\\u0004"}, {6, "\\u0005"}, {6, "\\u0006"},
                            {6, "\\u0007"}, {2, "\\b"},     {2, "\\t"},     {2, "\\n"},     {6, "\\u000b"}, {2, "\\f"},     {2, "\\r"},
                            {6, "\\u000e"}, {6, "\\u000f"}, {6, "\\u0010"}, {6, "\\u0011"}, {6, "\\u0012"}, {6, "\\u0013"}, {6, "\\u0014"},
                            {6, "\\u0015"}, {6, "\\u0016"}, {6, "\\u0017"}, {6, "\\u0018"}, {6, "\\u0019"}, {6, "\\u001a"}, {6, "\\u001b"},
                            {6, "\\u001c"}, {6, "\\u001d"}, {6, "\\u001e"}, {6, "\\u001f"}};
                        auto u = escaped[uint8_t(unescaped[i])];
                        buffer.insert(u.string, u.string + u.length);
                    }
                    else
                    {
                        oneChar(unescaped[i]);
                    }
            } // switch
        } // for
        oneChar('\"');
    }

    void oneChar(char c)
    {
        buffer.push_back(c);
    }
private:
    PaddedPODArray<UInt8> & buffer;

};


/// Format object elements into string, element, array, object, kv-pair.
/// Similar to string_builder in simdjson.h.
class SimdJSONElementFormatter
{
public:
    explicit SimdJSONElementFormatter(PaddedPODArray<UInt8> & buffer_) : format(buffer_) {}
    /** Append an element to the builder (to be printed) **/
    void append(simdjson::dom::element value)
    {
        switch (value.type())
        {
            case simdjson::dom::element_type::UINT64: {
                format.number(value.get_uint64().value_unsafe());
                break;
            }
            case simdjson::dom::element_type::INT64: {
                format.number(value.get_int64().value_unsafe());
                break;
            }
            case simdjson::dom::element_type::DOUBLE: {
                format.number(value.get_double().value_unsafe());
                break;
            }
            case simdjson::dom::element_type::STRING: {
                format.string(value.get_string().value_unsafe());
                break;
            }
            case simdjson::dom::element_type::BOOL: {
                if (value.get_bool().value_unsafe())
                    format.trueAtom();
                else
                    format.falseAtom();
                break;
            }
            case simdjson::dom::element_type::NULL_VALUE: {
                format.nullAtom();
                break;
            }
            case simdjson::dom::element_type::ARRAY: {
                append(value.get_array().value_unsafe());
                break;
            }
            case simdjson::dom::element_type::OBJECT: {
                append(value.get_object().value_unsafe());
                break;
            }
        }
    }
    /** Append an array to the builder (to be printed) **/
    void append(simdjson::dom::array value)
    {
        format.startArray();
        auto iter = value.begin();
        auto end = value.end();
        if (iter != end)
        {
            append(*iter);
            for (++iter; iter != end; ++iter)
            {
                format.comma();
                append(*iter);
            }
        }
        format.endArray();
    }

    void append(simdjson::dom::object value)
    {
        format.startObject();
        auto pair = value.begin();
        auto end = value.end();
        if (pair != end)
        {
            append(*pair);
            for (++pair; pair != end; ++pair)
            {
                format.comma();
                append(*pair);
            }
        }
        format.endObject();
    }

    void append(simdjson::dom::key_value_pair kv)
    {
        format.key(kv.key);
        append(kv.value);
    }
private:
    SimdJSONBasicFormatter format;
};

/// This class can be used as an argument for the template class FunctionJSON.
/// It provides ability to parse JSONs using simdjson library.
struct SimdJSONParser
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

        ALWAYS_INLINE ElementType type() const
        {
            switch (element.type())
            {
                case simdjson::dom::element_type::INT64: return ElementType::INT64;
                case simdjson::dom::element_type::UINT64: return ElementType::UINT64;
                case simdjson::dom::element_type::DOUBLE: return ElementType::DOUBLE;
                case simdjson::dom::element_type::STRING: return ElementType::STRING;
                case simdjson::dom::element_type::ARRAY: return ElementType::ARRAY;
                case simdjson::dom::element_type::OBJECT: return ElementType::OBJECT;
                case simdjson::dom::element_type::BOOL: return ElementType::BOOL;
                case simdjson::dom::element_type::NULL_VALUE: return ElementType::NULL_VALUE;
            }
        }

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
    void reserve(size_t max_size);

private:
    simdjson::dom::parser parser;
};

inline ALWAYS_INLINE SimdJSONParser::Array SimdJSONParser::Element::getArray() const
{
    return element.get_array().value_unsafe();
}

inline ALWAYS_INLINE SimdJSONParser::Object SimdJSONParser::Element::getObject() const
{
    return element.get_object().value_unsafe();
}

}

#endif
