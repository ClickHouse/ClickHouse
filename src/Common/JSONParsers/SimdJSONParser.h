#pragma once

#include "config.h"

#if USE_SIMDJSON
#    include <base/types.h>
#    include <Common/Exception.h>
#    include <base/defines.h>
#    include <simdjson.h>
#    include "ElementTypes.h"
#    include <Common/PODArray_fwd.h>
#    include <Common/PODArray.h>
#    include <charconv>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int JSON_PARSE_ERROR;
}

// NOLINTBEGIN(bugprone-macro-parentheses)

#define SIMDJSON_ASSIGN_OR_THROW_IMPL(_result, _lhs, _rexpr) \
    auto && (_result) = (_rexpr);                               \
    if ((_result).error() != ::simdjson::SUCCESS)                \
        throw DB::ErrnoException(ErrorCodes::JSON_PARSE_ERROR, "simdjson error: {}", std::string(::simdjson::error_message((_result).error()))); \
    _lhs = std::move(_result).value_unsafe()

#define SIMDJSON_ASSIGN_OR_THROW(_lhs, _rexpr) \
    SIMDJSON_ASSIGN_OR_THROW_IMPL(               \
        DB_ANONYMOUS_VARIABLE(_simdjson_sesult), _lhs, _rexpr)

// NOLINTEND(bugprone-macro-parentheses)

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

    void append(simdjson::ondemand::value value)
    {
        #pragma clang diagnostic push
        #pragma clang diagnostic ignored "-Wcovered-switch-default"
        switch (value.type())
        {
            case simdjson::ondemand::json_type::array:
                append(value.get_array());
                break;
            case simdjson::ondemand::json_type::object:
                append(value.get_object());
                break;
            case simdjson::ondemand::json_type::number:
            {

                simdjson::ondemand::number_type nt{};
                auto res = value.get_number_type().get(nt);
                chassert(res == simdjson::SUCCESS);
                switch (nt)
                {
                    case simdjson::ondemand::number_type::signed_integer:
                        format.number(value.get_int64().value_unsafe());
                        break;
                    case simdjson::ondemand::number_type::unsigned_integer:
                        format.number(value.get_uint64().value_unsafe());
                        break;
                    case simdjson::ondemand::number_type::floating_point_number:
                        format.number(value.get_double().value_unsafe());
                        break;
                    case simdjson::ondemand::number_type::big_integer:
                        format.string(value.get_string().value_unsafe());
                        break;
                    default:
                        break;
                }
                break;
            }
            case simdjson::ondemand::json_type::string:
                format.string(value.get_string().value_unsafe());
                break;
            case simdjson::ondemand::json_type::boolean:
                if (value.get_bool().value_unsafe())
                    format.trueAtom();
                else
                    format.falseAtom();
                break;
            case simdjson::ondemand::json_type::null:
                format.nullAtom();
                break;
            default:
                break;
        }
        #pragma clang diagnostic pop
    }

    void append(simdjson::ondemand::array array)
    {
        format.startArray();
        int i = 0;
        for (simdjson::ondemand::value value : array)
        {
            if (i++ != 0)
                format.comma();
            append(value);
        }
        format.endArray();
    }

    void append(simdjson::ondemand::object object)
    {
        format.startObject();
        int i = 0;
        for (simdjson::ondemand::field field : object)
        {
            if (i++ != 0)
                format.comma();
            append(field);
        }
        format.endObject();
    }

    void append(simdjson::ondemand::field field)
    {
        format.key(field.unescaped_key());
        append(field.value());
    }
private:
    SimdJSONBasicFormatter format;
};

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
        void reset() {}

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
        //std::cerr << "gethere json: " << json << std::endl;
        if (document.error())
        {
            //std::cerr <<"gethere dom parser error" << std::endl;
            return false;
        }
        result = document.value_unsafe();
        return true;
    }

    /// Optional: Allocates memory to parse JSON documents faster.
    void reserve(size_t max_size)
    {
        if (parser.allocate(max_size) != simdjson::error_code::SUCCESS)
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Couldn't allocate {} bytes when parsing JSON", max_size);
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
        ALWAYS_INLINE explicit Element(simdjson::ondemand::value && value_) { value = std::move(value_); }
        ALWAYS_INLINE Element & operator=(const simdjson::ondemand::value & value_) { value = value_; return *this; }

        ALWAYS_INLINE ElementType type() const
        {
            auto t = value.type();
            if (t.error())
            {
                //std::cerr << "gethere type error: " << t.error() << std::endl;
                return ElementType::NULL_VALUE;
            }

            if (t.value() == simdjson::ondemand::json_type::object)
                return ElementType::OBJECT;
            if (t.value() == simdjson::ondemand::json_type::array)
                return ElementType::ARRAY;
            if (t.value() == simdjson::ondemand::json_type::boolean)
                return ElementType::BOOL;
            if (t.value() == simdjson::ondemand::json_type::string)
                return ElementType::STRING;
            if (t.value() == simdjson::ondemand::json_type::number)
            {
                auto res = value.get_number_type();
                if (res.error())
                    return ElementType::NULL_VALUE;
                if (res.value() == simdjson::ondemand::number_type::signed_integer)
                    return ElementType::INT64;
                if (res.value() == simdjson::ondemand::number_type::unsigned_integer)
                    return ElementType::UINT64;
                if (res.value() == simdjson::ondemand::number_type::floating_point_number)
                    return ElementType::DOUBLE;
            }
            return ElementType::NULL_VALUE;
        }

        ALWAYS_INLINE bool isInt64() const
        {
            //std::cerr << "gethere is int64" << std::endl;
            auto t = value.type();
            if (t.error())
            {
                //std::cerr << "gethere isint64 error: " << t.error() << std::endl;
                return false;
            }
            //std::cerr << "gethere type: " << t.value() << std::endl;
            if (t.value() != simdjson::ondemand::json_type::number)
                return false;
            auto res = value.get_number_type();
            return !res.error() && res.value() == simdjson::ondemand::number_type::signed_integer;
        }
        ALWAYS_INLINE bool isUInt64() const
        {
            auto t = value.type();
            if (t.error())
            {
                //std::cerr << "gethere isuint64 error: " << t.error() << std::endl;
                return false;
            }
            //std::cerr << "gethere type: " << t.value() << std::endl;
            if (t.value() != simdjson::ondemand::json_type::number)
                return false;
            //std::cerr << "gethere is uint64" << std::endl;
            auto res = value.get_number_type();
            return !res.error() && res.value() == simdjson::ondemand::number_type::unsigned_integer;
        }
        ALWAYS_INLINE bool isDouble() const
        {
            auto t = value.type();
            if (t.error())
            {
                //std::cerr << "gethere isdouble error: " << t.error() << std::endl;
                return false;
            }
            //std::cerr << "gethere type: " << t.value() << std::endl;
            if (t.value() != simdjson::ondemand::json_type::number)
                return false;
            //std::cerr << "gethere is double" << std::endl;
            auto res = value.get_number_type();
            return !res.error() && res.value() == simdjson::ondemand::number_type::floating_point_number;
        }
        ALWAYS_INLINE bool isString() const
        {
            //std::cerr << "gethere is string" << std::endl;
            auto r = value.type();
            return !r.error() && r.value() == simdjson::ondemand::json_type::string;
        }
        ALWAYS_INLINE bool isArray() const
        {
            //std::cerr <<"gethere is array" << std::endl;
            auto r = value.type();
            //std::cerr <<"gethere is r" << r.error() << std::endl;
            return !r.error() && r.value() == simdjson::ondemand::json_type::array;
        }
        ALWAYS_INLINE bool isObject() const
        {
            //std::cerr <<"gethere is obj" << std::endl;
            auto r = value.type();
            return !r.error() && r.value() == simdjson::ondemand::json_type::object;
        }
        ALWAYS_INLINE bool isBool() const
        {
            //std::cerr <<"gethere is bool" << std::endl;
            auto r = value.type();
            if (r.error())
            {
                //std::cerr << "error: " << r.error() << std::endl;
                return false;
            }
            return r.value() == simdjson::ondemand::json_type::boolean;
        }
        ALWAYS_INLINE bool isNull() const
        {
            //std::cerr <<"gethere isnull()" << std::endl;
            auto r = value.type();
            if (r.error())
            {
                //std::cerr << "error: " << r.error() << std::endl;
                return false;
            }
            return r.value() == simdjson::ondemand::json_type::null;
            //return value.type() == simdjson::ondemand::json_type::null;
        }

        ALWAYS_INLINE Int64 getInt64() const { return value.get_int64().value(); }
        ALWAYS_INLINE UInt64 getUInt64() const { return value.get_uint64().value(); }
        ALWAYS_INLINE double getDouble() const { return value.get_double().value(); }
        ALWAYS_INLINE bool getBool() const
        {
            auto b = value.get_bool();
            if (b.error())
            {
                //std::cerr << "gethere getbool error:" << b.error() << std::endl;
                return false;
            }
            return b.value();
        }
        ALWAYS_INLINE std::string_view getString() const
        {
            auto r = value.get_string();
            if (r.error())
                return {};
            return r.value();
        }
        ALWAYS_INLINE Array getArray() const
        {
            //std::cerr << "gethere getarray, stack:" << std::endl;

            //array = std::make_shared<Array>(value.get_array().value());
            SIMDJSON_ASSIGN_OR_THROW(auto arr, value.get_array());
            //array = arr;
            return arr;
        }
        ALWAYS_INLINE Object getObject() const
        {
            //std::cerr <<"gethere getobj" << std::endl;
            SIMDJSON_ASSIGN_OR_THROW(auto obj, value.get_object());
            //std::cerr <<"gethere getobj suc" << std::endl;
            return obj;
        }

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
            Iterator() = default;
            ALWAYS_INLINE Iterator(const simdjson::ondemand::array_iterator & it_) : it(it_) {} /// NOLINT
            ALWAYS_INLINE Element operator*() const { return Element((*it).value()); }
            ALWAYS_INLINE Iterator & operator++() { ++it; return *this; }
            ALWAYS_INLINE friend bool operator!=(const Iterator & left, const Iterator & right) { return left.it != right.it; }
            ALWAYS_INLINE friend bool operator==(const Iterator & left, const Iterator & right) { return !(left != right); }
        private:
            mutable simdjson::ondemand::array_iterator it;
        };

        ALWAYS_INLINE Array(const simdjson::ondemand::array & array_) : array(array_) {} /// NOLINT
        ALWAYS_INLINE Array(const Array & rhs)
        {
            array = rhs.array;
            last_index = rhs.last_index;
            it = rhs.it;
            arr_size = rhs.arr_size;
        }
        Array& operator=(const Array& rhs)
        {
            if (this != &rhs)
            {
                array = rhs.array;
                last_index = rhs.last_index;
                it = rhs.it;
                arr_size = rhs.arr_size;
            }
            return *this;
        }

        ALWAYS_INLINE Iterator begin() const { return array.begin().value(); }
        ALWAYS_INLINE Iterator end() const { return array.end().value(); }
        ALWAYS_INLINE size_t size() const
        {
            reset();
            //std::cerr << "gethere array.size()" << std::endl;
            //if (arr_size)
            //{
            //    //std::cerr << "gethere array size cached :" << *arr_size << std::endl;
            //    return *arr_size;
            //}
            //std::cerr << "gethere array.size() no cached " << std::endl;
            auto r = array.count_elements();
            if (r.error())
                return 0;
            return r.value();
            //return *arr_size;
            //return array.count_elements().value();
        }
        ALWAYS_INLINE Element operator[](size_t index) const
        {
            //std::cerr << "gethere array[] index:" << index << ", ln:" << last_index << std::endl;
            if (index <= last_index)
            {
                array.reset();
                SIMDJSON_ASSIGN_OR_THROW(it, array.begin());
                last_index = 0;
                for (; last_index < index; ++(it.value()), ++last_index)
                    ;
                SIMDJSON_ASSIGN_OR_THROW(auto ele, *(it.value()));
                return Element(std::move(ele));
            }
            if (!it)
            {
                //SIMDJSON_ASSIGN_OR_THROW(auto iter, array.begin());
                array.reset();
                SIMDJSON_ASSIGN_OR_THROW(it, array.begin());
                //it = iter;
            }
            size_t diff = index - last_index;
            while (diff--)
                ++(it.value());
            last_index = index;
            SIMDJSON_ASSIGN_OR_THROW(auto ele, *(it.value()));
            return Element(std::move(ele));
        }

        void reset() const
        {
            array.reset();
            last_index = 0;
        }

    private:
        mutable size_t last_index{};
        mutable std::optional<simdjson::ondemand::array_iterator> it;
        mutable simdjson::ondemand::array array;
        mutable std::optional<size_t> arr_size{};
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
                auto field_wrapper = *it;
                if (field_wrapper.error())
                {
                    return {};
                }
                std::string_view key;
                auto key_error = field_wrapper.unescaped_key().get(key);
                if (key_error)
                {
                    return {};
                }
                ::simdjson::ondemand::value v = field_wrapper.value();
                return {key, Element(std::move(v))};
            }

            ALWAYS_INLINE Iterator & operator++() { ++it; return *this; }
            ALWAYS_INLINE friend bool operator!=(const Iterator & left, const Iterator & right) { return left.it != right.it; }
            ALWAYS_INLINE friend bool operator==(const Iterator & left, const Iterator & right) { return !(left != right); }
        private:
            mutable simdjson::ondemand::object_iterator it;
        };

        ALWAYS_INLINE Object(const simdjson::ondemand::object & object_) : object(object_) {} /// NOLINT
        ALWAYS_INLINE Iterator begin() const { return object.begin().value(); }
        ALWAYS_INLINE Iterator end() const { return object.end().value(); }
        ///NOTE: call size() before iterate
        ALWAYS_INLINE size_t size() const
        {
            //std::cerr << "gethere obj size" << std::endl;
            auto r = object.count_fields();
            if (r.error())
                return 0;
            return r.value();
        }

        bool find(std::string_view key, Element & result) const
        {
            auto x = object.find_field_unordered(key);
            if (x.error())
                return false;

            result = x.value_unsafe();
            return true;
        }

        bool reset()
        {
            auto v = object.reset();
            if (v.error())
                return false;
            return true;
        }

        /// Optional: Provides access to an object's element by index.
        KeyValuePair operator[](size_t index) const
        {
            SIMDJSON_ASSIGN_OR_THROW(auto it, object.begin());
            while (index--)
            {
                (void)*(it); /// NEED TO DO THIS TO ITERATE
                ++it;
            }
            SIMDJSON_ASSIGN_OR_THROW(auto field, *it);
            std::string_view key = field.unescaped_key().value();
            simdjson::ondemand::value value = field.value();
            return {key, Element(std::move(value))};
        }

    private:
        mutable simdjson::ondemand::object object;
    };

    /// Parses a JSON document, returns the reference to its root element if succeeded.
    bool parse(std::string_view json, Element & result)
    {
        padstr = json;
        auto doc = parser.iterate(padstr);
        if (doc.error())
        {
            //std::cerr << "gethere parse json fail, " << doc.error() << std::endl;
            return false;
        }
        //std::cerr << "gethere parse json ok: " << json << std::endl;
        document = std::move(doc.value());
        auto scalar = document.is_scalar();
        if (scalar.error())
        {
            //std::cerr << "gethere is_scalar call fail:" << scalar.error() << std::endl;
            return false;
        }
        if (scalar.value())
        {
            if (!checkIfValidScalar())
                return false;
            //std::cerr << "gethere scala: " << json << std::endl;
            padded_scalar_string.reserve(2 + json.size() + simdjson::SIMDJSON_PADDING);
            padded_scalar_string.insert(0, "[");
            padded_scalar_string.insert(1, json.data(), json.size());
            padded_scalar_string.insert(json.size() + 1, "]");
            padded_scalar_string.insert(json.size() + 2, '\0', simdjson::SIMDJSON_PADDING);

            auto res = parser.iterate(simdjson::padded_string_view(padded_scalar_string));
            if (res.error())
            {
                //std::cerr << "gethere parse json fail, " << res.error() << std::endl;
                return false;
            }
            document = std::move(res.value());
            auto v = document.get_value();
            if (v.error())
            {
                //std::cerr << "gethere get value fail, " << v.error() << std::endl;
                return false;
            }
            result = v.get_array().at(0);
            return true;
        }
        //std::cerr << "gethere parse json not scalar" << std::endl;
        auto v = document.get_value();
        if (v.error())
        {
            //std::cerr << "gethere get value fail, " << v.error() << std::endl;
            return false;
        }
        //std::cerr << "gethere get value ok" << std::endl;
        result = v.value();
        return true;
    }

private:
    bool checkIfValidScalar()
    {
        auto type = document.type();
        if (type.error())
            return false;
        switch (type.value())
        {
            case simdjson::ondemand::json_type::string:
            {
                auto s = document.get_string();
                if (s.error())
                    return false;
                break;
            }
            case simdjson::ondemand::json_type::boolean:
            {
                auto b = document.get_bool();
                if (b.error())
                    return false;
                break;
            }
            case simdjson::ondemand::json_type::number:
            {
                auto res = document.get_number_type();
                if (res.error())
                    return false;
                if (res.value() == simdjson::ondemand::number_type::signed_integer)
                {
                    auto i = document.get_int64();
                    if (i.error())
                        return false;
                }
                if (res.value() == simdjson::ondemand::number_type::unsigned_integer)
                {
                    auto u = document.get_uint64();
                    if (u.error())
                        return false;
                }
                if (res.value() == simdjson::ondemand::number_type::floating_point_number)
                {
                    auto d = document.get_double();
                    if (d.error())
                        return false;
                }
                break;
            }
            case simdjson::ondemand::json_type::null:
            {
                auto v = document.raw_json();
                if (v.error())
                    return false;
                if (v.value() != "null")
                    return false;
                break;
            }
            default:
                break;
        }
        return true;
    }
    simdjson::ondemand::parser parser;
    simdjson::ondemand::document document{};
    simdjson::padded_string padstr;
    std::string padded_scalar_string;
    simdjson::padded_string_view padstr_view;
};

using SimdJSONParser = DomSimdJSONParser;

}

#endif
