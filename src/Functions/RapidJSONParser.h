#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

#if USE_RAPIDJSON
#    include <common/types.h>
#    include <common/defines.h>
#    include <rapidjson/document.h>


namespace DB
{

/// This class can be used as an argument for the template class FunctionJSON.
/// It provides ability to parse JSONs using rapidjson library.
struct RapidJSONParser
{
    class Array;
    class Object;

    /// References an element in a JSON document, representing a JSON null, boolean, string, number,
    /// array or object.
    class Element
    {
    public:
        ALWAYS_INLINE Element() {}
        ALWAYS_INLINE Element(const rapidjson::Value & value_) : ptr(&value_) {}

        ALWAYS_INLINE bool isInt64() const { return ptr->IsInt64(); }
        ALWAYS_INLINE bool isUInt64() const { return ptr->IsUint64(); }
        ALWAYS_INLINE bool isDouble() const { return ptr->IsDouble(); }
        ALWAYS_INLINE bool isString() const { return ptr->IsString(); }
        ALWAYS_INLINE bool isArray() const { return ptr->IsArray(); }
        ALWAYS_INLINE bool isObject() const { return ptr->IsObject(); }
        ALWAYS_INLINE bool isBool() const { return ptr->IsBool(); }
        ALWAYS_INLINE bool isNull() const { return ptr->IsNull(); }

        ALWAYS_INLINE Int64 getInt64() const { return ptr->GetInt64(); }
        ALWAYS_INLINE UInt64 getUInt64() const { return ptr->GetUint64(); }
        ALWAYS_INLINE double getDouble() const { return ptr->GetDouble(); }
        ALWAYS_INLINE bool getBool() const { return ptr->GetBool(); }
        ALWAYS_INLINE std::string_view getString() const { return {ptr->GetString(), ptr->GetStringLength()}; }
        Array getArray() const;
        Object getObject() const;

    private:
        const rapidjson::Value * ptr = nullptr;
    };

    /// References an array in a JSON document.
    class Array
    {
    public:
        class Iterator
        {
        public:
            ALWAYS_INLINE Iterator(const rapidjson::Value::ConstValueIterator & it_) : it(it_) {}
            ALWAYS_INLINE Element operator*() const { return *it; }
            ALWAYS_INLINE Iterator & operator ++() { ++it; return *this; }
            ALWAYS_INLINE Iterator operator ++(int) { auto res = *this; ++it; return res; }
            ALWAYS_INLINE friend bool operator ==(const Iterator & left, const Iterator & right) { return left.it == right.it; }
            ALWAYS_INLINE friend bool operator !=(const Iterator & left, const Iterator & right) { return !(left == right); }
        private:
            rapidjson::Value::ConstValueIterator it;
        };

        ALWAYS_INLINE Array(const rapidjson::Value & value_) : ptr(&value_) {}
        ALWAYS_INLINE Iterator begin() const { return ptr->Begin(); }
        ALWAYS_INLINE Iterator end() const { return ptr->End(); }
        ALWAYS_INLINE size_t size() const { return ptr->Size(); }
        ALWAYS_INLINE Element operator[](size_t index) const { assert(index < size()); return *(ptr->Begin() + index); }

    private:
        const rapidjson::Value * ptr = nullptr;
    };

    using KeyValuePair = std::pair<std::string_view, Element>;

    /// References an object in a JSON document.
    class Object
    {
    public:
        class Iterator
        {
        public:
            ALWAYS_INLINE Iterator(const rapidjson::Value::ConstMemberIterator & it_) : it(it_) {}
            ALWAYS_INLINE KeyValuePair operator *() const { std::string_view key{it->name.GetString(), it->name.GetStringLength()}; return {key, it->value}; }
            ALWAYS_INLINE Iterator & operator ++() { ++it; return *this; }
            ALWAYS_INLINE Iterator operator ++(int) { auto res = *this; ++it; return res; }
            ALWAYS_INLINE friend bool operator ==(const Iterator & left, const Iterator & right) { return left.it == right.it; }
            ALWAYS_INLINE friend bool operator !=(const Iterator & left, const Iterator & right) { return !(left == right); }
        private:
            rapidjson::Value::ConstMemberIterator it;
        };

        ALWAYS_INLINE Object(const rapidjson::Value & value_) : ptr(&value_) {}
        ALWAYS_INLINE Iterator begin() const { return ptr->MemberBegin(); }
        ALWAYS_INLINE Iterator end() const { return ptr->MemberEnd(); }
        ALWAYS_INLINE size_t size() const { return ptr->MemberCount(); }

        bool find(const std::string_view & key, Element & result) const
        {
            auto it = ptr->FindMember(rapidjson::StringRef(key.data(), key.length()));
            if (it == ptr->MemberEnd())
                return false;

            result = it->value;
            return true;
        }

        /// Optional: Provides access to an object's element by index.
        ALWAYS_INLINE KeyValuePair operator[](size_t index) const
        {
            assert (index < size());
            auto it = ptr->MemberBegin() + index;
            std::string_view key{it->name.GetString(), it->name.GetStringLength()};
            return {key, it->value};
        }

    private:
        const rapidjson::Value * ptr = nullptr;
    };

    /// Parses a JSON document, returns the reference to its root element if succeeded.
    bool parse(const std::string_view & json, Element & result)
    {
        rapidjson::MemoryStream ms(json.data(), json.size());
        rapidjson::EncodedInputStream<rapidjson::UTF8<>, rapidjson::MemoryStream> is(ms);
        document.ParseStream(is);
        if (document.HasParseError() || (ms.Tell() != json.size()))
            return false;
        result = document;
        return true;
    }

#if 0
    /// Optional: Allocates memory to parse JSON documents faster.
    void reserve(size_t max_size);
#endif

private:
    rapidjson::Document document;
};

inline ALWAYS_INLINE RapidJSONParser::Array RapidJSONParser::Element::getArray() const
{
    return *ptr;
}

inline ALWAYS_INLINE RapidJSONParser::Object RapidJSONParser::Element::getObject() const
{
    return *ptr;
}

}
#endif
