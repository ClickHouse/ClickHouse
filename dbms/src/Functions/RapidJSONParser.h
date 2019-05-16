#pragma once

#include <Common/config.h>
#if USE_RAPIDJSON

#include <common/StringRef.h>
#include <Common/Exception.h>
#include <Core/Types.h>

#include <rapidjson/document.h>


namespace DB
{

/// This class can be used as an argument for the template class FunctionJSON.
/// It provides ability to parse JSONs using rapidjson library.
struct RapidJSONParser
{
    static constexpr bool need_preallocate = false;
    void preallocate(size_t) {}

    bool parse(const char * data, size_t size)
    {
        InputStream in(data, size);
        document.ParseStream(in);
        return !document.HasParseError();
    }

    struct Iterator
    {
    public:
        Iterator() {}
        Iterator(const rapidjson::Document & document) : value(&document) {}
        Iterator(const Iterator & src)
            : value(src.value)
            , parent_scope_is_object(src.parent_scope_is_object)
            , current_in_array(src.current_in_array)
            , end_of_array(src.end_of_array) {}

        Iterator & operator =(const Iterator & src)
        {
            value = src.value;
            parent_scope_is_object = src.parent_scope_is_object;
            current_in_array = src.current_in_array;
            end_of_array = src.end_of_array;
            return *this;
        }

        const rapidjson::Value & getValue() const { return *value; }

        bool downToArray()
        {
            if (value->Empty())
                return false;
            current_in_array = &*value->Begin();
            end_of_array = &*value->End();
            value = current_in_array;
            ++current_in_array;
            parent_scope_is_object = false;
            return true;
        }

        bool next()
        {
            if (current_in_array == end_of_array)
                return false;
            value = current_in_array;
            ++current_in_array;
            return true;
        }

        bool downToObject()
        {
            if (value->ObjectEmpty())
                return false;
            current_in_object = &*value->MemberBegin();
            end_of_object = &*value->MemberEnd();
            value = &current_in_object->value;
            ++current_in_object;
            parent_scope_is_object = true;
            return true;
        }

        bool downToObject(StringRef & first_key)
        {
            if (value->ObjectEmpty())
                return false;
            current_in_object = &*value->MemberBegin();
            end_of_object = &*value->MemberEnd();
            const auto & name = current_in_object->name;
            first_key.data = name.GetString();
            first_key.size = name.GetStringLength();
            value = &current_in_object->value;
            ++current_in_object;
            parent_scope_is_object = true;
            return true;
        }

        bool nextKeyValue()
        {
            if (current_in_object == end_of_object)
                return false;
            value = &current_in_object->value;
            ++current_in_object;
            return true;
        }

        bool nextKeyValue(StringRef & key)
        {
            if (current_in_object == end_of_object)
                return false;
            const auto & name = current_in_object->name;
            key.data = name.GetString();
            key.size = name.GetStringLength();
            value = &current_in_object->value;
            ++current_in_object;
            return true;
        }

        StringRef getKey() const
        {
            const auto & name = (current_in_object - 1)->name;
            return {name.GetString(), name.GetStringLength()};
        }

        bool parentScopeIsObject() const { return parent_scope_is_object; }

        bool isInteger() const { return value->IsInt64(); }
        bool isFloat() const { return value->IsDouble(); }
        bool isBool() const { return value->IsBool(); }
        bool isString() const { return value->IsString(); }
        bool isArray() const { return value->IsArray(); }
        bool isObject() const { return value->IsObject(); }
        bool isNull() const { return value->IsNull(); }

        Int64 getInteger() const { return value->GetInt64(); }
        double getFloat() const { return value->GetDouble(); }
        bool getBool() const { return value->GetBool(); }
        StringRef getString() const { return {value->GetString(), value->GetStringLength()}; }

    private:
        const rapidjson::Value * value = nullptr;
        bool parent_scope_is_object = false;

        union
        {
            const rapidjson::GenericMember<rapidjson::UTF8<>, rapidjson::MemoryPoolAllocator<>> * current_in_object;
            const rapidjson::Value * current_in_array;
        };
        union
        {
            const rapidjson::GenericMember<rapidjson::UTF8<>, rapidjson::MemoryPoolAllocator<>> * end_of_object;
            const rapidjson::Value * end_of_array;
        };
    };

    Iterator getRoot() { return Iterator{document}; }

    static bool downToArray(Iterator & it) { return it.downToArray(); }
    static bool downToObject(Iterator & it) { return it.downToObject(); }
    static bool downToObject(Iterator & it, StringRef & first_key) { return it.downToObject(first_key); }
    static bool parentScopeIsObject(const Iterator & it) { return it.parentScopeIsObject(); }
    static bool next(Iterator & it) { return it.next(); }
    static bool nextKeyValue(Iterator & it) { return it.nextKeyValue(); }
    static bool nextKeyValue(Iterator & it, StringRef & key) { return it.nextKeyValue(key); }
    static bool isInteger(const Iterator & it) { return it.isInteger(); }
    static bool isFloat(const Iterator & it) { return it.isFloat(); }
    static bool isString(const Iterator & it) { return it.isString(); }
    static bool isArray(const Iterator & it) { return it.isArray(); }
    static bool isObject(const Iterator & it) { return it.isObject(); }
    static bool isBool(const Iterator & it) { return it.isBool(); }
    static bool isNull(const Iterator & it) { return it.isNull(); }
    static StringRef getKey(const Iterator & it) { return it.getKey(); }
    static StringRef getString(const Iterator & it) { return it.getString(); }
    static Int64 getInteger(const Iterator & it) { return it.getInteger(); }
    static double getFloat(const Iterator & it) { return it.getFloat(); }
    static bool getBool(const Iterator & it) { return it.getBool(); }

private:
    class InputStream
    {
    public:
        InputStream(const char * data, size_t size) : begin(data), end(data + size), current(data) {}

        using Ch = char;
        Ch Peek() { if (current == end) return 0; return *current; }
        Ch Take() { if (current == end) return 0; return *current++; }
        size_t Tell() const { return current - begin; }

        Ch* PutBegin() { return nullptr; }
        void Put(Ch) {}
        void Flush() {}
        size_t PutEnd(Ch*) { return 0; }

    private:
        const char * begin;
        const char * end;
        const char * current;
    };

    rapidjson::Document document;
};

}
#endif
